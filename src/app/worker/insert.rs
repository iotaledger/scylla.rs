// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0
use super::*;

/// An insert request worker
#[derive(Clone)]
pub struct InsertWorker<S: Insert<K, V>, K, V> {
    /// The keyspace this worker is for
    pub keyspace: S,
    /// The key used to insert the record
    pub key: K,
    /// The value to be inserted
    pub value: V,
    /// The number of times this worker will retry on failure
    pub retries: usize,
}

impl<S: Insert<K, V>, K, V> InsertWorker<S, K, V>
where
    S: 'static + Insert<K, V>,
    K: 'static + Send,
    V: 'static + Send,
{
    /// Create a new insert worker with a number of retries
    pub fn new(keyspace: S, key: K, value: V, retries: usize) -> Self {
        Self {
            keyspace,
            key,
            value,
            retries,
        }
    }
    /// Create a new boxed insert worker with a number of retries
    pub fn boxed(keyspace: S, key: K, value: V, retries: usize) -> Box<Self> {
        Box::new(Self::new(keyspace, key, value, retries))
    }
}

impl<S, K, V> Worker for InsertWorker<S, K, V>
where
    S: 'static + Insert<K, V>,
    K: 'static + Send + Sync + Clone,
    V: 'static + Send + Sync + Clone,
{
    fn handle_response(self: Box<Self>, giveload: Vec<u8>) -> anyhow::Result<()> {
        Self::decode_response(giveload.try_into()?)?;
        Ok(())
    }

    fn handle_error(
        mut self: Box<Self>,
        mut error: WorkerError,
        reporter: Option<&mut UnboundedSender<<Reporter as Actor>::Event>>,
    ) -> anyhow::Result<()> {
        let reporter_running = reporter.is_some();
        if let WorkerError::Cql(ref mut cql_error) = error {
            if let (Some(id), Some(reporter)) = (cql_error.take_unprepared_id(), reporter) {
                return handle_unprepared_error(&self, &self.keyspace, &self.key, &self.value, id, reporter)
                    .map_err(|e| anyhow!("Error trying to prepare query: {}", e));
            }
        }
        if self.retries > 0 {
            self.retries -= 1;
            // currently we assume all cql/worker errors are retryable, but we might change this in future
            let req = self
                .keyspace
                .insert_query(&self.key, &self.value)
                .consistency(Consistency::One)
                .build()?;
            tokio::spawn(async { req.send_global(self) });
        } else {
            // no more retries
            // print error!
            error!("{:?}, reporter running: {}", error, reporter_running);
        }
        Ok(())
    }
}

/// Handle an unprepared CQL error by sending a prepare
/// request and resubmitting the original query as an
/// unprepared statement
pub fn handle_unprepared_error<W, S, K, V>(
    worker: &Box<W>,
    keyspace: &S,
    key: &K,
    value: &V,
    id: [u8; 16],
    reporter: &mut UnboundedSender<<Reporter as Actor>::Event>,
) -> anyhow::Result<()>
where
    W: 'static + Worker + Clone,
    S: 'static + Insert<K, V>,
    K: 'static + Send,
    V: 'static + Send,
{
    let statement = keyspace.insert_statement::<K, V>();
    info!("Attempting to prepare statement '{}', id: '{:?}'", statement, id);
    let Prepare(payload) = Prepare::new().statement(&statement).build()?;
    let prepare_worker = Box::new(PrepareWorker::new(id, statement));
    let prepare_request = ReporterEvent::Request {
        worker: prepare_worker,
        payload,
    };
    reporter.send(prepare_request).ok();
    let req = keyspace
        .insert_query(&key, &value)
        .consistency(Consistency::One)
        .build()?;
    let payload = req.into_payload();
    let retry_request = ReporterEvent::Request {
        worker: worker.clone(),
        payload,
    };
    reporter.send(retry_request).ok();
    Ok(())
}
