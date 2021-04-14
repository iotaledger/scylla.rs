// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0
use super::*;

#[derive(Clone)]
pub struct DeleteWorker<S: Delete<K, V>, K, V> {
    pub keyspace: S,
    pub key: K,
    pub retries: usize,
    pub _marker: std::marker::PhantomData<V>,
}

impl<S: Delete<K, V>, K, V> DeleteWorker<S, K, V>
where
    S: 'static + Delete<K, V>,
    K: 'static + Send,
    V: 'static + Send,
{
    pub fn new(keyspace: S, key: K, retries: usize) -> Self {
        Self {
            keyspace,
            key,
            retries,
            _marker: std::marker::PhantomData,
        }
    }
    pub fn boxed(keyspace: S, key: K, retries: usize) -> Box<Self> {
        Box::new(Self::new(keyspace, key, retries))
    }
}

impl<S, K, V> Worker for DeleteWorker<S, K, V>
where
    S: 'static + Delete<K, V>,
    K: 'static + Send + Clone,
    V: 'static + Send + Clone,
{
    fn handle_response(self: Box<Self>, giveload: Vec<u8>) -> anyhow::Result<()> {
        Self::decode_response(giveload.try_into()?)?;
        Ok(())
    }

    fn handle_error(
        mut self: Box<Self>,
        mut error: WorkerError,
        reporter: &Option<ReporterHandle>,
    ) -> anyhow::Result<()> {
        if let WorkerError::Cql(ref mut cql_error) = error {
            if let (Some(id), Some(reporter)) = (cql_error.take_unprepared_id(), reporter) {
                return handle_unprepared_error(&self, &self.keyspace, &self.key, id, reporter)
                    .map_err(|e| anyhow!("Error trying to prepare query: {}", e));
            }
        }
        if self.retries > 0 {
            self.retries -= 1;
            // currently we assume all cql/worker errors are retryable, but we might change this in future
            let req = self
                .keyspace
                .delete_query(&self.key)
                .consistency(Consistency::One)
                .build()?;
            tokio::spawn(async { req.send_global(self) });
        } else {
            // no more retries
            // print error!
            error!("{:?}, reporter running: {}", error, reporter.is_some());
        }
        Ok(())
    }
}

/// Handle an unprepared CQL error by sending a prepare
/// request and resubmitting the original delete query as an
/// unprepared statement
pub fn handle_unprepared_error<W, S, K, V>(
    worker: &Box<W>,
    keyspace: &S,
    key: &K,
    id: [u8; 16],
    reporter: &ReporterHandle,
) -> anyhow::Result<()>
where
    W: 'static + Worker + Clone,
    S: 'static + Delete<K, V>,
    K: 'static + Send,
    V: 'static + Send,
{
    let statement = keyspace.delete_statement::<K, V>();
    info!("Attempting to prepare statement '{}', id: '{:?}'", statement, id);
    let Prepare(payload) = Prepare::new().statement(&statement).build()?;
    let prepare_worker = PrepareWorker::boxed(id, statement);
    let prepare_request = ReporterEvent::Request {
        worker: prepare_worker,
        payload,
    };
    reporter.send(prepare_request).ok();
    let req = keyspace.delete_query(&key).consistency(Consistency::One).build()?;
    let payload = req.into_payload();
    let retry_request = ReporterEvent::Request {
        worker: worker.clone(),
        payload,
    };
    reporter.send(retry_request).ok();
    Ok(())
}
