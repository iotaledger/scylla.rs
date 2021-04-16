// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0
use super::*;

/// A delete request worker
#[derive(Clone)]
pub struct DeleteWorker<S: Delete<K, V>, K, V> {
    /// The keyspace this worker is for
    pub keyspace: S,
    /// The key used to delete the record
    pub key: K,
    /// The number of times this worker will retry on failure
    pub retries: usize,
    _marker: std::marker::PhantomData<V>,
}

impl<S: Delete<K, V>, K, V> DeleteWorker<S, K, V>
where
    S: 'static + Delete<K, V>,
    K: 'static + Send,
    V: 'static + Send,
{
    /// Create a new delete worker with a number of retries
    pub fn new(keyspace: S, key: K, retries: usize) -> Self {
        Self {
            keyspace,
            key,
            retries,
            _marker: std::marker::PhantomData,
        }
    }
    /// Create a new boxed delete worker with a number of retries
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
    fn handle_response(self: Box<Self>, giveload: Vec<u8>) {
        let _void = Self::decode_response(Decoder::from(giveload));
    }

    fn handle_error(mut self: Box<Self>, mut error: WorkerError, reporter: &Option<ReporterHandle>) {
        if let WorkerError::Cql(ref mut cql_error) = error {
            if let (Some(id), Some(reporter)) = (cql_error.take_unprepared_id(), reporter) {
                handle_unprepared_error(&self, &self.keyspace, &self.key, id, reporter);
                return ();
            }
        }
        if self.retries > 0 {
            self.retries -= 1;
            // currently we assume all cql/worker errors are retryable, but we might change this in future
            let req = self
                .keyspace
                .delete_query(&self.key)
                .consistency(Consistency::One)
                .build();
            tokio::spawn(async { req.send_global(self) });
        } else {
            // no more retries
            // print error!
            error!("{:?}, reporter running: {}", error, reporter.is_some());
        }
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
) where
    W: 'static + Worker + Clone,
    S: 'static + Delete<K, V>,
    K: 'static + Send,
    V: 'static + Send,
{
    let statement = keyspace.delete_statement::<K, V>();
    info!("Attempting to prepare statement '{}', id: '{:?}'", statement, id);
    let Prepare(payload) = Prepare::new().statement(&statement).build();
    let prepare_worker = PrepareWorker::boxed(id, statement);
    let prepare_request = ReporterEvent::Request {
        worker: prepare_worker,
        payload,
    };
    reporter.send(prepare_request).ok();
    let req = keyspace.delete_query(&key).consistency(Consistency::One).build();
    let payload = req.into_payload();
    let retry_request = ReporterEvent::Request {
        worker: worker.clone(),
        payload,
    };
    reporter.send(retry_request).ok();
}
