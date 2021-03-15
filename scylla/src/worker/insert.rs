// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0
use super::*;

#[derive(Clone)]
pub struct InsertWorker<S: Insert<K, V>, K, V> {
    pub keyspace: S,
    pub key: K,
    pub value: V,
}

impl<S: Insert<K, V>, K, V> InsertWorker<S, K, V>
where
    S: 'static + Select<K, V>,
    K: 'static + Send,
    V: 'static + Send,
{
    pub fn new(keyspace: S, key: K, value: V) -> Self {
        Self { keyspace, key, value }
    }
    pub fn boxed(keyspace: S, key: K, value: V) -> Box<Self> {
        Box::new(Self::new(keyspace, key, value))
    }
}

impl<S, K, V> Worker for InsertWorker<S, K, V>
where
    S: 'static + Insert<K, V>,
    K: 'static + Send + Clone,
    V: 'static + Send + Clone,
{
    fn handle_response(self: Box<Self>, giveload: Vec<u8>) {
        let void = Self::decode_response(Decoder::from(giveload));
    }

    fn handle_error(self: Box<Self>, mut error: WorkerError, reporter: &Option<ReporterHandle>) {
        error!("{:?}, reporter running: {}", error, reporter.is_some());
        if let WorkerError::Cql(ref mut cql_error) = error {
            if let (Some(id), Some(reporter)) = (cql_error.take_unprepared_id(), reporter) {
                handle_unprepared_error(&self, &self.keyspace, &self.key, &self.value, id, reporter);
            }
        }
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
    reporter: &ReporterHandle,
) where
    W: 'static + Worker + Clone,
    S: 'static + Insert<K, V>,
    K: 'static + Send,
    V: 'static + Send,
{
    let statement = keyspace.insert_statement::<K, V>();
    info!("Attempting to prepare statement '{}', id: '{:?}'", statement, id);
    let Prepare(payload) = Prepare::new().statement(&statement).build();
    let prepare_worker = Box::new(PrepareWorker::new(id, statement));
    let prepare_request = ReporterEvent::Request {
        worker: prepare_worker,
        payload,
    };
    reporter.send(prepare_request).ok();
    let req = keyspace
        .insert_query(&key, &value)
        .consistency(Consistency::One)
        .build();
    let payload = req.into_payload();
    let retry_request = ReporterEvent::Request {
        worker: worker.clone(),
        payload,
    };
    reporter.send(retry_request).ok();
}
