// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

pub struct SelectWorker<H, S: Select<K, V>, K, V>
where
    S: 'static + Select<K, V>,
    K: 'static + Send,
    V: 'static + Send,
    H: 'static + Send + HandleResponse<Self, Response = Decoder> + HandleError<Self>,
{
    pub handle: H,
    pub keyspace: S,
    pub key: K,
    pub _marker: std::marker::PhantomData<V>,
}

impl<H, S: Select<K, V>, K, V> SelectWorker<H, S, K, V>
where
    S: 'static + Select<K, V>,
    K: 'static + Send,
    V: 'static + Send,
    H: 'static + Send + HandleResponse<Self, Response = Decoder> + HandleError<Self>,
{
    pub fn new(handle: H, keyspace: S, key: K, _marker: std::marker::PhantomData<V>) -> Self {
        Self {
            handle,
            keyspace,
            key,
            _marker,
        }
    }
}

impl<H, S, K, V> Worker for SelectWorker<H, S, K, V>
where
    S: 'static + Select<K, V>,
    K: 'static + Send,
    V: 'static + Send,
    H: 'static + Send + HandleResponse<Self, Response = Decoder> + HandleError<Self>,
{
    fn handle_response(self: Box<Self>, giveload: Vec<u8>) {
        let rows = Self::decode_response(Decoder::from(giveload));
        H::handle_response(self, rows)
    }

    fn handle_error(self: Box<Self>, mut error: WorkerError, reporter: &Option<ReporterHandle>) {
        error!("{:?}, reporter running: {}", error, reporter.is_some());
        if let WorkerError::Cql(ref mut cql_error) = error {
            if let (Some(id), Some(reporter)) = (cql_error.take_unprepared_id(), reporter) {
                let statement = self.keyspace.select_statement::<K, V>();
                info!("Attempting to prepare statement '{}', id: '{:?}'", statement, id);
                let Prepare(payload) = Prepare::new().statement(&statement).build();
                let worker = Box::new(PrepareWorker::new(id, statement));
                let prepare_request = ReporterEvent::Request { worker, payload };
                reporter.send(prepare_request).ok();
                let req = self
                    .keyspace
                    .select_query(&self.key)
                    .consistency(Consistency::One)
                    .build();
                let payload = req.into_payload();
                let retry_request = ReporterEvent::Request { worker: self, payload };
                reporter.send(retry_request).ok();
                return ();
            }
        }
        H::handle_error(self, error);
    }
}
