// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[derive(Clone)]
pub struct ValueWorker<H, S: Select<K, V>, K, V>
where
    S: 'static + Select<K, V> + Clone,
    K: 'static + Send + Clone,
    V: 'static + Send + Clone,
    H: 'static + Send + HandleResponse<Self, Response = Option<V>> + HandleError<Self> + Clone,
{
    pub handle: H,
    pub keyspace: S,
    pub key: K,
    pub _marker: std::marker::PhantomData<V>,
}

impl<H, S: Select<K, V>, K, V> ValueWorker<H, S, K, V>
where
    S: 'static + Select<K, V> + Clone,
    K: 'static + Send + Clone,
    V: 'static + Send + Clone,
    H: 'static + Send + HandleResponse<Self, Response = Option<V>> + HandleError<Self> + Clone,
{
    pub fn new(handle: H, keyspace: S, key: K, _marker: std::marker::PhantomData<V>) -> Self {
        Self {
            handle,
            keyspace,
            key,
            _marker,
        }
    }
    pub fn boxed(handle: H, keyspace: S, key: K, _marker: std::marker::PhantomData<V>) -> Box<Self> {
        Box::new(Self::new(handle, keyspace, key, _marker))
    }
}

impl<H, S, K, V> DecodeResponse<Option<V>> for ValueWorker<H, S, K, V>
where
    H: Send + HandleResponse<Self, Response = Option<V>> + HandleError<ValueWorker<H, S, K, V>> + Clone,
    S: Select<K, V> + Clone,
    K: Send + Clone,
    V: Send + Clone,
{
    fn decode_response(decoder: Decoder) -> Option<V> {
        S::decode(decoder)
    }
}

impl<S, H, K, V> Worker for ValueWorker<H, S, K, V>
where
    S: 'static + Select<K, V> + Clone,
    K: 'static + Send + Clone,
    V: 'static + Send + Clone,
    H: 'static + Send + HandleResponse<Self, Response = Option<V>> + HandleError<Self> + Clone,
{
    fn handle_response(self: Box<Self>, giveload: Vec<u8>) {
        match Decoder::try_from(giveload) {
            Ok(decoder) => H::handle_response(self, Self::decode_response(decoder)),
            Err(e) => H::handle_error(self, WorkerError::Other(e)),
        }
    }

    fn handle_error(self: Box<Self>, mut error: WorkerError, reporter: &Option<ReporterHandle>) {
        error!("{:?}, reporter running: {}", error, reporter.is_some());
        if let WorkerError::Cql(ref mut cql_error) = error {
            if let (Some(id), Some(reporter)) = (cql_error.take_unprepared_id(), reporter) {
                handle_unprepared_error(&self, &self.keyspace, &self.key, id, reporter).unwrap_or_else(|e| {
                    error!("Error trying to prepare query: {}", e);
                    H::handle_error(self, error);
                });
            }
        } else {
            H::handle_error(self, error);
        }
    }
}

impl<S, K, V> HandleResponse<ValueWorker<UnboundedSender<Result<Option<V>, WorkerError>>, S, K, V>>
    for UnboundedSender<Result<Option<V>, WorkerError>>
where
    S: 'static + Send + Select<K, V> + Clone,
    K: 'static + Send + Clone,
    V: 'static + Send + Clone,
{
    type Response = Option<V>;
    fn handle_response(
        worker: Box<ValueWorker<UnboundedSender<Result<Option<V>, WorkerError>>, S, K, V>>,
        response: Self::Response,
    ) {
        let _ = worker.handle.send(Ok(response));
    }
}

impl<S, K, V> HandleError<ValueWorker<UnboundedSender<Result<Option<V>, WorkerError>>, S, K, V>>
    for UnboundedSender<Result<Option<V>, WorkerError>>
where
    S: 'static + Send + Select<K, V> + Clone,
    K: 'static + Send + Clone,
    V: 'static + Send + Clone,
{
    fn handle_error(
        worker: Box<ValueWorker<UnboundedSender<Result<Option<V>, WorkerError>>, S, K, V>>,
        worker_error: WorkerError,
    ) {
        let _ = worker.handle.send(Err(worker_error));
    }
}
