// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[derive(Clone)]
pub struct SelectWorker<H, S: Select<K, V>, K, V>
where
    S: 'static + Select<K, V>,
    K: 'static + Send + Clone,
    V: 'static + Send + Clone,
    H: 'static + Send + HandleResponse<Self, Response = Decoder> + HandleError<Self> + Clone,
{
    pub handle: H,
    pub keyspace: S,
    pub key: K,
    pub _marker: std::marker::PhantomData<V>,
}

impl<H, S: Select<K, V>, K, V> SelectWorker<H, S, K, V>
where
    S: 'static + Select<K, V>,
    K: 'static + Send + Clone,
    V: 'static + Send + Clone,
    H: 'static + Send + HandleResponse<Self, Response = Decoder> + HandleError<Self> + Clone,
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

impl<H, S, K, V> Worker for SelectWorker<H, S, K, V>
where
    S: 'static + Select<K, V>,
    K: 'static + Send + Clone,
    V: 'static + Send + Clone,
    H: 'static + Send + HandleResponse<Self, Response = Decoder> + HandleError<Self> + Clone,
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

impl<S, K, V> HandleResponse<SelectWorker<UnboundedSender<Result<Decoder, WorkerError>>, S, K, V>>
    for UnboundedSender<Result<Decoder, WorkerError>>
where
    S: 'static + Send + Select<K, V>,
    K: 'static + Send + Clone,
    V: 'static + Send + Clone,
{
    type Response = Decoder;
    fn handle_response(
        worker: Box<SelectWorker<UnboundedSender<Result<Decoder, WorkerError>>, S, K, V>>,
        response: Self::Response,
    ) {
        let _ = worker.handle.send(Ok(response));
    }
}

impl<S, K, V> HandleError<SelectWorker<UnboundedSender<Result<Decoder, WorkerError>>, S, K, V>>
    for UnboundedSender<Result<Decoder, WorkerError>>
where
    S: 'static + Send + Select<K, V>,
    K: 'static + Send + Clone,
    V: 'static + Send + Clone,
{
    fn handle_error(
        worker: Box<SelectWorker<UnboundedSender<Result<Decoder, WorkerError>>, S, K, V>>,
        worker_error: WorkerError,
    ) {
        let _ = worker.handle.send(Err(worker_error));
    }
}

/// Handle an unprepared CQL error by sending a prepare
/// request and resubmitting the original query as an
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
    S: 'static + Select<K, V>,
    K: 'static + Send,
    V: 'static + Send,
{
    let statement = keyspace.select_statement::<K, V>();
    info!("Attempting to prepare statement '{}', id: '{:?}'", statement, id);
    let Prepare(payload) = Prepare::new().statement(&statement).build()?;
    let prepare_worker = Box::new(PrepareWorker::new(id, statement));
    let prepare_request = ReporterEvent::Request {
        worker: prepare_worker,
        payload,
    };
    reporter.send(prepare_request).ok();
    let req = keyspace
        .select_query::<V>(&key)?
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
