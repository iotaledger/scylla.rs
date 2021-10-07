// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::prelude::RowsDecoder;
use std::fmt::Debug;
/// A value selecting worker
pub struct ValueWorker<H, V, R> {
    pub request: Option<R>,
    /// A handle which can be used to return the queried value
    pub handle: H,
    /// The query page size, used when retying due to failure
    pub page_size: Option<i32>,
    /// The query paging state, used when retrying due to failure
    pub paging_state: Option<Vec<u8>>,
    /// The number of times this worker will retry on failure
    pub retries: usize,
    decode_fn: Box<dyn Send + Fn(Decoder) -> anyhow::Result<Option<V>>>,
}

impl<H, V, R> Debug for ValueWorker<H, V, R>
where
    H: Debug,
    R: Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ValueWorker")
            .field("request", &self.request)
            .field("handle", &self.handle)
            .field("page_size", &self.page_size)
            .field("paging_state", &self.paging_state)
            .field("retries", &self.retries)
            .finish()
    }
}

impl<H, V, R> ValueWorker<H, V, R>
where
    V: 'static,
{
    /// Create a new value selecting worker with a number of retries and a response handle
    pub fn new<S>(handle: H) -> Box<Self>
    where
        S: 'static + RowsDecoder<V>,
    {
        Box::new(Self {
            request: None,
            handle,
            page_size: None,
            paging_state: None,
            retries: 0,
            decode_fn: Box::new(S::try_decode_rows),
        })
    }
}
impl<H, V, R> ValueWorker<H, V, R> {
    pub fn using_decode_fn<F: 'static + Send + Fn(Decoder) -> anyhow::Result<Option<V>>>(
        handle: H,
        decode_fn: F,
    ) -> Box<Self> {
        Box::new(Self {
            request: None,
            handle,
            page_size: None,
            paging_state: None,
            retries: 0,
            decode_fn: Box::new(decode_fn),
        })
    }
    pub fn with_retries(mut self: Box<Self>, retries: usize, request: R) -> Box<Self> {
        if retries > 0 {
            self.retries = retries;
            self.request.replace(request);
        }
        self
    }
    /// Add paging information to this worker
    pub fn with_paging<P: Into<Option<Vec<u8>>>>(mut self: Box<Self>, page_size: i32, paging_state: P) -> Box<Self> {
        self.page_size = Some(page_size);
        self.paging_state = paging_state.into();
        self
    }
}

impl<H, V, R> Worker for ValueWorker<H, V, R>
where
    V: 'static + Send,
    H: 'static + HandleResponse<Self, Response = Option<V>> + HandleError<Self> + Debug + Clone + Send,
    R: 'static + Send + Debug + Clone + GetRequestExt<Option<V>>,
{
    fn handle_response(self: Box<Self>, giveload: Vec<u8>) -> anyhow::Result<()> {
        match Decoder::try_from(giveload) {
            Ok(decoder) => match (self.decode_fn)(decoder) {
                Ok(res) => self.handle.handle_response(res),
                Err(e) => self.handle.handle_error(WorkerError::Other(e)),
            },
            Err(e) => self.handle.handle_error(WorkerError::Other(e)),
        }
    }

    fn handle_error(mut self: Box<Self>, mut error: WorkerError, reporter: &ReporterHandle) -> anyhow::Result<()> {
        if let WorkerError::Cql(ref mut cql_error) = error {
            let handle = self.handle.clone();
            if let (Some(id), Some(request)) = (cql_error.take_unprepared_id(), self.request.take()) {
                handle_unprepared_error(self, request, id, reporter).or_else(|e| {
                    error!("Error trying to prepare query: {}", e);
                    handle.handle_error(error)
                })
            } else {
                match self.retry() {
                    Ok(_) => Ok(()),
                    Err(worker) => H::handle_error(&worker.handle, error),
                }
            }
        } else {
            match self.retry() {
                Ok(_) => Ok(()),
                Err(worker) => H::handle_error(&worker.handle, error),
            }
        }
    }
}

impl<H, V, R> RetryableWorker<R, Option<V>> for ValueWorker<H, V, R>
where
    V: 'static + Send,
    H: 'static + HandleResponse<Self, Response = Option<V>> + HandleError<Self> + Debug + Clone + Send,
    R: 'static + Send + Debug + Clone + GetRequestExt<Option<V>>,
{
    fn retries(&mut self) -> &mut usize {
        &mut self.retries
    }

    fn request(&self) -> Option<&R> {
        self.request.as_ref()
    }
}

impl<V, R> HandleResponse<ValueWorker<UnboundedSender<Result<Option<V>, WorkerError>>, V, R>>
    for UnboundedSender<Result<Option<V>, WorkerError>>
where
    V: 'static + Send,
    R: 'static + Send + Debug + Clone + GetRequestExt<Option<V>>,
{
    type Response = Option<V>;
    fn handle_response(&self, response: Self::Response) -> anyhow::Result<()> {
        self.send(Ok(response)).map_err(|e| anyhow!(e.to_string()))
    }
}

impl<V, R> HandleError<ValueWorker<UnboundedSender<Result<Option<V>, WorkerError>>, V, R>>
    for UnboundedSender<Result<Option<V>, WorkerError>>
where
    V: 'static + Send,
    R: 'static + Send + Debug + Clone + GetRequestExt<Option<V>>,
{
    fn handle_error(&self, worker_error: WorkerError) -> anyhow::Result<()> {
        self.send(Err(worker_error)).map_err(|e| anyhow!(e.to_string()))
    }
}
