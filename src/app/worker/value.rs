// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::prelude::RowsDecoder;
use std::{
    fmt::Debug,
    marker::PhantomData,
};
/// A value selecting worker
pub struct ValueWorker<H, V, R> {
    /// The worker's request
    pub request: R,
    /// A handle which can be used to return the queried value
    pub handle: H,
    /// The query page size, used when retying due to failure
    pub page_size: Option<i32>,
    /// The query paging state, used when retrying due to failure
    pub paging_state: Option<Vec<u8>>,
    /// The number of times this worker will retry on failure
    pub retries: usize,
    _val: PhantomData<fn(V) -> V>,
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

impl<H, V, R> Clone for ValueWorker<H, V, R>
where
    R: Clone,
    H: Clone,
{
    fn clone(&self) -> Self {
        Self {
            request: self.request.clone(),
            handle: self.handle.clone(),
            page_size: self.page_size.clone(),
            paging_state: self.paging_state.clone(),
            retries: self.retries.clone(),
            _val: PhantomData,
        }
    }
}

impl<H, V, R> ValueWorker<H, V, R> {
    /// Create a new value worker from a request and a handle
    pub fn new(request: R, handle: H) -> Box<Self> {
        Box::new(Self {
            request,
            handle,
            page_size: None,
            paging_state: None,
            retries: 0,
            _val: PhantomData,
        })
    }

    pub(crate) fn from(BasicRetryWorker { request, retries }: BasicRetryWorker<R>, handle: H) -> Box<Self>
    where
        H: 'static + HandleResponse<Option<V>> + HandleError + Debug + Send + Sync,
        R: 'static + Request + Debug + Send + Sync,
        V: 'static + RowsDecoder + Send,
    {
        Self::new(request, handle).with_retries(retries)
    }

    /// Add paging information to this worker
    pub fn with_paging<P: Into<Option<Vec<u8>>>>(mut self: Box<Self>, page_size: i32, paging_state: P) -> Box<Self> {
        self.page_size = Some(page_size);
        self.paging_state = paging_state.into();
        self
    }
}
impl<H, V, R> ValueWorker<H, V, R>
where
    V: 'static + Send + RowsDecoder,
    R: 'static + Send + Debug + Clone + Request + Sync,
    H: 'static + HandleResponse<Option<V>> + HandleError + Debug + Clone + Send + Sync,
{
    /// Give the worker an inbox and create a spawning worker
    pub fn with_inbox<I>(self: Box<Self>, inbox: I) -> Box<SpawnableRespondWorker<R, I, Self>> {
        SpawnableRespondWorker::new(inbox, *self)
    }
}

impl<H, V, R> Worker for ValueWorker<H, V, R>
where
    V: 'static + Send + RowsDecoder,
    H: 'static + HandleResponse<Option<V>> + HandleError + Debug + Send + Sync,
    R: 'static + Send + Debug + Request + Sync,
{
    fn handle_response(self: Box<Self>, giveload: Vec<u8>) -> anyhow::Result<()> {
        match Decoder::try_from(giveload) {
            Ok(decoder) => match V::try_decode_rows(decoder) {
                Ok(res) => self.handle.handle_response(res),
                Err(e) => self.handle.handle_error(WorkerError::Other(e)),
            },
            Err(e) => self.handle.handle_error(WorkerError::Other(e)),
        }
    }

    fn handle_error(self: Box<Self>, mut error: WorkerError, reporter: &ReporterHandle) -> anyhow::Result<()> {
        error!("{}", error);
        if let WorkerError::Cql(ref mut cql_error) = error {
            if let Some(id) = cql_error.take_unprepared_id() {
                handle_unprepared_error(self, id, reporter).or_else(|worker| {
                    error!("Error trying to reprepare query: {}", worker.request().statement());
                    worker.handle.handle_error(error)
                })
            } else {
                match self.retry() {
                    Ok(_) => Ok(()),
                    Err(worker) => worker.handle.handle_error(error),
                }
            }
        } else {
            match self.retry() {
                Ok(_) => Ok(()),
                Err(worker) => worker.handle.handle_error(error),
            }
        }
    }
}

impl<H, V, R> RetryableWorker<R> for ValueWorker<H, V, R>
where
    H: 'static + HandleResponse<Option<V>> + HandleError + Debug + Send + Sync,
    V: 'static + Send + RowsDecoder,
    R: 'static + Send + Debug + Request + Sync,
{
    fn retries(&self) -> usize {
        self.retries
    }

    fn request(&self) -> &R {
        &self.request
    }

    fn retries_mut(&mut self) -> &mut usize {
        &mut self.retries
    }
}

impl<R, H, V> RespondingWorker<R, H, Option<V>> for ValueWorker<H, V, R>
where
    H: 'static + HandleResponse<Option<V>> + HandleError + Debug + Send + Sync,
    R: 'static + Send + Debug + Request + Sync,
    V: 'static + Send + RowsDecoder,
{
    fn handle(&self) -> &H {
        &self.handle
    }
}
