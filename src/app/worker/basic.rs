use tokio::sync::mpsc::UnboundedReceiver;

use crate::prelude::RowsDecoder;

// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0
use super::*;
use std::{
    fmt::Debug,
    marker::PhantomData,
};

#[derive(Debug, Clone)]
pub struct BasicWorker;

impl BasicWorker {
    pub fn new() -> Box<Self> {
        Box::new(Self)
    }
}

impl Worker for BasicWorker {
    fn handle_response(self: Box<Self>, giveload: Vec<u8>) -> anyhow::Result<()> {
        Decoder::try_from(giveload)?;
        Ok(())
    }

    fn handle_error(self: Box<Self>, error: WorkerError, _reporter: &ReporterHandle) -> anyhow::Result<()> {
        anyhow::bail!(error);
    }
}

/// An insert request worker
pub struct BasicRetryWorker<R> {
    pub request: R,
    /// The number of times this worker will retry on failure
    pub retries: usize,
}

impl<R> Debug for BasicRetryWorker<R>
where
    R: Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BasicWorker")
            .field("request", &self.request)
            .field("retries", &self.retries)
            .finish()
    }
}

impl<R> Clone for BasicRetryWorker<R>
where
    R: Clone,
{
    fn clone(&self) -> Self {
        Self {
            request: self.request.clone(),
            retries: self.retries,
        }
    }
}

impl<R> BasicRetryWorker<R> {
    /// Create a new insert worker with a number of retries
    pub fn new(request: R) -> Box<Self> {
        Box::new(request.into())
    }

    pub fn with_retries(mut self: Box<Self>, retries: usize) -> Box<Self> {
        self.retries = retries;
        self
    }

    pub fn with_handle<H: 'static>(self: Box<Self>, handle: H) -> Box<SelectWorker<H, R>> {
        SelectWorker::<H, R>::from(*self, handle)
    }
}
impl<V> BasicRetryWorker<SelectRequest<V>>
where
    V: 'static + Send + RowsDecoder,
{
    pub fn with_value_handle<H>(self: Box<Self>, handle: H) -> Box<ValueWorker<H, V, SelectRequest<V>>> {
        ValueWorker::<H, V, SelectRequest<V>>::from(*self, handle)
    }

    pub async fn get_local(self: Box<Self>) -> Result<Option<V>, RequestError> {
        let (handle, inbox) = tokio::sync::mpsc::unbounded_channel::<Result<Option<V>, WorkerError>>();
        self.with_value_handle(handle).with_inbox(inbox).get_local().await
    }

    pub fn get_local_blocking(self: Box<Self>) -> Result<Option<V>, RequestError> {
        let (handle, inbox) = tokio::sync::mpsc::unbounded_channel::<Result<Option<V>, WorkerError>>();
        self.with_value_handle(handle).with_inbox(inbox).get_local_blocking()
    }

    pub async fn get_global(self: Box<Self>) -> Result<Option<V>, RequestError> {
        let (handle, inbox) = tokio::sync::mpsc::unbounded_channel::<Result<Option<V>, WorkerError>>();
        self.with_value_handle(handle).with_inbox(inbox).get_global().await
    }

    pub fn get_global_blocking(self: Box<Self>) -> Result<Option<V>, RequestError> {
        let (handle, inbox) = tokio::sync::mpsc::unbounded_channel::<Result<Option<V>, WorkerError>>();
        self.with_value_handle(handle).with_inbox(inbox).get_global_blocking()
    }
}

impl<R> From<R> for BasicRetryWorker<R> {
    fn from(request: R) -> Self {
        Self { request, retries: 0 }
    }
}

impl<R> Worker for BasicRetryWorker<R>
where
    R: 'static + Debug + Send + Clone + Request,
{
    fn handle_response(self: Box<Self>, giveload: Vec<u8>) -> anyhow::Result<()> {
        Decoder::try_from(giveload)?;
        Ok(())
    }

    fn handle_error(self: Box<Self>, mut error: WorkerError, reporter: &ReporterHandle) -> anyhow::Result<()> {
        if let WorkerError::Cql(ref mut cql_error) = error {
            if let Some(id) = cql_error.take_unprepared_id() {
                handle_unprepared_error(self, id, reporter)
                    .map_err(|e| anyhow::anyhow!("Error trying to prepare query: {}", e))
            } else {
                self.retry().map_err(|_| anyhow::anyhow!("Cannot retry!"))
            }
        } else {
            self.retry().map_err(|_| anyhow::anyhow!("Cannot retry!"))
        }
    }
}

impl<R> RetryableWorker<R> for BasicRetryWorker<R>
where
    R: 'static + Debug + Send + Clone + Request,
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

pub struct SpawnableRespondWorker<R, I, W> {
    pub inbox: I,
    pub worker: W,
    _req: PhantomData<fn(R) -> R>,
}

impl<R, I, W> SpawnableRespondWorker<R, I, W>
where
    R: Request + Clone + Debug + Send,
    W: RetryableWorker<R> + Clone,
{
    pub fn new(inbox: I, worker: W) -> Box<Self> {
        Box::new(Self {
            inbox,
            worker,
            _req: PhantomData,
        })
    }

    pub fn send_local(&self) -> Result<DecodeResult<R::Marker>, RequestError>
    where
        Self: 'static + Sized,
    {
        let worker = Box::new(self.worker.clone());
        send_local(worker.request().token(), worker.request().payload().clone(), worker)?;
        Ok(DecodeResult::new(R::marker(), R::TYPE))
    }

    pub fn send_global(&self) -> Result<DecodeResult<R::Marker>, RequestError>
    where
        Self: 'static + Sized,
    {
        let worker = Box::new(self.worker.clone());
        send_global(worker.request().token(), worker.request().payload().clone(), worker)?;
        Ok(DecodeResult::new(R::marker(), R::TYPE))
    }
}

#[async_trait::async_trait]
impl<R, W, O> GetRequestExt<O, R> for SpawnableRespondWorker<R, UnboundedReceiver<Result<O, WorkerError>>, W>
where
    R: 'static + Request + Clone + Debug + Send + Sync,
    W: 'static + RetryableWorker<R> + Clone + Send + Sync,
    O: 'static + Send,
{
    async fn get_local(&mut self) -> Result<O, RequestError>
    where
        Self: Sized,
    {
        self.send_local()?;
        Ok(self
            .inbox
            .recv()
            .await
            .ok_or_else(|| anyhow::anyhow!("No response from worker!"))??)
    }

    fn get_local_blocking(&mut self) -> Result<O, RequestError>
    where
        Self: Sized,
    {
        self.send_local()?;
        Ok(self
            .inbox
            .blocking_recv()
            .ok_or_else(|| anyhow::anyhow!("No response from worker!"))??)
    }

    async fn get_global(&mut self) -> Result<O, RequestError>
    where
        Self: Sized,
    {
        self.send_global()?;
        Ok(self
            .inbox
            .recv()
            .await
            .ok_or_else(|| anyhow::anyhow!("No response from worker!"))??)
    }

    fn get_global_blocking(&mut self) -> Result<O, RequestError>
    where
        Self: Sized,
    {
        self.send_global()?;
        Ok(self
            .inbox
            .blocking_recv()
            .ok_or_else(|| anyhow::anyhow!("No response from worker!"))??)
    }
}
