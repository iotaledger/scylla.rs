// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0
use super::*;
use std::{
    fmt::Debug,
    marker::PhantomData,
};
use tokio::sync::mpsc::UnboundedReceiver;

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
}

impl<R, H> IntoRespondingWorker<R, H, Decoder> for BasicRetryWorker<R>
where
    H: 'static + HandleResponse<Decoder> + HandleError + Debug + Send + Sync,
    R: 'static + Send + Debug + Request + Sync + SendRequestExt,
{
    type Output = SelectWorker<H, R>;
    fn with_handle(self: Box<Self>, handle: H) -> Box<SelectWorker<H, R>> {
        SelectWorker::<H, R>::from(*self, handle)
    }
}
impl<H, V> IntoRespondingWorker<SelectRequest<V>, H, Option<V>> for BasicRetryWorker<SelectRequest<V>>
where
    H: 'static + HandleResponse<Option<V>> + HandleError + Debug + Send + Sync,
    V: 'static + Send + RowsDecoder + Debug,
{
    type Output = ValueWorker<H, V, SelectRequest<V>>;
    fn with_handle(self: Box<Self>, handle: H) -> Box<ValueWorker<H, V, SelectRequest<V>>> {
        ValueWorker::<H, V, SelectRequest<V>>::from(*self, handle)
    }
}

impl<R> From<R> for BasicRetryWorker<R> {
    fn from(request: R) -> Self {
        Self { request, retries: 0 }
    }
}

impl<R> Worker for BasicRetryWorker<R>
where
    R: 'static + Debug + Send + Request + Sync,
{
    fn handle_response(self: Box<Self>, giveload: Vec<u8>) -> anyhow::Result<()> {
        Decoder::try_from(giveload)?;
        Ok(())
    }

    fn handle_error(self: Box<Self>, mut error: WorkerError, reporter: &ReporterHandle) -> anyhow::Result<()> {
        error!("{}", error);
        if let WorkerError::Cql(ref mut cql_error) = error {
            if let Some(id) = cql_error.take_unprepared_id() {
                handle_unprepared_error(self, id, reporter).map_err(|worker| {
                    error!("Error trying to reprepare query: {}", worker.request().statement());
                    anyhow::anyhow!("Error trying to reprepare query!")
                })
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
    R: 'static + Debug + Send + Request + Sync,
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
    R: Request + Debug + Send,
    W: RetryableWorker<R> + Clone,
{
    pub fn new(inbox: I, worker: W) -> Box<Self> {
        Box::new(Self {
            inbox,
            worker,
            _req: PhantomData,
        })
    }

    fn send_local(self: Box<Self>) -> Result<DecodeResult<R::Marker>, RequestError>
    where
        Self: 'static + Sized,
        R: SendRequestExt,
    {
        Box::new(self.worker.clone()).send_local();
        Ok(DecodeResult::new(R::Marker::new(), R::TYPE))
    }
    fn send_global(self: Box<Self>) -> Result<DecodeResult<R::Marker>, RequestError>
    where
        Self: 'static + Sized,
        R: SendRequestExt,
    {
        Box::new(self.worker.clone()).send_global();
        Ok(DecodeResult::new(R::Marker::new(), R::TYPE))
    }
}

impl<R, W> SpawnableRespondWorker<R, UnboundedReceiver<Result<Decoder, WorkerError>>, W>
where
    R: 'static + SendRequestExt + Clone + Debug + Send + Sync,
    W: 'static + RetryableWorker<R> + Clone,
{
    pub async fn get_local(&mut self) -> Result<<R::Marker as Marker>::Output, RequestError>
    where
        Self: Sized,
    {
        let marker = Box::new(self.worker.clone()).send_local()?;
        Ok(marker.try_decode(
            self.inbox
                .recv()
                .await
                .ok_or_else(|| anyhow::anyhow!("No response from worker!"))??,
        )?)
    }

    pub fn get_local_blocking(&mut self) -> Result<<R::Marker as Marker>::Output, RequestError>
    where
        Self: Sized,
    {
        let marker = Box::new(self.worker.clone()).send_local()?;
        Ok(marker.try_decode(
            self.inbox
                .blocking_recv()
                .ok_or_else(|| anyhow::anyhow!("No response from worker!"))??,
        )?)
    }

    pub async fn get_global(&mut self) -> Result<<R::Marker as Marker>::Output, RequestError>
    where
        Self: Sized,
    {
        let marker = Box::new(self.worker.clone()).send_global()?;
        Ok(marker.try_decode(
            self.inbox
                .recv()
                .await
                .ok_or_else(|| anyhow::anyhow!("No response from worker!"))??,
        )?)
    }

    pub fn get_global_blocking(&mut self) -> Result<<R::Marker as Marker>::Output, RequestError>
    where
        Self: Sized,
    {
        let marker = Box::new(self.worker.clone()).send_global()?;
        Ok(marker.try_decode(
            self.inbox
                .blocking_recv()
                .ok_or_else(|| anyhow::anyhow!("No response from worker!"))??,
        )?)
    }
}
