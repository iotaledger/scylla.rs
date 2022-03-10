use crate::prelude::ErrorCode;

// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0
use super::*;
use std::marker::PhantomData;
use tokio::sync::mpsc::UnboundedReceiver;

/// A basic worker which cannot respond or retry
#[derive(Debug, Clone)]
pub struct BasicWorker;

impl BasicWorker {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self
    }
}

impl Worker for BasicWorker {
    fn handle_response(self: Box<Self>, _body: ResponseBody) -> anyhow::Result<()> {
        Ok(())
    }

    fn handle_error(self: Box<Self>, error: WorkerError, _reporter: &ReporterHandle) -> anyhow::Result<()> {
        anyhow::bail!(error);
    }
}

/// A basic worker which can retry on failure
#[derive(Debug, Clone)]
pub struct QueryRetryWorker<R> {
    /// The worker's request
    pub request: R,
    /// The number of times this worker will retry on failure
    pub retries: usize,
}

impl<R> QueryRetryWorker<R> {
    /// Create a new insert worker with a number of retries
    pub fn new(request: R) -> Self {
        request.into()
    }
}

impl<R, H> IntoRespondingWorker<R, H> for QueryRetryWorker<R>
where
    H: 'static + HandleResponse + HandleError + Debug + Send + Sync,
    R: 'static + Send + Debug + Sync + SendRequestExt + Clone,
{
    type Output = RespondingQueryWorker<H, R>;
    fn with_handle(self, handle: H) -> RespondingQueryWorker<H, R> {
        RespondingQueryWorker::<H, R>::from(self, handle)
    }
}

impl<R> From<R> for QueryRetryWorker<R> {
    fn from(request: R) -> Self {
        Self { request, retries: 3 }
    }
}

impl<R> Worker for QueryRetryWorker<R>
where
    R: 'static + Debug + Send + SendRequestExt + Clone + Sync,
{
    fn handle_response(self: Box<Self>, _body: ResponseBody) -> anyhow::Result<()> {
        Ok(())
    }

    fn handle_error(self: Box<Self>, error: WorkerError, reporter: &ReporterHandle) -> anyhow::Result<()> {
        error!("{}", error);
        if let WorkerError::Cql(ref cql_error) = error {
            match cql_error.code() {
                ErrorCode::Unprepared => {
                    anyhow::bail!("Unprepared query!")
                }
                _ => self.retry(reporter).map_err(|_| anyhow::anyhow!("Cannot retry!")),
            }
        } else {
            self.retry(reporter).map_err(|_| anyhow::anyhow!("Cannot retry!"))
        }
    }
}

impl<R> RetryableWorker<R> for QueryRetryWorker<R>
where
    R: 'static + Debug + Send + SendRequestExt + Clone + Sync,
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

/// A basic worker which can retry on failure
#[derive(Debug, Clone)]
pub struct ExecuteRetryWorker<R> {
    /// The worker's request
    pub request: R,
    /// The number of times this worker will retry on failure
    pub retries: usize,
}

impl<R> ExecuteRetryWorker<R> {
    /// Create a new insert worker with a number of retries
    pub fn new(request: R) -> Self {
        request.into()
    }

    /// Convert this to a basic worker
    pub fn convert(self) -> QueryRetryWorker<R::OutRequest>
    where
        R: ReprepareExt,
    {
        QueryRetryWorker {
            request: self.request.convert(),
            retries: self.retries,
        }
    }
}

impl<R, H> IntoRespondingWorker<R, H> for ExecuteRetryWorker<R>
where
    H: 'static + HandleResponse + HandleError + Debug + Send + Sync,
    R: 'static + Send + Debug + Sync + SendRequestExt + Clone + RequestFrameExt<Frame = ExecuteFrame> + ReprepareExt,
{
    type Output = RespondingExecuteWorker<H, R>;
    fn with_handle(self, handle: H) -> RespondingExecuteWorker<H, R> {
        RespondingExecuteWorker::<H, R>::from(self, handle)
    }
}

impl<R> From<R> for ExecuteRetryWorker<R> {
    fn from(request: R) -> Self {
        Self { request, retries: 3 }
    }
}

impl<R> Worker for ExecuteRetryWorker<R>
where
    R: 'static + Debug + Send + SendRequestExt + Clone + RequestFrameExt<Frame = ExecuteFrame> + ReprepareExt + Sync,
    R::OutRequest: Send + Sync,
{
    fn handle_response(self: Box<Self>, _body: ResponseBody) -> anyhow::Result<()> {
        Ok(())
    }

    fn handle_error(self: Box<Self>, error: WorkerError, reporter: &ReporterHandle) -> anyhow::Result<()> {
        error!("{}", error);
        if let WorkerError::Cql(ref cql_error) = error {
            match cql_error.code() {
                ErrorCode::Unprepared => {
                    let statement = self.request().statement().clone();
                    let worker = self.convert();
                    let mut worker_opt = Some(worker);
                    handle_unprepared_error(statement, &mut worker_opt, reporter)?;
                    if let Some(worker) = worker_opt {
                        worker.retry(reporter).map_err(|_| anyhow::anyhow!("Cannot retry!"))
                    } else {
                        Ok(())
                    }
                }
                _ => self.retry(reporter).map_err(|_| anyhow::anyhow!("Cannot retry!")),
            }
        } else {
            self.retry(reporter).map_err(|_| anyhow::anyhow!("Cannot retry!"))
        }
    }
}

impl<R> RetryableWorker<R> for ExecuteRetryWorker<R>
where
    R: 'static + Debug + Send + SendRequestExt + Clone + RequestFrameExt<Frame = ExecuteFrame> + ReprepareExt + Sync,
    R::OutRequest: Send + Sync,
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

/// A worker which can spawn cloneable workers and await their responses
pub struct SpawnableRespondWorker<R, I, W> {
    pub(crate) inbox: I,
    pub(crate) worker: W,
    _req: PhantomData<fn(R) -> R>,
}

impl<R, I, W> SpawnableRespondWorker<R, I, W>
where
    R: SendRequestExt + Clone + Debug + Send,
    W: RetryableWorker<R> + Clone,
{
    /// Create a new spawning worker with an inbox and a worker to spawn
    pub fn new(inbox: I, worker: W) -> Self {
        Self {
            inbox,
            worker,
            _req: PhantomData,
        }
    }

    /// Send a spawned worker to a specific reporter, without waiting for a response
    pub fn send_to_reporter(self, reporter: &ReporterHandle) -> Result<DecodeResult<R::Marker>, RequestError>
    where
        Self: 'static + Sized,
        R: SendRequestExt,
    {
        self.worker.clone().send_to_reporter(reporter)?;
        Ok(DecodeResult::new(self.worker.request().marker(), R::TYPE))
    }

    /// Send a spawned worker to the local datacenter, without waiting for a response
    pub fn send_local(self) -> Result<DecodeResult<R::Marker>, RequestError>
    where
        Self: 'static + Sized,
        R: SendRequestExt + ShardAwareExt,
    {
        self.worker.clone().send_local()?;
        Ok(DecodeResult::new(self.worker.request().marker(), R::TYPE))
    }

    /// Send a spawned worker to a global datacenter, without waiting for a response
    pub fn send_global(self) -> Result<DecodeResult<R::Marker>, RequestError>
    where
        Self: 'static + Sized,
        R: SendRequestExt + ShardAwareExt,
    {
        self.worker.clone().send_global()?;
        Ok(DecodeResult::new(self.worker.request().marker(), R::TYPE))
    }
}

impl<R, W> SpawnableRespondWorker<R, UnboundedReceiver<Result<ResponseBody, WorkerError>>, W>
where
    R: 'static + SendRequestExt + ShardAwareExt + Clone + Debug + Send + Sync,
    W: 'static + RetryableWorker<R> + Clone,
{
    /// Send a spawned worker to the local datacenter and await a response asynchronously
    pub async fn get_local(&mut self) -> Result<<R::Marker as Marker>::Output, RequestError>
    where
        Self: Sized,
    {
        let marker = self.worker.clone().send_local()?;
        Ok(marker.inner.try_decode(
            self.inbox
                .recv()
                .await
                .ok_or_else(|| anyhow::anyhow!("No response from worker!"))??,
        )?)
    }

    /// Send a spawned worker to the local datacenter and await a response synchronously
    pub fn get_local_blocking(&mut self) -> Result<<R::Marker as Marker>::Output, RequestError>
    where
        Self: Sized,
    {
        let marker = self.worker.clone().send_local()?;
        Ok(marker.inner.try_decode(
            self.inbox
                .blocking_recv()
                .ok_or_else(|| anyhow::anyhow!("No response from worker!"))??,
        )?)
    }

    /// Send a spawned worker to a global datacenter and await a response asynchronously
    pub async fn get_global(&mut self) -> Result<<R::Marker as Marker>::Output, RequestError>
    where
        Self: Sized,
    {
        let marker = self.worker.clone().send_global()?;
        Ok(marker.inner.try_decode(
            self.inbox
                .recv()
                .await
                .ok_or_else(|| anyhow::anyhow!("No response from worker!"))??,
        )?)
    }

    /// Send a spawned worker to a global datacenter and await a response synchronously
    pub fn get_global_blocking(&mut self) -> Result<<R::Marker as Marker>::Output, RequestError>
    where
        Self: Sized,
    {
        let marker = self.worker.clone().send_global()?;
        Ok(marker.inner.try_decode(
            self.inbox
                .blocking_recv()
                .ok_or_else(|| anyhow::anyhow!("No response from worker!"))??,
        )?)
    }
}
