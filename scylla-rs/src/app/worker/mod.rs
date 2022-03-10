// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

mod basic;
mod batch;
mod prepare;
mod respond;

pub use crate::app::stage::reporter::{
    ReporterEvent,
    ReporterHandle,
};
use crate::{
    app::{
        access::*,
        ring::RingSendError,
    },
    cql::{
        ErrorCode,
        ErrorFrame,
    },
    prelude::{
        FrameError,
        ResponseBody,
    },
};
use anyhow::anyhow;
pub use basic::*;
pub use batch::*;
use log::*;
pub use prepare::*;
pub use respond::*;
use std::fmt::Debug;
use thiserror::Error;
use tokio::sync::mpsc::UnboundedSender;

/// WorkerId trait type which will be implemented by worker in order to send their channel_tx.
pub trait Worker: Send + Sync + std::fmt::Debug + 'static {
    /// Reporter will invoke this method to Send the cql response to worker
    fn handle_response(self: Box<Self>, body: ResponseBody) -> anyhow::Result<()>;
    /// Reporter will invoke this method to Send the worker error to worker
    fn handle_error(self: Box<Self>, error: WorkerError, reporter: &ReporterHandle) -> anyhow::Result<()>;
}

impl<W: Worker + ?Sized> Worker for Box<W> {
    fn handle_response(self: Box<Self>, body: ResponseBody) -> anyhow::Result<()> {
        (*self).handle_response(body)
    }

    fn handle_error(self: Box<Self>, error: WorkerError, reporter: &ReporterHandle) -> anyhow::Result<()> {
        (*self).handle_error(error, reporter)
    }
}

#[derive(Error, Debug)]
/// The CQL worker error.
pub enum WorkerError {
    /// The CQL Error reported from ScyllaDB.
    #[error(transparent)]
    Cql(ErrorFrame),
    /// The overload when we do not have any more streams.
    #[error("Overloaded Worker")]
    Overload,
    /// We lost the worker due to the abortion of ScyllaDB connection.
    #[error("Lost Worker")]
    Lost,
    /// There is no ring initialized.
    #[error("No Ring Available")]
    NoRing,
    /// An error with the response frame
    #[error(transparent)]
    FrameError(#[from] FrameError),
    /// Misc errors
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

/// should be implemented on the handle of the worker
pub trait HandleResponse {
    /// Handle response for worker of type W
    fn handle_response(self, body: ResponseBody) -> anyhow::Result<()>;
}
/// should be implemented on the handle of the worker
pub trait HandleError {
    /// Handle error for worker of type W
    fn handle_error(self, worker_error: WorkerError) -> anyhow::Result<()>;
}

/// Defines a worker which contains enough information to retry on a failure
pub trait RetryableWorker<R: SendRequestExt + Clone>: Worker {
    /// Get the number of retries remaining
    fn retries(&self) -> usize;
    /// Mutably access the number of retries remaining
    fn retries_mut(&mut self) -> &mut usize;
    /// Get the request
    fn request(&self) -> &R;
    /// Update the retry count
    fn with_retries(mut self, retries: usize) -> Self
    where
        Self: Sized,
    {
        *self.retries_mut() = retries;
        self
    }
    /// Retry the worker
    fn retry(mut self, reporter: &ReporterHandle) -> Result<(), Self>
    where
        Self: 'static + Sized,
    {
        if self.retries() > 0 {
            *self.retries_mut() -= 1;
            // currently we assume all cql/worker errors are retryable, but we might change this in future
            self.send_to_reporter(reporter).ok();
            Ok(())
        } else {
            Err(self)
        }
    }

    /// Send the worker to a specific reporter, without waiting for a response
    fn send_to_reporter(self, reporter: &ReporterHandle) -> Result<DecodeResult<R::Marker>, RequestError>
    where
        Self: 'static + Sized,
    {
        let marker = self.request().marker();
        let request = ReporterEvent::Request {
            frame: self.request().frame().clone().into(),
            worker: Box::new(self),
        };
        reporter.send(request).map_err(|e| RingSendError::SendError(e))?;
        Ok(DecodeResult::new(marker, R::TYPE))
    }

    /// Send the worker to the local datacenter, without waiting for a response
    fn send_local(self) -> Result<DecodeResult<R::Marker>, RequestError>
    where
        Self: 'static + Sized,
        R: ShardAwareExt,
    {
        let marker = self.request().marker();
        let req = self.request().clone();
        send_local(req.keyspace().cloned().as_deref(), req.token(), req.into_frame(), self)?;
        Ok(DecodeResult::new(marker, R::TYPE))
    }

    /// Send the worker to a global datacenter, without waiting for a response
    fn send_global(self) -> Result<DecodeResult<R::Marker>, RequestError>
    where
        Self: 'static + Sized,
        R: ShardAwareExt,
    {
        let marker = self.request().marker();
        let req = self.request().clone();
        send_local(req.keyspace().cloned().as_deref(), req.token(), req.into_frame(), self)?;
        Ok(DecodeResult::new(marker, R::TYPE))
    }
}

/// Defines helper functions for getting query results with a worker
#[async_trait::async_trait]
pub trait GetWorkerExt<R, H>: RetryableWorker<R>
where
    R: SendRequestExt + ShardAwareExt + Clone,
    H: HandleResponse + HandleError,
{
    /// Send the worker to the local datacenter and await a response asynchronously
    async fn get_local(self) -> Result<<R::Marker as Marker>::Output, RequestError>
    where
        R::Marker: Send + Sync;

    /// Send the worker to the local datacenter and await a response synchronously
    fn get_local_blocking(self) -> Result<<R::Marker as Marker>::Output, RequestError>;

    /// Send the worker to a global datacenter and await a response asynchronously
    async fn get_global(self) -> Result<<R::Marker as Marker>::Output, RequestError>
    where
        R::Marker: Send + Sync;

    /// Send the worker to a global datacenter and await a response synchronously
    fn get_global_blocking(self) -> Result<<R::Marker as Marker>::Output, RequestError>;
}
#[async_trait::async_trait]
impl<W, R> GetWorkerExt<R, tokio::sync::oneshot::Sender<Result<ResponseBody, WorkerError>>> for W
where
    R: SendRequestExt + ShardAwareExt + Clone,
    W: RetryableWorker<R> + IntoRespondingWorker<R, tokio::sync::oneshot::Sender<Result<ResponseBody, WorkerError>>>,
{
    async fn get_local(self) -> Result<<R::Marker as Marker>::Output, RequestError>
    where
        R::Marker: Send + Sync,
    {
        let (handle, inbox) = tokio::sync::oneshot::channel();
        let marker = self.with_handle(handle).send_local()?;
        Ok(marker.inner.try_decode(
            inbox
                .await
                .map_err(|e| anyhow::anyhow!("No response from worker: {}", e))??,
        )?)
    }

    fn get_local_blocking(self) -> Result<<R::Marker as Marker>::Output, RequestError> {
        let (handle, inbox) = tokio::sync::oneshot::channel();
        let marker = self.with_handle(handle).send_local()?;
        Ok(marker.inner.try_decode(
            futures::executor::block_on(inbox).map_err(|e| anyhow::anyhow!("No response from worker: {}", e))??,
        )?)
    }

    async fn get_global(self) -> Result<<R::Marker as Marker>::Output, RequestError>
    where
        R::Marker: Send + Sync,
    {
        let (handle, inbox) = tokio::sync::oneshot::channel();
        let marker = self.with_handle(handle).send_global()?;
        Ok(marker.inner.try_decode(
            inbox
                .await
                .map_err(|e| anyhow::anyhow!("No response from worker: {}", e))??,
        )?)
    }

    fn get_global_blocking(self) -> Result<<R::Marker as Marker>::Output, RequestError> {
        let (handle, inbox) = tokio::sync::oneshot::channel();
        let marker = self.with_handle(handle).send_global()?;
        Ok(marker.inner.try_decode(
            futures::executor::block_on(inbox).map_err(|e| anyhow::anyhow!("No response from worker: {}", e))??,
        )?)
    }
}
#[async_trait::async_trait]
impl<W, R> GetWorkerExt<R, UnboundedSender<Result<ResponseBody, WorkerError>>> for W
where
    R: SendRequestExt + ShardAwareExt + Clone,
    W: RetryableWorker<R> + IntoRespondingWorker<R, UnboundedSender<Result<ResponseBody, WorkerError>>>,
{
    async fn get_local(self) -> Result<<R::Marker as Marker>::Output, RequestError>
    where
        R::Marker: Send + Sync,
    {
        let (handle, mut inbox) = tokio::sync::mpsc::unbounded_channel();
        let marker = self.with_handle(handle).send_local()?;
        Ok(marker.inner.try_decode(
            inbox
                .recv()
                .await
                .ok_or_else(|| anyhow::anyhow!("No response from worker"))??,
        )?)
    }

    fn get_local_blocking(self) -> Result<<R::Marker as Marker>::Output, RequestError> {
        let (handle, mut inbox) = tokio::sync::mpsc::unbounded_channel();
        let marker = self.with_handle(handle).send_local()?;
        Ok(marker.inner.try_decode(
            inbox
                .blocking_recv()
                .ok_or_else(|| anyhow::anyhow!("No response from worker"))??,
        )?)
    }

    async fn get_global(self) -> Result<<R::Marker as Marker>::Output, RequestError>
    where
        R::Marker: Send + Sync,
    {
        let (handle, mut inbox) = tokio::sync::mpsc::unbounded_channel();
        let marker = self.with_handle(handle).send_global()?;
        Ok(marker.inner.try_decode(
            inbox
                .recv()
                .await
                .ok_or_else(|| anyhow::anyhow!("No response from worker"))??,
        )?)
    }

    fn get_global_blocking(self) -> Result<<R::Marker as Marker>::Output, RequestError> {
        let (handle, mut inbox) = tokio::sync::mpsc::unbounded_channel();
        let marker = self.with_handle(handle).send_global()?;
        Ok(marker.inner.try_decode(
            inbox
                .blocking_recv()
                .ok_or_else(|| anyhow::anyhow!("No response from worker"))??,
        )?)
    }
}

/// Defines a worker which can be given a handle to be capable of responding to the sender
pub trait IntoRespondingWorker<R, H>
where
    H: HandleResponse + HandleError,
    R: SendRequestExt + Clone,
{
    /// The type of worker which will be created
    type Output: RespondingWorker<R, H> + RetryableWorker<R>;
    /// Give the worker a handle
    fn with_handle(self, handle: H) -> Self::Output;
}

/// A worker which can respond to a sender
#[async_trait::async_trait]
pub trait RespondingWorker<R, H>: Worker
where
    H: HandleResponse + HandleError,
{
    /// Get the handle this worker will use to respond
    fn handle(&self) -> &H;
}

impl HandleResponse for UnboundedSender<Result<ResponseBody, WorkerError>> {
    fn handle_response(self, body: ResponseBody) -> anyhow::Result<()> {
        self.send(Ok(body)).map_err(|e| anyhow!(e.to_string()))
    }
}

impl<O> HandleError for UnboundedSender<Result<O, WorkerError>> {
    fn handle_error(self, worker_error: WorkerError) -> anyhow::Result<()> {
        self.send(Err(worker_error)).map_err(|e| anyhow!(e.to_string()))
    }
}

impl HandleResponse for tokio::sync::oneshot::Sender<Result<ResponseBody, WorkerError>> {
    fn handle_response(self, body: ResponseBody) -> anyhow::Result<()> {
        self.send(Ok(body)).map_err(|_| anyhow!("Failed to send response!"))
    }
}

impl<O> HandleError for tokio::sync::oneshot::Sender<Result<O, WorkerError>> {
    fn handle_error(self, worker_error: WorkerError) -> anyhow::Result<()> {
        self.send(Err(worker_error))
            .map_err(|_| anyhow!("Failed to send error response!"))
    }
}

/// Handle an unprepared CQL error by sending a prepare
/// request and resubmitting the original query as an
/// unprepared statement
pub fn handle_unprepared_error<W, R>(
    statement: String,
    worker: &mut Option<W>,
    reporter: &ReporterHandle,
) -> anyhow::Result<()>
where
    W: 'static + Worker + RetryableWorker<R> + Send + Sync,
    R: RequestFrameExt + SendRequestExt + Clone,
{
    info!("Attempting to prepare statement '{}'", statement);
    PrepareWorker::<PreparedQuery>::new(PrepareRequest::new(PrepareFrame::new(statement), 0, None))
        .send_to_reporter(reporter)?;
    worker.take().unwrap().send_to_reporter(reporter)?;
    Ok(())
}

/// retry to send a request
pub fn retry_send(keyspace: &str, mut r: RingSendError, mut retries: u8) -> Result<(), Box<dyn Worker>> {
    loop {
        if let ReporterEvent::Request { worker, frame } = r.into() {
            if retries > 0 {
                if let Err(still_error) = send_global(Some(keyspace), rand::random(), frame, worker) {
                    r = still_error;
                    retries -= 1;
                } else {
                    break;
                };
            } else {
                return Err(worker);
            }
        } else {
            unreachable!("the reporter variant must be request");
        }
    }
    Ok(())
}
