// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

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
        CqlError,
        Decoder,
    },
};
use anyhow::anyhow;
pub use basic::{
    BasicRetryWorker,
    BasicWorker,
    SpawnableRespondWorker,
};
use log::*;
pub use prepare::PrepareWorker;
pub use select::SelectWorker;
use std::convert::TryFrom;
use thiserror::Error;
use tokio::sync::mpsc::UnboundedSender;
pub use value::ValueWorker;

mod basic;
mod prepare;
mod select;
mod value;

/// WorkerId trait type which will be implemented by worker in order to send their channel_tx.
pub trait Worker: Send + Sync + std::fmt::Debug {
    /// Reporter will invoke this method to Send the cql response to worker
    fn handle_response(self: Box<Self>, giveload: Vec<u8>) -> anyhow::Result<()>;
    /// Reporter will invoke this method to Send the worker error to worker
    fn handle_error(self: Box<Self>, error: WorkerError, reporter: &ReporterHandle) -> anyhow::Result<()>;
}

#[derive(Error, Debug)]
/// The CQL worker error.
pub enum WorkerError {
    /// The CQL Error reported from ScyllaDB.
    #[error("Worker CqlError: {0}")]
    Cql(CqlError),
    /// The IO Error.
    #[error(transparent)]
    Other(#[from] anyhow::Error),
    /// The overload when we do not have any more streams.
    #[error("Worker Overload")]
    Overload,
    /// We lost the worker due to the abortion of ScyllaDB connection.
    #[error("Worker Lost")]
    Lost,
    /// There is no ring initialized.
    #[error("Worker NoRing")]
    NoRing,
}

/// should be implemented on the handle of the worker
pub trait HandleResponse<O> {
    /// Handle response for worker of type W
    fn handle_response(self, response: O) -> anyhow::Result<()>;
}
/// should be implemented on the handle of the worker
pub trait HandleError {
    /// Handle error for worker of type W
    fn handle_error(self, worker_error: WorkerError) -> anyhow::Result<()>;
}

/// Defines a worker which contains enough information to retry on a failure
#[async_trait::async_trait]
pub trait RetryableWorker<R>: Worker {
    /// Get the number of retries remaining
    fn retries(&self) -> usize;
    /// Mutably access the number of retries remaining
    fn retries_mut(&mut self) -> &mut usize;
    /// Get the request
    fn request(&self) -> &R;
    /// Update the retry count
    fn with_retries(mut self: Box<Self>, retries: usize) -> Box<Self>
    where
        Self: Sized,
    {
        *self.retries_mut() = retries;
        self
    }
    /// Retry the worker
    fn retry(mut self: Box<Self>) -> Result<(), Box<Self>>
    where
        Self: 'static + Sized,
        R: Request,
    {
        if self.retries() > 0 {
            *self.retries_mut() -= 1;
            // currently we assume all cql/worker errors are retryable, but we might change this in future
            send_global(self.request().token(), self.request().payload(), self).ok();
            Ok(())
        } else {
            Err(self)
        }
    }

    /// Send the worker to a specific reporter, without waiting for a response
    fn send_to_reporter(self: Box<Self>, reporter: &ReporterHandle) -> Result<DecodeResult<R::Marker>, RequestError>
    where
        Self: 'static + Sized,
        R: SendRequestExt,
    {
        let request = ReporterEvent::Request {
            payload: self.request().payload(),
            worker: self,
        };
        reporter.send(request).map_err(|e| RingSendError::SendError(e))?;
        Ok(DecodeResult::new(R::Marker::new(), R::TYPE))
    }

    /// Send the worker to the local datacenter, without waiting for a response
    fn send_local(self: Box<Self>) -> Result<DecodeResult<R::Marker>, RequestError>
    where
        Self: 'static + Sized,
        R: SendRequestExt,
    {
        send_local(self.request().token(), self.request().payload(), self)?;
        Ok(DecodeResult::new(R::Marker::new(), R::TYPE))
    }

    /// Send the worker to a global datacenter, without waiting for a response
    fn send_global(self: Box<Self>) -> Result<DecodeResult<R::Marker>, RequestError>
    where
        Self: 'static + Sized,
        R: SendRequestExt,
    {
        send_global(self.request().token(), self.request().payload(), self)?;
        Ok(DecodeResult::new(R::Marker::new(), R::TYPE))
    }

    /// Send the worker to the local datacenter and await a response asynchronously
    async fn get_local(self: Box<Self>) -> Result<<R::Marker as Marker>::Output, RequestError>
    where
        R: SendRequestExt,
        Self: 'static + IntoRespondingWorker<R, tokio::sync::oneshot::Sender<Result<Decoder, WorkerError>>, Decoder>,
        R::Marker: Send + Sync,
    {
        let (handle, inbox) = tokio::sync::oneshot::channel();
        let marker = self.with_handle(handle).send_local()?;
        Ok(marker.try_decode(
            inbox
                .await
                .map_err(|e| anyhow::anyhow!("No response from worker: {}", e))??,
        )?)
    }

    /// Send the worker to the local datacenter and await a response synchronously
    fn get_local_blocking(self: Box<Self>) -> Result<<R::Marker as Marker>::Output, RequestError>
    where
        R: SendRequestExt,
        Self: 'static + IntoRespondingWorker<R, tokio::sync::oneshot::Sender<Result<Decoder, WorkerError>>, Decoder>,
    {
        let (handle, inbox) = tokio::sync::oneshot::channel();
        let marker = self.with_handle(handle).send_local()?;
        let blocking_inbox =
            tokio::task::block_in_place(move || tokio::runtime::Handle::current().block_on(async move { inbox.await }));
        Ok(marker.try_decode(blocking_inbox.map_err(|e| anyhow::anyhow!("No response from worker: {}", e))??)?)
    }

    /// Send the worker to a global datacenter and await a response asynchronously
    async fn get_global(self: Box<Self>) -> Result<<R::Marker as Marker>::Output, RequestError>
    where
        R: SendRequestExt,
        Self: 'static + IntoRespondingWorker<R, tokio::sync::oneshot::Sender<Result<Decoder, WorkerError>>, Decoder>,
        R::Marker: Send + Sync,
    {
        let (handle, inbox) = tokio::sync::oneshot::channel();
        let marker = self.with_handle(handle).send_global()?;
        Ok(marker.try_decode(
            inbox
                .await
                .map_err(|e| anyhow::anyhow!("No response from worker: {}", e))??,
        )?)
    }

    /// Send the worker to a global datacenter and await a response synchronously
    fn get_global_blocking(self: Box<Self>) -> Result<<R::Marker as Marker>::Output, RequestError>
    where
        R: SendRequestExt,
        Self: 'static + IntoRespondingWorker<R, tokio::sync::oneshot::Sender<Result<Decoder, WorkerError>>, Decoder>,
    {
        let (handle, inbox) = tokio::sync::oneshot::channel();
        let marker = self.with_handle(handle).send_global()?;
        let blocking_inbox =
            tokio::task::block_in_place(move || tokio::runtime::Handle::current().block_on(async move { inbox.await }));
        Ok(marker.try_decode(blocking_inbox.map_err(|e| anyhow::anyhow!("No response from worker: {}", e))??)?)
    }
}

/// Defines a worker which can be given a handle to be capable of responding to the sender
pub trait IntoRespondingWorker<R, H, O>
where
    H: HandleResponse<O> + HandleError,
    R: SendRequestExt,
{
    /// The type of worker which will be created
    type Output: RespondingWorker<R, H, O> + RetryableWorker<R>;
    /// Give the worker a handle
    fn with_handle(self: Box<Self>, handle: H) -> Box<Self::Output>;
}

/// A worker which can respond to a sender
#[async_trait::async_trait]
pub trait RespondingWorker<R, H, O>: Worker
where
    H: HandleResponse<O> + HandleError,
{
    /// Get the handle this worker will use to respond
    fn handle(&self) -> &H;
}

impl<O> HandleResponse<O> for UnboundedSender<Result<O, WorkerError>> {
    fn handle_response(self, response: O) -> anyhow::Result<()> {
        self.send(Ok(response)).map_err(|e| anyhow!(e.to_string()))
    }
}

impl<O> HandleError for UnboundedSender<Result<O, WorkerError>> {
    fn handle_error(self, worker_error: WorkerError) -> anyhow::Result<()> {
        self.send(Err(worker_error)).map_err(|e| anyhow!(e.to_string()))
    }
}

impl<O> HandleResponse<O> for tokio::sync::oneshot::Sender<Result<O, WorkerError>> {
    fn handle_response(self, response: O) -> anyhow::Result<()> {
        self.send(Ok(response)).map_err(|_| anyhow!("Failed to send response!"))
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
pub fn handle_unprepared_error<W, R>(worker: Box<W>, id: [u8; 16], reporter: &ReporterHandle) -> Result<(), Box<W>>
where
    W: 'static + Worker + RetryableWorker<R> + Send + Sync,
    R: Request,
{
    let statement = worker.request().statement();
    info!("Attempting to prepare statement '{}', id: '{:?}'", statement, id);
    PrepareWorker::new(id, statement).send_to_reporter(reporter).ok();
    let payload = worker.request().payload();
    let retry_request = ReporterEvent::Request { worker, payload };
    reporter.send(retry_request).ok();
    Ok(())
}
