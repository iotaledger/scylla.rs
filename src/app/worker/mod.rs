// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

pub use crate::app::stage::reporter::{
    ReporterEvent,
    ReporterHandle,
};
use crate::{
    app::access::*,
    cql::{
        CqlError,
        Decoder,
        Prepare,
        RowsDecoder,
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

#[async_trait::async_trait]
pub trait RetryableWorker<R>: Worker {
    fn retries(&self) -> usize;
    fn retries_mut(&mut self) -> &mut usize;
    fn request(&self) -> &R;
    fn with_retries(mut self, retries: usize) -> Self
    where
        Self: Sized,
    {
        *self.retries_mut() = retries;
        self
    }
    fn retry(mut self: Box<Self>) -> Result<(), Box<Self>>
    where
        Self: 'static + Sized,
        R: Request,
    {
        if self.retries() > 0 {
            *self.retries_mut() -= 1;
            // currently we assume all cql/worker errors are retryable, but we might change this in future
            send_global(self.request().token(), self.request().payload().clone(), self);
            Ok(())
        } else {
            Err(self)
        }
    }
    fn send_local(self: Box<Self>) -> Result<DecodeResult<R::Marker>, RequestError>
    where
        Self: 'static + Sized,
        R: SendRequestExt,
    {
        send_local(self.request().token(), self.request().payload().clone(), self)?;
        Ok(DecodeResult::new(R::Marker::new(), R::TYPE))
    }
    fn send_global(self: Box<Self>) -> Result<DecodeResult<R::Marker>, RequestError>
    where
        Self: 'static + Sized,
        R: SendRequestExt,
    {
        send_global(self.request().token(), self.request().payload().clone(), self)?;
        Ok(DecodeResult::new(R::Marker::new(), R::TYPE))
    }

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

    fn get_local_blocking(self: Box<Self>) -> Result<<R::Marker as Marker>::Output, RequestError>
    where
        R: SendRequestExt,
        Self: 'static + IntoRespondingWorker<R, tokio::sync::oneshot::Sender<Result<Decoder, WorkerError>>, Decoder>,
    {
        let (handle, inbox) = tokio::sync::oneshot::channel();
        let marker = self.with_handle(handle).send_local()?;
        Ok(marker.try_decode(
            futures::executor::block_on(inbox).map_err(|e| anyhow::anyhow!("No response from worker: {}", e))??,
        )?)
    }

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

    fn get_global_blocking(self: Box<Self>) -> Result<<R::Marker as Marker>::Output, RequestError>
    where
        R: SendRequestExt,
        Self: 'static + IntoRespondingWorker<R, tokio::sync::oneshot::Sender<Result<Decoder, WorkerError>>, Decoder>,
    {
        let (handle, inbox) = tokio::sync::oneshot::channel();
        let marker = self.with_handle(handle).send_global()?;
        Ok(marker.try_decode(
            futures::executor::block_on(inbox).map_err(|e| anyhow::anyhow!("No response from worker: {}", e))??,
        )?)
    }
}

pub trait IntoRespondingWorker<R, H, O>
where
    H: HandleResponse<O> + HandleError,
    R: SendRequestExt,
{
    type Output: RespondingWorker<R, H, O> + RetryableWorker<R>;
    fn with_handle(self: Box<Self>, handle: H) -> Box<Self::Output>;
}

#[async_trait::async_trait]
pub trait RespondingWorker<R, H, O>: Worker
where
    H: HandleResponse<O> + HandleError,
{
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
    let Prepare(payload) = match Prepare::new().statement(&statement).build() {
        Ok(p) => p,
        Err(_) => return Err(worker),
    };
    let prepare_worker = Box::new(PrepareWorker::new(id, statement));
    let prepare_request = ReporterEvent::Request {
        worker: prepare_worker,
        payload,
    };
    reporter.send(prepare_request).ok();
    let payload = worker.request().payload().clone();
    let retry_request = ReporterEvent::Request { worker, payload };
    reporter.send(retry_request).ok();
    Ok(())
}
