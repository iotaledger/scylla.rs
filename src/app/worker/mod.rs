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
pub trait Worker: Send + std::fmt::Debug {
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
    Other(anyhow::Error),
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
pub trait HandleResponse<W> {
    /// Defines the response type
    type Response;
    /// Handle response for worker of type W
    fn handle_response(&self, response: Self::Response) -> anyhow::Result<()>;
}
/// should be implemented on the handle of the worker
pub trait HandleError<W> {
    /// Handle error for worker of type W
    fn handle_error(&self, worker_error: WorkerError) -> anyhow::Result<()>;
}

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
}

pub trait RespondingWorker<I, H, O>: Sized + Worker
where
    H: HandleResponse<Self, Response = O> + HandleError<Self>,
{
    fn handle(&self) -> &H;
    fn inbox(&self) -> &I;
}

impl<W, O> HandleResponse<W> for UnboundedSender<Result<O, WorkerError>> {
    type Response = O;
    fn handle_response(&self, response: Self::Response) -> anyhow::Result<()> {
        self.send(Ok(response)).map_err(|e| anyhow!(e.to_string()))
    }
}

impl<W, O> HandleError<W> for UnboundedSender<Result<O, WorkerError>> {
    fn handle_error(&self, worker_error: WorkerError) -> anyhow::Result<()> {
        self.send(Err(worker_error)).map_err(|e| anyhow!(e.to_string()))
    }
}

/// Handle an unprepared CQL error by sending a prepare
/// request and resubmitting the original query as an
/// unprepared statement
pub fn handle_unprepared_error<W, R>(worker: Box<W>, id: [u8; 16], reporter: &ReporterHandle) -> anyhow::Result<()>
where
    W: 'static + Worker + RetryableWorker<R>,
    R: Request,
{
    let statement = worker.request().statement();
    info!("Attempting to prepare statement '{}', id: '{:?}'", statement, id);
    let Prepare(payload) = Prepare::new().statement(&statement).build()?;
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
