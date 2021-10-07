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
pub trait HandleResponse<W: Worker> {
    /// Defines the response type
    type Response;
    /// Handle response for worker of type W
    fn handle_response(&self, response: Self::Response) -> anyhow::Result<()>;
}
/// should be implemented on the handle of the worker
pub trait HandleError<W: Worker> {
    /// Handle error for worker of type W
    fn handle_error(&self, worker_error: WorkerError) -> anyhow::Result<()>;
}

pub trait RetryableWorker<R, O>: Worker
where
    R: 'static + GetRequestExt<O> + Clone + Send,
    <R as SendRequestExt>::Marker: 'static,
    O: Send,
{
    fn retries(&mut self) -> &mut usize;
    fn request(&self) -> Option<&R>;

    fn retry(mut self: Box<Self>) -> Result<(), Box<Self>>
    where
        Self: 'static + Sized,
    {
        if *self.retries() > 0 && self.request().is_some() {
            *self.retries() -= 1;
            // currently we assume all cql/worker errors are retryable, but we might change this in future
            let req = self.request().cloned().unwrap();
            tokio::spawn(async { req.send_global(self) });
            Ok(())
        } else {
            Err(self)
        }
    }
}

/// Handle an unprepared CQL error by sending a prepare
/// request and resubmitting the original query as an
/// unprepared statement
pub fn handle_unprepared_error<W, R, O>(
    worker: Box<W>,
    request: R,
    id: [u8; 16],
    reporter: &ReporterHandle,
) -> anyhow::Result<()>
where
    W: 'static + Worker,
    R: GetRequestExt<O>,
    O: Send,
{
    let statement = request.statement();
    info!("Attempting to prepare statement '{}', id: '{:?}'", statement, id);
    let Prepare(payload) = Prepare::new().statement(&statement).build()?;
    let prepare_worker = Box::new(PrepareWorker::new(id, statement));
    let prepare_request = ReporterEvent::Request {
        worker: prepare_worker,
        payload,
    };
    reporter.send(prepare_request).ok();
    let payload = request.into_payload();
    let retry_request = ReporterEvent::Request { worker, payload };
    reporter.send(retry_request).ok();
    Ok(())
}
