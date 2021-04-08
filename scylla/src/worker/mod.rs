// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

pub use crate::stage::ReporterEvent;
use crate::{access::*, stage::ReporterHandle};
use anyhow::{anyhow, bail};
use log::*;
use scylla_cql::CqlError;
use select::handle_unprepared_error;
use std::convert::TryFrom;
use thiserror::Error;
use tokio::sync::mpsc::UnboundedSender;

/// WorkerId trait type which will be implemented by worker in order to send their channel_tx.
pub trait Worker: Send {
    /// Reporter will invoke this method to Send the cql response to worker
    fn handle_response(self: Box<Self>, giveload: Vec<u8>) -> anyhow::Result<()>;
    /// Reporter will invoke this method to Send the worker error to worker
    fn handle_error(self: Box<Self>, error: WorkerError, reporter: &Option<ReporterHandle>) -> anyhow::Result<()>;
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
pub trait HandleResponse<W: Worker + DecodeResponse<Self::Response>>: Send {
    /// Defines the response type
    type Response;
    /// Handle response for worker of type W
    fn handle_response(worker: Box<W>, response: Self::Response) -> anyhow::Result<()>;
}
/// should be implemented on the handle of the worker
pub trait HandleError<W: Worker>: Send {
    /// Handle error for worker of type W
    fn handle_error(worker: Box<W>, worker_error: WorkerError) -> anyhow::Result<()>;
}

/// Decode response as T
pub trait DecodeResponse<T> {
    /// Decode decoder into T type
    fn decode_response(decoder: Decoder) -> T;
}

impl<W: Worker> DecodeResponse<Decoder> for W {
    fn decode_response(decoder: Decoder) -> Decoder {
        decoder
    }
}

pub mod delete;
pub mod insert;
pub mod prepare;
pub mod select;
pub mod value;

pub use delete::DeleteWorker;
pub use insert::InsertWorker;
pub use prepare::PrepareWorker;
pub use select::SelectWorker;
pub use value::ValueWorker;
