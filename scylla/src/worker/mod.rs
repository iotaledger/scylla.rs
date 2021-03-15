// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

pub use crate::stage::ReporterEvent;
use crate::{access::*, stage::ReporterHandle};
use log::*;
use scylla_cql::CqlError;
use std::io::Error;

/// WorkerId trait type which will be implemented by worker in order to send their channel_tx.
pub trait Worker: Send {
    /// Reporter will invoke this method to Send the cql response to worker
    fn handle_response(self: Box<Self>, giveload: Vec<u8>);
    /// Reporter will invoke this method to Send the worker error to worker
    fn handle_error(self: Box<Self>, error: WorkerError, reporter: &Option<ReporterHandle>);
}

#[derive(Debug)]
/// The CQL worker error.
pub enum WorkerError {
    /// The CQL Error reported from ScyllaDB.
    Cql(CqlError),
    /// The IO Error.
    Io(Error),
    /// The overload when we do not have any more streams.
    Overload,
    /// We lost the worker due to the abortion of ScyllaDB connection.
    Lost,
    /// There is no ring initialized.
    NoRing,
}

impl std::fmt::Display for WorkerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WorkerError::Cql(cql_error) => write!(f, "Worker CqlError: {:?}", cql_error),
            WorkerError::Io(io_error) => write!(f, "Worker IoError: {:?}", io_error),
            WorkerError::Overload => write!(f, "Worker Overload"),
            WorkerError::Lost => write!(f, "Worker Lost"),
            WorkerError::NoRing => write!(f, "Worker NoRing"),
        }
    }
}

impl std::error::Error for WorkerError {}

/// should be implemented on the handle of the worker
pub trait HandleResponse<W: Worker + DecodeResponse<Self::Response>>: Send {
    /// Defines the response type
    type Response;
    /// Handle response for worker of type W
    fn handle_response(worker: Box<W>, response: Self::Response);
}
/// should be implemented on the handle of the worker
pub trait HandleError<W: Worker>: Send {
    /// Handle error for worker of type W
    fn handle_error(worker: Box<W>, worker_error: WorkerError);
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

pub mod insert;
pub mod prepare;
pub mod select;
pub mod value;

pub use prepare::PrepareWorker;
pub use select::SelectWorker;
