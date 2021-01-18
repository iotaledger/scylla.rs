// Copyright 2020 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use crate::stage::ReporterHandle;

pub use crate::stage::ReporterEvent;
use std::io::Error;

/// WorkerId trait type which will be implemented by worker in order to send their channel_tx.
pub trait Worker: Send + std::fmt::Debug {
    /// Send the response.
    fn send_response(self: Box<Self>, tx: &Option<ReporterHandle>, giveload: Vec<u8>);
    /// Send the error.
    fn send_error(self: Box<Self>, error: WorkerError);
}

#[derive(Debug)]
/// The CQL worker error.
pub enum WorkerError {
    // The CQL Error reported from ScyllaDB.
    // Cql(CqlError),
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
            // UNCOMMENT WorkerError::Cql(cql_error) => write!(f, "Worker CqlError: {:?}", cql_error),
            WorkerError::Io(io_error) => write!(f, "Worker IoError: {:?}", io_error),
            WorkerError::Overload => write!(f, "Worker Overload"),
            WorkerError::Lost => write!(f, "Worker Lost"),
            WorkerError::NoRing => write!(f, "Worker NoRing"),
        }
    }
}

impl std::error::Error for WorkerError {}
