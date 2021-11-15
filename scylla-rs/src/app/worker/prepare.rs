// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;
use std::fmt::Debug;

/// A statement prepare worker
#[derive(Debug)]
pub struct PrepareWorker {
    /// The expected id for this statement
    pub(crate) id: [u8; 16],
    pub(crate) retries: usize,
    pub(crate) request: PrepareRequest,
}
impl PrepareWorker {
    /// Create a new prepare worker
    pub fn new(keyspace_name: Option<String>, id: [u8; 16], statement: &str) -> Box<Self> {
        Box::new(Self {
            id,
            retries: 0,
            request: PrepareRequest {
                keyspace_name,
                statement: statement.to_string().into(),
                token: rand::random(),
            },
        })
    }
}
impl From<PrepareRequest> for PrepareWorker {
    fn from(request: PrepareRequest) -> Self {
        Self {
            id: md5::compute(request.statement().as_bytes()).into(),
            retries: 0,
            request,
        }
    }
}
impl Worker for PrepareWorker {
    fn handle_response(self: Box<Self>, _giveload: Vec<u8>) -> anyhow::Result<()> {
        info!("Successfully prepared statement: '{}'", self.request.statement());
        Ok(())
    }
    fn handle_error(self: Box<Self>, error: WorkerError, _reporter: &ReporterHandle) -> anyhow::Result<()> {
        error!(
            "Failed to prepare statement: {}, error: {}",
            self.request.statement(),
            error
        );
        self.retry().ok();
        Ok(())
    }
}

impl RetryableWorker<PrepareRequest> for PrepareWorker {
    fn retries(&self) -> usize {
        self.retries
    }

    fn retries_mut(&mut self) -> &mut usize {
        &mut self.retries
    }

    fn request(&self) -> &PrepareRequest {
        &self.request
    }
}

impl<H> IntoRespondingWorker<PrepareRequest, H, Decoder> for PrepareWorker
where
    H: 'static + HandleResponse<Decoder> + HandleError + Debug + Send + Sync,
{
    type Output = RespondingPrepareWorker<H>;

    fn with_handle(self: Box<Self>, handle: H) -> Box<Self::Output> {
        Box::new(RespondingPrepareWorker {
            id: self.id,
            retries: self.retries,
            request: self.request,
            handle,
        })
    }
}

/// A statement prepare worker
#[derive(Debug)]
pub struct RespondingPrepareWorker<H> {
    /// The expected id for this statement
    pub(crate) id: [u8; 16],
    pub(crate) request: PrepareRequest,
    pub(crate) retries: usize,
    pub(crate) handle: H,
}

impl<H> Worker for RespondingPrepareWorker<H>
where
    H: 'static + HandleResponse<Decoder> + HandleError + Debug + Send + Sync,
{
    fn handle_response(self: Box<Self>, giveload: Vec<u8>) -> anyhow::Result<()> {
        match Decoder::try_from(giveload) {
            Ok(decoder) => self.handle.handle_response(decoder),
            Err(e) => self.handle.handle_error(WorkerError::Other(e)),
        }
    }
    fn handle_error(self: Box<Self>, error: WorkerError, _reporter: &ReporterHandle) -> anyhow::Result<()> {
        error!("{}", error);
        match self.retry() {
            Ok(_) => Ok(()),
            Err(worker) => worker.handle.handle_error(error),
        }
    }
}

impl<H> RetryableWorker<PrepareRequest> for RespondingPrepareWorker<H>
where
    H: 'static + HandleResponse<Decoder> + HandleError + Debug + Send + Sync,
{
    fn retries(&self) -> usize {
        self.retries
    }

    fn retries_mut(&mut self) -> &mut usize {
        &mut self.retries
    }

    fn request(&self) -> &PrepareRequest {
        &self.request
    }
}

impl<H> RespondingWorker<PrepareRequest, H, Decoder> for RespondingPrepareWorker<H>
where
    H: 'static + HandleResponse<Decoder> + HandleError + Debug + Send + Sync,
{
    fn handle(&self) -> &H {
        &self.handle
    }
}
