// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0
use super::*;
use std::{
    fmt::Debug,
    marker::PhantomData,
};

#[derive(Debug, Clone)]
pub struct BasicWorker;

impl BasicWorker {
    pub fn new() -> Box<Self> {
        Box::new(Self)
    }
}

impl Worker for BasicWorker {
    fn handle_response(self: Box<Self>, giveload: Vec<u8>) -> anyhow::Result<()> {
        Decoder::try_from(giveload)?;
        Ok(())
    }

    fn handle_error(self: Box<Self>, error: WorkerError, reporter: &ReporterHandle) -> anyhow::Result<()> {
        anyhow::bail!(error);
    }
}

/// An insert request worker
pub struct BasicRetryWorker<R, O> {
    pub request: R,
    /// The number of times this worker will retry on failure
    pub retries: usize,
    _output: PhantomData<fn(O) -> O>,
}

impl<R, O> Debug for BasicRetryWorker<R, O>
where
    R: Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BasicWorker")
            .field("request", &self.request)
            .field("retries", &self.retries)
            .finish()
    }
}

impl<R, O> Clone for BasicRetryWorker<R, O>
where
    R: Clone,
{
    fn clone(&self) -> Self {
        Self {
            request: self.request.clone(),
            retries: self.retries,
            _output: PhantomData,
        }
    }
}

impl<R, O> BasicRetryWorker<R, O> {
    /// Create a new insert worker with a number of retries
    pub fn new(request: R, retries: usize) -> Box<Self> {
        Box::new(Self {
            request,
            retries,
            _output: PhantomData,
        })
    }
}

impl<R, O> Worker for BasicRetryWorker<R, O>
where
    R: 'static + Debug + Send + GetRequestExt<O> + Clone,
    <R as SendRequestExt>::Marker: 'static,
    O: 'static + Send,
{
    fn handle_response(self: Box<Self>, giveload: Vec<u8>) -> anyhow::Result<()> {
        Decoder::try_from(giveload)?;
        Ok(())
    }

    fn handle_error(mut self: Box<Self>, mut error: WorkerError, reporter: &ReporterHandle) -> anyhow::Result<()> {
        if let WorkerError::Cql(ref mut cql_error) = error {
            if let Some(id) = cql_error.take_unprepared_id() {
                let request = self.request.clone();
                handle_unprepared_error(self, request, id, reporter)
                    .map_err(|e| anyhow::anyhow!("Error trying to prepare query: {}", e))
            } else {
                self.retry().map_err(|_| anyhow::anyhow!("Cannot retry!"))
            }
        } else {
            self.retry().map_err(|_| anyhow::anyhow!("Cannot retry!"))
        }
    }
}

impl<R, O> RetryableWorker<R, O> for BasicRetryWorker<R, O>
where
    R: 'static + GetRequestExt<O> + Clone + Send + Debug,
    <R as SendRequestExt>::Marker: 'static,
    O: 'static + Send,
{
    fn retries(&mut self) -> &mut usize {
        &mut self.retries
    }

    fn request(&self) -> Option<&R> {
        Some(&self.request)
    }
}
