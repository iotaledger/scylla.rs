// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

/// A statement prepare worker
#[derive(Debug)]
pub struct PrepareWorker<P> {
    pub(crate) retries: usize,
    pub(crate) request: PrepareRequest<P>,
}
impl<P> PrepareWorker<P> {
    /// Create a new prepare worker
    pub fn new(request: PrepareRequest<P>) -> Self {
        Self { retries: 0, request }
    }
}

impl<P> From<PrepareRequest<P>> for PrepareWorker<P> {
    fn from(request: PrepareRequest<P>) -> Self {
        Self { retries: 0, request }
    }
}
impl<P> Worker for PrepareWorker<P>
where
    P: 'static + From<PreparedQuery> + Debug + Send + Sync,
{
    fn handle_response(self: Box<Self>, _body: ResponseBody) -> anyhow::Result<()> {
        info!(
            "Successfully prepared statement: '{}'",
            self.request().frame().statement()
        );
        Ok(())
    }
    fn handle_error(self: Box<Self>, error: WorkerError, reporter: &ReporterHandle) -> anyhow::Result<()> {
        error!(
            "Failed to prepare statement: {}, error: {}",
            self.request().frame().statement(),
            error
        );
        self.retry(reporter).ok();
        Ok(())
    }
}

impl<P> RetryableWorker<PrepareRequest<P>> for PrepareWorker<P>
where
    P: 'static + From<PreparedQuery> + Debug + Send + Sync,
{
    fn retries(&self) -> usize {
        self.retries
    }

    fn retries_mut(&mut self) -> &mut usize {
        &mut self.retries
    }

    fn request(&self) -> &PrepareRequest<P> {
        &self.request
    }
}

impl<H, P> IntoRespondingWorker<PrepareRequest<P>, H> for PrepareWorker<P>
where
    H: 'static + HandleResponse + HandleError + Debug + Send + Sync,
    P: 'static + From<PreparedQuery> + Debug + Send + Sync,
{
    type Output = RespondingPrepareWorker<H, P>;

    fn with_handle(self, handle: H) -> Self::Output {
        RespondingPrepareWorker {
            retries: self.retries,
            request: self.request,
            handle,
        }
    }
}

/// A statement prepare worker
#[derive(Debug)]
pub struct RespondingPrepareWorker<H, P> {
    pub(crate) request: PrepareRequest<P>,
    pub(crate) retries: usize,
    pub(crate) handle: H,
}

impl<H, P> Worker for RespondingPrepareWorker<H, P>
where
    H: 'static + HandleResponse + HandleError + Debug + Send + Sync,
    P: 'static + From<PreparedQuery> + Debug + Send + Sync,
{
    fn handle_response(self: Box<Self>, body: ResponseBody) -> anyhow::Result<()> {
        self.handle.handle_response(body)
    }
    fn handle_error(self: Box<Self>, error: WorkerError, reporter: &ReporterHandle) -> anyhow::Result<()> {
        error!("{}", error);
        match self.retry(reporter) {
            Ok(_) => Ok(()),
            Err(worker) => worker.handle.handle_error(error),
        }
    }
}

impl<H, P> RetryableWorker<PrepareRequest<P>> for RespondingPrepareWorker<H, P>
where
    H: 'static + HandleResponse + HandleError + Debug + Send + Sync,
    P: 'static + From<PreparedQuery> + Debug + Send + Sync,
{
    fn retries(&self) -> usize {
        self.retries
    }

    fn retries_mut(&mut self) -> &mut usize {
        &mut self.retries
    }

    fn request(&self) -> &PrepareRequest<P> {
        &self.request
    }
}

impl<H, P> RespondingWorker<PrepareRequest<P>, H> for RespondingPrepareWorker<H, P>
where
    H: 'static + HandleResponse + HandleError + Debug + Send + Sync,
    P: 'static + From<PreparedQuery> + Debug + Send + Sync,
{
    fn handle(&self) -> &H {
        &self.handle
    }
}
