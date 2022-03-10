// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::prelude::BatchRequest;

/// A select worker
#[derive(Clone, Debug)]
pub struct BatchWorker {
    /// The worker's request
    pub request: BatchRequest,
    /// The number of times this worker will retry on failure
    pub retries: usize,
}

impl BatchWorker {
    pub fn new(request: BatchRequest) -> Self {
        Self { request, retries: 3 }
    }
}

impl Worker for BatchWorker {
    fn handle_response(self: Box<Self>, _body: ResponseBody) -> anyhow::Result<()> {
        Ok(())
    }

    fn handle_error(self: Box<Self>, error: WorkerError, reporter: &ReporterHandle) -> anyhow::Result<()> {
        error!("{}", error);
        if let WorkerError::Cql(ref cql_error) = error {
            match cql_error.code() {
                ErrorCode::Unprepared => {
                    anyhow::bail!("Unprepared query!")
                }
                _ => self.retry(reporter).map_err(|_| anyhow::anyhow!("Cannot retry!")),
            }
        } else {
            self.retry(reporter).map_err(|_| anyhow::anyhow!("Cannot retry!"))
        }
    }
}

impl RetryableWorker<BatchRequest> for BatchWorker {
    fn retries(&self) -> usize {
        self.retries
    }

    fn request(&self) -> &BatchRequest {
        &self.request
    }

    fn retries_mut(&mut self) -> &mut usize {
        &mut self.retries
    }
}

impl<H> IntoRespondingWorker<BatchRequest, H> for BatchWorker
where
    H: 'static + HandleResponse + HandleError + Debug + Send + Sync,
{
    type Output = RespondingBatchWorker<H>;
    fn with_handle(self, handle: H) -> RespondingBatchWorker<H> {
        RespondingBatchWorker::<H>::from(self, handle)
    }
}

/// A select worker
#[derive(Debug)]
pub struct RespondingBatchWorker<H> {
    /// The worker's request
    pub request: BatchRequest,
    /// A handle which can be used to return the queried value
    pub handle: H,
    /// The number of times this worker will retry on failure
    pub retries: usize,
}

impl<H> RespondingBatchWorker<H>
where
    H: 'static,
{
    /// Create a new value selecting worker with a number of retries and a response handle
    pub fn new(request: BatchRequest, handle: H) -> Self {
        Self {
            request,
            handle,
            retries: 3,
        }
    }

    pub(crate) fn from(BatchWorker { request, retries }: BatchWorker, handle: H) -> Self
    where
        H: HandleResponse + HandleError + Debug + Send + Sync,
    {
        Self::new(request, handle).with_retries(retries)
    }
}

impl<H> Worker for RespondingBatchWorker<H>
where
    H: 'static + HandleResponse + HandleError + Debug + Send + Sync,
{
    fn handle_response(self: Box<Self>, body: ResponseBody) -> anyhow::Result<()> {
        self.handle.handle_response(body)
    }

    fn handle_error(self: Box<Self>, error: WorkerError, reporter: &ReporterHandle) -> anyhow::Result<()> {
        error!("{}", error);
        if let WorkerError::Cql(ref cql_error) = error {
            match cql_error.code() {
                ErrorCode::Unprepared => {
                    let id = cql_error.unprepared_id().unwrap();
                    if let Some(statement) = self.request().get_statement(&id).cloned() {
                        info!("Attempting to prepare statement '{}'", statement);
                        PrepareWorker::<PreparedQuery>::new(PrepareRequest::new(PrepareFrame::new(statement), 0, None))
                            .send_to_reporter(reporter)?;
                        match self.retry(reporter) {
                            Ok(_) => Ok(()),
                            Err(worker) => worker.handle.handle_error(error),
                        }
                    } else {
                        anyhow::bail!("Cannot find statement for unprepared id {:?}", id)
                    }
                }
                _ => match self.retry(reporter) {
                    Ok(_) => Ok(()),
                    Err(worker) => worker.handle.handle_error(error),
                },
            }
        } else {
            match self.retry(reporter) {
                Ok(_) => Ok(()),
                Err(worker) => worker.handle.handle_error(error),
            }
        }
    }
}

impl<H> RetryableWorker<BatchRequest> for RespondingBatchWorker<H>
where
    H: 'static + HandleResponse + HandleError + Debug + Send + Sync,
{
    fn retries(&self) -> usize {
        self.retries
    }

    fn request(&self) -> &BatchRequest {
        &self.request
    }

    fn retries_mut(&mut self) -> &mut usize {
        &mut self.retries
    }
}

impl<H> RespondingWorker<BatchRequest, H> for RespondingBatchWorker<H>
where
    H: 'static + HandleResponse + HandleError + Debug + Send + Sync,
{
    fn handle(&self) -> &H {
        &self.handle
    }
}
