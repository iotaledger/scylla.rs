// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

/// A select worker
#[derive(Clone, Debug)]
pub struct RespondingQueryWorker<H, R> {
    /// The worker's request
    pub request: R,
    /// A handle which can be used to return the queried value
    pub handle: H,
    /// The query page size, used when retying due to failure
    pub page_size: Option<i32>,
    /// The query paging state, used when retrying due to failure
    pub paging_state: Option<Vec<u8>>,
    /// The number of times this worker will retry on failure
    pub retries: usize,
}

impl<H, R> RespondingQueryWorker<H, R>
where
    H: 'static,
{
    /// Create a new value selecting worker with a number of retries and a response handle
    pub fn new(request: R, handle: H) -> Self {
        Self {
            request,
            handle,
            page_size: None,
            paging_state: None,
            retries: 3,
        }
    }

    pub(crate) fn from(QueryRetryWorker { request, retries }: QueryRetryWorker<R>, handle: H) -> Self
    where
        H: HandleResponse + HandleError + Debug + Send + Sync,
        R: 'static + SendRequestExt + Debug + Send + Sync + Clone,
    {
        Self::new(request, handle).with_retries(retries)
    }

    /// Add paging information to this worker
    pub fn with_paging<P: Into<Option<Vec<u8>>>>(mut self, page_size: i32, paging_state: P) -> Self {
        self.page_size = Some(page_size);
        self.paging_state = paging_state.into();
        self
    }
}

impl<H, R> Worker for RespondingQueryWorker<H, R>
where
    H: 'static + HandleResponse + HandleError + Debug + Send + Sync,
    R: 'static + Send + Debug + SendRequestExt + Sync + Clone,
{
    fn handle_response(self: Box<Self>, body: ResponseBody) -> anyhow::Result<()> {
        self.handle.handle_response(body)
    }

    fn handle_error(self: Box<Self>, error: WorkerError, reporter: &ReporterHandle) -> anyhow::Result<()> {
        error!("{}", error);
        if let WorkerError::Cql(ref cql_error) = error {
            match cql_error.code() {
                ErrorCode::Unprepared => {
                    anyhow::bail!("Unprepared query!");
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

impl<H, R> RetryableWorker<R> for RespondingQueryWorker<H, R>
where
    H: 'static + HandleResponse + HandleError + Debug + Send + Sync,
    R: 'static + Send + Debug + SendRequestExt + Clone + Sync,
{
    fn retries(&self) -> usize {
        self.retries
    }

    fn request(&self) -> &R {
        &self.request
    }

    fn retries_mut(&mut self) -> &mut usize {
        &mut self.retries
    }
}

impl<R, H> RespondingWorker<R, H> for RespondingQueryWorker<H, R>
where
    H: 'static + HandleResponse + HandleError + Debug + Send + Sync,
    R: 'static + Send + Debug + SendRequestExt + Clone + Sync,
{
    fn handle(&self) -> &H {
        &self.handle
    }
}

/// A select worker
#[derive(Clone, Debug)]
pub struct RespondingExecuteWorker<H, R> {
    /// The worker's request
    pub request: R,
    /// A handle which can be used to return the queried value
    pub handle: H,
    /// The query page size, used when retying due to failure
    pub page_size: Option<i32>,
    /// The query paging state, used when retrying due to failure
    pub paging_state: Option<Vec<u8>>,
    /// The number of times this worker will retry on failure
    pub retries: usize,
}

impl<H, R> RespondingExecuteWorker<H, R>
where
    H: 'static,
{
    /// Create a new value selecting worker with a number of retries and a response handle
    pub fn new(request: R, handle: H) -> Self {
        Self {
            request,
            handle,
            page_size: None,
            paging_state: None,
            retries: 3,
        }
    }

    pub(crate) fn from(ExecuteRetryWorker { request, retries }: ExecuteRetryWorker<R>, handle: H) -> Self
    where
        H: HandleResponse + HandleError + Debug + Send + Sync,
        R: 'static
            + SendRequestExt
            + RequestFrameExt<Frame = ExecuteFrame>
            + ReprepareExt
            + Debug
            + Send
            + Sync
            + Clone,
    {
        Self::new(request, handle).with_retries(retries)
    }

    /// Add paging information to this worker
    pub fn with_paging<P: Into<Option<Vec<u8>>>>(mut self, page_size: i32, paging_state: P) -> Self {
        self.page_size = Some(page_size);
        self.paging_state = paging_state.into();
        self
    }

    /// Convert this to a basic worker
    pub fn convert(self) -> RespondingQueryWorker<H, R::OutRequest>
    where
        R: ReprepareExt,
    {
        RespondingQueryWorker {
            request: self.request.convert(),
            retries: self.retries,
            handle: self.handle,
            page_size: self.page_size,
            paging_state: self.paging_state,
        }
    }
}

impl<H, R> Worker for RespondingExecuteWorker<H, R>
where
    H: 'static + HandleResponse + HandleError + Debug + Send + Sync,
    R: 'static + Send + Debug + SendRequestExt + RequestFrameExt<Frame = ExecuteFrame> + ReprepareExt + Sync + Clone,
{
    fn handle_response(self: Box<Self>, body: ResponseBody) -> anyhow::Result<()> {
        self.handle.handle_response(body)
    }

    fn handle_error(self: Box<Self>, error: WorkerError, reporter: &ReporterHandle) -> anyhow::Result<()> {
        error!("{}", error);
        if let WorkerError::Cql(ref cql_error) = error {
            match cql_error.code() {
                ErrorCode::Unprepared => {
                    let statement = self.request().statement().clone();
                    let worker = self.convert();
                    let mut worker_opt = Some(worker);
                    handle_unprepared_error(statement, &mut worker_opt, reporter)?;
                    if let Some(worker) = worker_opt {
                        worker.retry(reporter).map_err(|_| anyhow::anyhow!("Cannot retry!"))
                    } else {
                        Ok(())
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

impl<H, R> RetryableWorker<R> for RespondingExecuteWorker<H, R>
where
    H: 'static + HandleResponse + HandleError + Debug + Send + Sync,
    R: 'static + Send + Debug + SendRequestExt + RequestFrameExt<Frame = ExecuteFrame> + ReprepareExt + Sync + Clone,
{
    fn retries(&self) -> usize {
        self.retries
    }

    fn request(&self) -> &R {
        &self.request
    }

    fn retries_mut(&mut self) -> &mut usize {
        &mut self.retries
    }
}

impl<R, H> RespondingWorker<R, H> for RespondingExecuteWorker<H, R>
where
    H: 'static + HandleResponse + HandleError + Debug + Send + Sync,
    R: 'static + Send + Debug + SendRequestExt + RequestFrameExt<Frame = ExecuteFrame> + ReprepareExt + Sync + Clone,
{
    fn handle(&self) -> &H {
        &self.handle
    }
}
