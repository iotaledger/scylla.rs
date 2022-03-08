// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::{
    sender::SenderEvent,
    Payloads,
};
use crate::{
    app::worker::{
        Worker,
        WorkerError,
    },
    cql::compression::Compression,
    prelude::{
        ResponseBody,
        ResponseFrame,
    },
};
use async_trait::async_trait;
use backstage::core::{
    AbortableUnboundedHandle,
    Actor,
    ActorError,
    ActorResult,
    Rt,
    ShutdownEvent,
    StreamExt,
    SupHandle,
    UnboundedChannel,
    UnboundedHandle,
};
use std::{
    collections::HashMap,
    marker::PhantomData,
};

/// Workers Map holds all the workers_ids
type Workers = HashMap<u16, Box<dyn Worker>>;
/// Reporter's handle, used to push cql request
pub type ReporterHandle = UnboundedHandle<ReporterEvent>;

/// Reporter event enum.
#[derive(Debug)]
pub enum ReporterEvent {
    /// The request Cql query.
    Request {
        /// The worker which is used to process the request.
        worker: Box<dyn Worker>,
        /// The request payload.
        payload: Vec<u8>,
    },
    /// The response Cql query.
    Response {
        /// The reponse stream ID.
        stream_id: u16,
    },
    /// The stream error.
    Err(anyhow::Error, u16),
    /// Shutdown signal
    Shutdown,
}
impl ShutdownEvent for ReporterEvent {
    fn shutdown_event() -> Self {
        Self::Shutdown
    }
}
/// Reporter state
pub struct Reporter<C: Compression> {
    streams: Vec<u16>,
    workers: Workers,
    _compression: PhantomData<fn(C) -> C>,
}

/// The Reporter actor lifecycle implementation
#[async_trait]
impl<S, C: 'static + Compression> Actor<S> for Reporter<C>
where
    S: SupHandle<Self>,
{
    type Data = (Payloads, AbortableUnboundedHandle<SenderEvent>);
    type Channel = UnboundedChannel<ReporterEvent>;
    async fn init(&mut self, rt: &mut Rt<Self, S>) -> ActorResult<Self::Data> {
        let parent_id = rt
            .parent_id()
            .ok_or_else(|| ActorError::exit_msg("reporter without stage supervisor"))?;
        let sender_scope_id = rt
            .sibling("sender")
            .scope_id()
            .await
            .ok_or_else(|| ActorError::exit_msg("reporter without sender sibling"))?;
        let payloads = rt
            .lookup(parent_id)
            .await
            .ok_or_else(|| ActorError::exit_msg("reporter unables to lookup for payloads"))?;
        let sender_handle = rt.link(sender_scope_id, false).await.map_err(ActorError::exit)?;
        Ok((payloads, sender_handle))
    }
    async fn run(&mut self, rt: &mut Rt<Self, S>, (mut payloads, sender): Self::Data) -> ActorResult<()> {
        while let Some(event) = rt.inbox_mut().next().await {
            match event {
                ReporterEvent::Request { worker, payload } => {
                    // log::info!("reporter received request, payload len: {}", payload.len());
                    match C::compress(payload) {
                        Ok(mut payload) => {
                            // log::info!("Compressed payload len: {}", payload.len());
                            if let Some(stream) = self.streams.pop() {
                                // log::info!("Assigning payload to stream {}", stream);
                                // Assign stream_id to the payload
                                assign_stream_to_payload(stream, &mut payload);
                                // log::info!("Request header: {:?}", &payload[0..9]);
                                // store payload as reusable at payloads[stream]
                                payloads[stream as usize].as_mut().replace(payload);
                                self.workers.insert(stream, worker);
                                sender.send(stream).unwrap_or_else(|e| log::error!("{}", e));
                            } else {
                                // Send overload to the worker in-case we don't have anymore streams
                                worker
                                    .handle_error(WorkerError::Overload, Some(rt.handle()))
                                    .unwrap_or_else(|e| log::error!("{}", e));
                            }
                        }
                        Err(e) => {
                            worker
                                .handle_error(WorkerError::Other(anyhow::anyhow!(e)), Some(rt.handle()))
                                .unwrap_or_else(|e| log::error!("{}", e));
                        }
                    }
                }
                ReporterEvent::Response { stream_id } => {
                    self.handle_response(rt.handle(), stream_id, &mut payloads)
                        .unwrap_or_else(|e| log::error!("{}", e));
                }
                ReporterEvent::Err(io_error, stream_id) => {
                    self.handle_error(rt.handle(), stream_id, &mut payloads, WorkerError::Other(io_error))
                        .unwrap_or_else(|e| log::error!("{}", e));
                }
                ReporterEvent::Shutdown => {
                    rt.stop().await;
                    if rt.microservices_stopped() {
                        rt.inbox_mut().close();
                    }
                }
            }
        }
        self.force_consistency(rt.handle());
        Ok(())
    }
}
impl<C: 'static + Compression> Reporter<C> {
    /// Create new reporter
    pub(super) fn new(streams: Vec<u16>) -> Self {
        Self {
            streams,
            workers: HashMap::new(),
            _compression: PhantomData,
        }
    }

    fn handle_response(&mut self, handle: &ReporterHandle, stream: u16, payloads: &mut Payloads) -> anyhow::Result<()> {
        // push the stream_id back to streams vector.
        self.streams.push(stream);
        // remove the worker from workers.
        if let Some(worker) = self.workers.remove(&stream) {
            if let Some(payload) = payloads[stream as usize].as_mut().take() {
                match ResponseFrame::decode::<C>(payload).map_err(WorkerError::FrameError) {
                    Ok(frame) => match frame.into_body() {
                        ResponseBody::Error(err) => {
                            worker.handle_error(WorkerError::Cql(err), Some(handle))?;
                        }
                        body => {
                            worker.handle_response(body)?;
                        }
                    },
                    Err(e) => {
                        worker.handle_error(e, Some(handle))?;
                    }
                }
            } else {
                log::error!("No payload found while handling response for stream {}!", stream);
            }
        } else {
            log::error!("No worker found while handling response for stream {}!", stream);
        }
        Ok(())
    }
    fn handle_error(
        &mut self,
        handle: &ReporterHandle,
        stream: u16,
        payloads: &mut Payloads,
        error: WorkerError,
    ) -> anyhow::Result<()> {
        // push the stream_id back to streams vector.
        self.streams.push(stream);
        // remove the worker from workers and send error.
        if let Some(worker) = self.workers.remove(&stream) {
            // drop payload.
            if let Some(_payload) = payloads[stream as usize].as_mut().take() {
                worker.handle_error(error, Some(handle))?;
            } else {
                log::error!("No payload found while handling error for stream {}!", stream);
            }
        } else {
            log::error!("No worker found while handling error for stream {}!", stream);
        }
        Ok(())
    }
    fn force_consistency(&mut self, handle: &ReporterHandle) {
        for (stream_id, worker_id) in self.workers.drain() {
            // push the stream_id back into the streams vector
            self.streams.push(stream_id);
            // tell worker_id that we lost the response for his request, because we lost scylla connection in
            // middle of request cycle, still this is a rare case.
            worker_id
                .handle_error(WorkerError::Lost, Some(handle))
                .unwrap_or_else(|e| log::error!("{}", e));
        }
    }
}

// private functions
fn assign_stream_to_payload(stream: u16, payload: &mut Vec<u8>) {
    payload[2] = (stream >> 8) as u8; // payload[2] is where the first byte of the stream_id should be,
    payload[3] = stream as u8; // payload[3] is the second byte of the stream_id. please refer to cql specs
}
