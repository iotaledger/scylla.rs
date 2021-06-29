// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::{
    app::worker::{Worker, WorkerError},
    cql::{CqlError, Decoder},
};
use anyhow::anyhow;
use std::{collections::HashSet, convert::TryFrom};

/// Workers Map holds all the workers_ids
type Workers = HashMap<i16, Box<dyn Worker>>;

/// Reporter state
pub struct Reporter {
    address: SocketAddr,
    session_id: usize,
    reporter_id: u8,
    streams: HashSet<i16>,
    shard_id: u16,
    workers: Workers,
    payloads: Payloads,
}

#[build]
#[derive(Clone)]
pub fn build_reporter(
    session_id: usize,
    reporter_id: u8,
    shard_id: u16,
    streams: HashSet<i16>,
    address: SocketAddr,
    payloads: Payloads,
) -> Reporter {
    Reporter {
        address,
        session_id,
        reporter_id,
        streams,
        shard_id,
        workers: HashMap::new(),
        payloads,
    }
}

#[async_trait]
impl Actor for Reporter {
    type Dependencies = ();
    type Event = ReporterEvent;
    type Channel = TokioChannel<Self::Event>;

    async fn run<'a, Reg: RegistryAccess + Send + Sync>(
        &mut self,
        rt: &mut ActorScopedRuntime<'a, Self, Reg>,
        _deps: Self::Dependencies,
    ) -> Result<(), ActorError>
    where
        Self: Sized,
    {
        rt.update_status(ServiceStatus::Running).await;
        let mut sender_handle: Option<Act<super::sender::Sender>> = None;
        let mut my_handle = rt.my_handle().await;
        while let Some(event) = rt.next_event().await {
            match event {
                ReporterEvent::Request { worker, mut payload } => {
                    if let Some(stream) = self.streams.iter().next().cloned() {
                        // Send the event
                        match sender_handle.as_mut() {
                            Some(sender) => {
                                self.streams.remove(&stream);
                                // Assign stream_id to the payload
                                assign_stream_to_payload(stream, &mut payload);
                                // store payload as reusable at payloads[stream]
                                self.payloads[stream as usize].as_mut().replace(payload);
                                self.workers.insert(stream, worker);
                                sender.send(stream).await.unwrap_or_else(|e| error!("{}", e));
                            }
                            None => {
                                // This means the sender_tx had been droped as a result of checkpoint from
                                // receiver
                                worker
                                    .handle_error(WorkerError::Other(anyhow!("No Sender!")), Some(&mut my_handle))
                                    .await
                                    .unwrap_or_else(|e| error!("{}", e));
                            }
                        }
                    } else {
                        // Send overload to the worker in-case we don't have anymore streams
                        worker
                            .handle_error(WorkerError::Overload, Some(&mut my_handle))
                            .await
                            .unwrap_or_else(|e| error!("{}", e));
                    }
                }
                ReporterEvent::Response { stream_id } => {
                    self.handle_response(stream_id, &mut my_handle)
                        .await
                        .unwrap_or_else(|e| error!("{}", e));
                }
                ReporterEvent::Err(io_error, stream_id) => {
                    self.handle_error(stream_id, WorkerError::Other(io_error), &mut my_handle)
                        .await
                        .unwrap_or_else(|e| error!("{}", e));
                }
                ReporterEvent::Session(session) => {
                    match session {
                        Session::New => {
                            self.session_id += 1;
                            sender_handle = rt.actor_event_handle::<super::sender::Sender>().await;
                            info!(
                                "address: {}, shard_id: {}, reporter_id: {}, received session: {:?}",
                                &self.address, self.shard_id, self.reporter_id, self.session_id
                            );
                            if !rt.service().await.is_stopping() {
                                // degraded service
                                rt.update_status(ServiceStatus::Degraded).await;
                            }
                        }
                        Session::Service => {
                            //if rt.service().await.is_stopped() {
                            //    // drop the sender_handle
                            //    sender_handle = None;
                            //} else {
                            //    // do not update the status if service is_stopping
                            //    if !self.service.is_stopping() {
                            //        // degraded service
                            //        rt.update_status(ServiceStatus::Degraded).await;
                            //    }
                            //}
                            //let microservices_len = self.service.microservices.len();
                            //// check if all microservices are stopped
                            //if self.service.microservices.values().all(|ms| ms.is_stopped()) && microservices_len == 2 {
                            //    // first we drain workers map from stucked requests, to force_consistency of
                            //    // the old_session requests
                            //    self.force_consistency();
                            //    warn!(
                            //        "address: {}, shard_id: {}, reporter_id: {}, closing session: {:?}",
                            //        &self.address, self.shard_id, self.reporter_id, self.session_id
                            //    );
                            //    if !self.service.is_stopping() {
                            //        // Maintenance service mode
                            //        rt.update_status(ServiceStatus::Maintenance).await;
                            //    }
                            //} else if self.service.microservices.values().all(|ms| ms.is_running())
                            //    && microservices_len == 2
                            //{
                            //    if !self.service.is_stopping() {
                            //        // running service
                            //        rt.update_status(ServiceStatus::Running).await;
                            //    }
                            //};
                        }
                        Session::Shutdown => {
                            // drop the sender_handle to gracefully shut it down
                            sender_handle = None;
                        }
                    }
                }
            }
        }
        rt.update_status(ServiceStatus::Stopping).await;
        self.force_consistency(&mut my_handle).await;
        warn!(
            "reporter_id: {} of shard_id: {} in node: {}, gracefully shutting down.",
            self.reporter_id, self.shard_id, &self.address
        );
        rt.update_status(ServiceStatus::Stopped).await;
        Ok(())
    }
}

/// Reporter event enum.
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
        stream_id: i16,
    },
    /// The stream error.
    Err(anyhow::Error, i16),
    /// The stage session.
    Session(Session),
}

impl Clone for ReporterEvent {
    fn clone(&self) -> Self {
        match self {
            ReporterEvent::Request { worker: _, payload: _ } => panic!("Cannot clone reporter request!"),
            ReporterEvent::Response { stream_id } => Self::Response { stream_id: *stream_id },
            ReporterEvent::Err(_, _) => panic!("Cannot clone reporter error!"),
            ReporterEvent::Session(s) => Self::Session(*s),
        }
    }
}

#[derive(Clone, Copy)]
pub enum Session {
    New,
    Service,
    Shutdown,
}

impl Reporter {
    async fn handle_response(&mut self, stream: i16, handle: &mut Act<Reporter>) -> anyhow::Result<()> {
        // push the stream_id back to streams vector.
        self.streams.insert(stream);
        // remove the worker from workers.
        if let Some(worker) = self.workers.remove(&stream) {
            if let Some(payload) = self.payloads[stream as usize].as_mut().take() {
                if is_cql_error(&payload) {
                    let error = Decoder::try_from(payload)
                        .and_then(|decoder| CqlError::new(&decoder).map(|e| WorkerError::Cql(e)))
                        .unwrap_or_else(|e| WorkerError::Other(e));
                    worker.handle_error(error, Some(handle)).await?;
                } else {
                    worker.handle_response(payload).await?;
                }
            } else {
                error!("No payload found while handling response for stream {}!", stream);
            }
        } else {
            error!("No worker found while handling response for stream {}!", stream);
        }
        Ok(())
    }

    async fn handle_error(
        &mut self,
        stream: i16,
        error: WorkerError,
        handle: &mut Act<Reporter>,
    ) -> anyhow::Result<()> {
        // push the stream_id back to streams vector.
        self.streams.insert(stream);
        // remove the worker from workers and send error.
        if let Some(worker) = self.workers.remove(&stream) {
            // drop payload.
            if let Some(_payload) = self.payloads[stream as usize].as_mut().take() {
                worker.handle_error(error, Some(handle)).await?;
            } else {
                error!("No payload found while handling error for stream {}!", stream);
            }
        } else {
            error!("No worker found while handling error for stream {}!", stream);
        }
        Ok(())
    }

    async fn force_consistency(&mut self, handle: &mut Act<Reporter>) {
        for (stream_id, worker_id) in self.workers.drain() {
            // push the stream_id back into the streams vector
            self.streams.insert(stream_id);
            // tell worker_id that we lost the response for his request, because we lost scylla connection in
            // middle of request cycle, still this is a rare case.
            worker_id
                .handle_error(WorkerError::Lost, Some(handle))
                .await
                .unwrap_or_else(|e| error!("{}", e));
        }
    }
}

pub fn compute_reporter_num(stream_id: i16, appends_num: i16) -> u8 {
    (stream_id / appends_num) as u8
}

// private functions
fn assign_stream_to_payload(stream: i16, payload: &mut Vec<u8>) {
    payload[2] = (stream >> 8) as u8; // payload[2] is where the first byte of the stream_id should be,
    payload[3] = stream as u8; // payload[3] is the second byte of the stream_id. please refer to cql specs
}

fn is_cql_error(buffer: &[u8]) -> bool {
    buffer[4] == 0
}
