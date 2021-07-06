// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::{
    app::worker::{Worker, WorkerError},
    cql::{CqlError, Decoder},
};
use std::{collections::HashSet, convert::TryFrom};

/// Workers Map holds all the workers_ids
type Workers = HashMap<i16, Box<dyn Worker>>;

pub(crate) type ReporterId = u8;

/// Reporter state
pub struct Reporter {
    address: SocketAddr,
    pub(crate) reporter_id: ReporterId,
    streams: HashSet<i16>,
    shard_id: u16,
    workers: Workers,
    payloads: Payloads,
}

#[build]
#[derive(Clone)]
pub fn build_reporter(
    reporter_id: ReporterId,
    shard_id: u16,
    streams: HashSet<i16>,
    address: SocketAddr,
    payloads: Payloads,
) -> Reporter {
    Reporter {
        address,
        reporter_id,
        streams,
        shard_id,
        workers: HashMap::new(),
        payloads,
    }
}

#[async_trait]
impl Actor for Reporter {
    type Dependencies = Act<super::sender::Sender>;
    type Event = ReporterEvent;
    type Channel = TokioChannel<Self::Event>;

    async fn run<'a, Reg: RegistryAccess + Send + Sync, Sup: EventDriven>(
        &mut self,
        rt: &mut ActorScopedRuntime<'a, Self, Reg, Sup>,
        mut sender: Self::Dependencies,
    ) -> Result<(), ActorError>
    where
        Self: Sized,
        Sup::Event: SupervisorEvent,
        <Sup::Event as SupervisorEvent>::Children: From<PhantomData<Self>>,
    {
        rt.update_status(ServiceStatus::Running).await.ok();
        let mut my_handle = rt.my_handle().await;
        while let Some(event) = rt.next_event().await {
            match event {
                ReporterEvent::Request { worker, mut payload } => {
                    if let Some(stream) = self.streams.iter().next().cloned() {
                        // Send the event
                        self.streams.remove(&stream);
                        // Assign stream_id to the payload
                        assign_stream_to_payload(stream, &mut payload);
                        // store payload as reusable at payloads[stream]
                        self.payloads[stream as usize].as_mut().replace(payload);
                        self.workers.insert(stream, worker);
                        sender.send(stream).await.unwrap_or_else(|e| error!("{}", e));
                    } else {
                        // Send overload to the worker in-case we don't have anymore streams
                        worker
                            .handle_error(
                                WorkerError::Overload,
                                Some(&mut my_handle.clone().into_inner().into_inner()),
                            )
                            .unwrap_or_else(|e| error!("{}", e));
                    }
                }
                ReporterEvent::Response { stream_id } => {
                    self.handle_response(stream_id, &mut my_handle)
                        .unwrap_or_else(|e| error!("{}", e));
                }
                ReporterEvent::Err(io_error, stream_id) => {
                    self.handle_error(stream_id, WorkerError::Other(io_error), &mut my_handle)
                        .unwrap_or_else(|e| error!("{}", e));
                }
            }
        }
        rt.update_status(ServiceStatus::Stopping).await.ok();
        self.force_consistency(&mut my_handle);
        warn!(
            "reporter_id: {} of shard_id: {} in node: {}, gracefully shutting down.",
            self.reporter_id, self.shard_id, &self.address
        );
        rt.update_status(ServiceStatus::Stopped).await.ok();
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
}

impl Reporter {
    fn handle_response(&mut self, stream: i16, handle: &mut Act<Reporter>) -> anyhow::Result<()> {
        // push the stream_id back to streams vector.
        self.streams.insert(stream);
        // remove the worker from workers.
        if let Some(worker) = self.workers.remove(&stream) {
            if let Some(payload) = self.payloads[stream as usize].as_mut().take() {
                if is_cql_error(&payload) {
                    let error = Decoder::try_from(payload)
                        .and_then(|decoder| CqlError::new(&decoder).map(|e| WorkerError::Cql(e)))
                        .unwrap_or_else(|e| WorkerError::Other(e));
                    worker.handle_error(error, Some(&mut handle.clone().into_inner().into_inner()))?;
                } else {
                    worker.handle_response(payload)?;
                }
            } else {
                error!("No payload found while handling response for stream {}!", stream);
            }
        } else {
            error!("No worker found while handling response for stream {}!", stream);
        }
        Ok(())
    }

    fn handle_error(&mut self, stream: i16, error: WorkerError, handle: &mut Act<Reporter>) -> anyhow::Result<()> {
        // push the stream_id back to streams vector.
        self.streams.insert(stream);
        // remove the worker from workers and send error.
        if let Some(worker) = self.workers.remove(&stream) {
            // drop payload.
            if let Some(_payload) = self.payloads[stream as usize].as_mut().take() {
                worker.handle_error(error, Some(&mut handle.clone().into_inner().into_inner()))?;
            } else {
                error!("No payload found while handling error for stream {}!", stream);
            }
        } else {
            error!("No worker found while handling error for stream {}!", stream);
        }
        Ok(())
    }

    fn force_consistency(&mut self, handle: &mut Act<Reporter>) {
        for (stream_id, worker_id) in self.workers.drain() {
            // push the stream_id back into the streams vector
            self.streams.insert(stream_id);
            // tell worker_id that we lost the response for his request, because we lost scylla connection in
            // middle of request cycle, still this is a rare case.
            worker_id
                .handle_error(WorkerError::Lost, Some(&mut handle.clone().into_inner().into_inner()))
                .unwrap_or_else(|e| error!("{}", e));
        }
    }
}

pub fn compute_reporter_num(stream_id: i16, appends_num: i16) -> ReporterId {
    (stream_id / appends_num) as ReporterId
}

// private functions
fn assign_stream_to_payload(stream: i16, payload: &mut Vec<u8>) {
    payload[2] = (stream >> 8) as u8; // payload[2] is where the first byte of the stream_id should be,
    payload[3] = stream as u8; // payload[3] is the second byte of the stream_id. please refer to cql specs
}

fn is_cql_error(buffer: &[u8]) -> bool {
    buffer[4] == 0
}
