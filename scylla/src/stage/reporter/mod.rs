// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::worker::{Worker, WorkerError};
use anyhow::anyhow;
use scylla_cql::{CqlError, Decoder};
use sender::SenderHandle;
use std::{
    convert::TryFrom,
    ops::{Deref, DerefMut},
};

mod event_loop;
mod init;
mod terminating;

/// Workers Map holds all the workers_ids
type Workers = HashMap<i16, Box<dyn Worker>>;

// Reporter builder
builder!(ReporterBuilder {
    session_id: usize,
    reporter_id: u8,
    shard_id: u16,
    streams: Vec<i16>,
    address: SocketAddr,
    payloads: Payloads
});

/// ReporterHandle to be passed to the children (Stage)
#[derive(Clone)]
pub struct ReporterHandle {
    tx: mpsc::UnboundedSender<ReporterEvent>,
}
/// NodeInbox is used to recv events
pub struct ReporterInbox {
    rx: mpsc::UnboundedReceiver<ReporterEvent>,
}

impl Deref for ReporterHandle {
    type Target = mpsc::UnboundedSender<ReporterEvent>;

    fn deref(&self) -> &Self::Target {
        &self.tx
    }
}

impl DerefMut for ReporterHandle {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.tx
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

pub enum Session {
    New(Service, sender::SenderHandle),
    Service(Service),
    Shutdown,
}

/// Reporter state
pub struct Reporter {
    service: Service,
    address: SocketAddr,
    session_id: usize,
    reporter_id: u8,
    streams: Vec<i16>,
    shard_id: u16,
    workers: Workers,
    sender_handle: Option<SenderHandle>,
    payloads: Payloads,
    handle: Option<ReporterHandle>,
    inbox: ReporterInbox,
}

impl Reporter {
    pub fn clone_handle(&self) -> Option<ReporterHandle> {
        self.handle.clone()
    }
}

impl ActorBuilder<StageHandle> for ReporterBuilder {}

/// implementation of builder
impl Builder for ReporterBuilder {
    type State = Reporter;
    fn build(self) -> Self::State {
        let (tx, rx) = mpsc::unbounded_channel::<ReporterEvent>();
        let handle = Some(ReporterHandle { tx });
        let inbox = ReporterInbox { rx };

        Self::State {
            service: Service::new(),
            address: self.address.unwrap(),
            session_id: self.session_id.unwrap(),
            reporter_id: self.reporter_id.unwrap(),
            streams: self.streams.unwrap(),
            shard_id: self.shard_id.unwrap(),
            workers: HashMap::new(),
            sender_handle: None,
            payloads: self.payloads.unwrap(),
            handle,
            inbox,
        }
        .set_name()
    }
}

/// impl name of the Reporter
impl Name for Reporter {
    fn set_name(mut self) -> Self {
        // create name from the reporter_id
        let name = self.reporter_id.to_string();
        self.service.update_name(name);
        self
    }
    fn get_name(&self) -> String {
        self.service.get_name()
    }
}

#[async_trait::async_trait]
impl AknShutdown<Reporter> for StageHandle {
    async fn aknowledge_shutdown(self, mut state: Reporter, _status: Result<(), Need>) {
        state.service.update_status(ServiceStatus::Stopped);
        let event = StageEvent::Reporter(state.service);
        let _ = self.send(event);
    }
}

impl Reporter {
    fn force_consistency(&mut self) {
        for (stream_id, worker_id) in self.workers.drain() {
            // push the stream_id back into the streams vector
            self.streams.push(stream_id);
            // tell worker_id that we lost the response for his request, because we lost scylla connection in
            // middle of request cycle, still this is a rare case.
            worker_id.handle_error(WorkerError::Lost, &self.handle);
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
