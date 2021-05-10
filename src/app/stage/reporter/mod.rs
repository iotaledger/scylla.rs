// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::{
    app::worker::{Worker, WorkerError},
    cql::{CqlError, Decoder},
};
use anyhow::anyhow;
use sender::SenderHandle;
use std::{borrow::Cow, collections::HashSet, convert::TryFrom};

mod init;
mod run;
mod shutdown;

/// Workers Map holds all the workers_ids
type Workers = HashMap<i16, Box<dyn Worker>>;

#[build]
#[derive(Clone)]
pub fn build_reporter<StageEvent, StageHandle>(
    service: Service,
    session_id: usize,
    reporter_id: u8,
    shard_id: u16,
    streams: HashSet<i16>,
    address: SocketAddr,
    payloads: Payloads,
) -> Reporter {
    let (sender, inbox) = mpsc::unbounded_channel::<ReporterEvent>();
    let handle = ReporterHandle { sender };

    Reporter {
        service,
        address,
        session_id,
        reporter_id,
        streams,
        shard_id,
        workers: HashMap::new(),
        sender_handle: None,
        payloads,
        handle,
        inbox,
    }
}

/// ReporterHandle to be passed to the children (Stage)
#[derive(Clone)]
pub struct ReporterHandle {
    sender: mpsc::UnboundedSender<ReporterEvent>,
}

impl EventHandle<ReporterEvent> for ReporterHandle {
    fn send(&mut self, message: ReporterEvent) -> anyhow::Result<()> {
        self.sender.send(message).ok();
        Ok(())
    }

    fn shutdown(mut self) -> Option<Self>
    where
        Self: Sized,
    {
        if let Ok(()) = self.send(ReporterEvent::Session(Session::Shutdown)) {
            None
        } else {
            Some(self)
        }
    }

    fn update_status(&mut self, service: Service) -> anyhow::Result<()>
    where
        Self: Sized,
    {
        todo!()
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
    streams: HashSet<i16>,
    shard_id: u16,
    workers: Workers,
    sender_handle: Option<SenderHandle>,
    payloads: Payloads,
    handle: ReporterHandle,
    inbox: mpsc::UnboundedReceiver<ReporterEvent>,
}

impl ActorTypes for Reporter {
    const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);

    type Error = Cow<'static, str>;

    fn service(&mut self) -> &mut Service {
        &mut self.service
    }
}

impl EventActor<StageEvent, StageHandle> for Reporter {
    type Event = ReporterEvent;

    type Handle = ReporterHandle;

    fn handle(&mut self) -> &mut Self::Handle {
        &mut self.handle
    }
}

impl Reporter {
    fn force_consistency(&mut self) {
        for (stream_id, worker_id) in self.workers.drain() {
            // push the stream_id back into the streams vector
            self.streams.insert(stream_id);
            // tell worker_id that we lost the response for his request, because we lost scylla connection in
            // middle of request cycle, still this is a rare case.
            worker_id
                .handle_error(WorkerError::Lost, Some(&mut self.handle))
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
