// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::node::*;
use crate::application::*;
use std::ops::{Deref, DerefMut};

use std::{cell::UnsafeCell, collections::HashMap, net::SocketAddr, sync::Arc};

mod event_loop;
mod init;
mod receiver;
mod reporter;
mod sender;
mod terminating;

pub use reporter::{ReporterEvent, ReporterHandle};

/// The reporters of shard id to its corresponding sender of stage reporter events.
#[derive(Clone)]
pub struct ReportersHandles(HashMap<u8, mpsc::UnboundedSender<reporter::ReporterEvent>>);
/// The thread-safe reusable payloads.
pub type Payloads = Arc<Vec<Reusable>>;

impl Deref for ReportersHandles {
    type Target = HashMap<u8, mpsc::UnboundedSender<reporter::ReporterEvent>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for ReportersHandles {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Shutdown for ReportersHandles {
    fn shutdown(self) -> Option<Self>
    where
        Self: Sized,
    {
        for reporter_handle in self.values() {
            reporter_handle.send(ReporterEvent::Session(reporter::Session::Shutdown));
        }
        None
    }
}

// Stage builder
builder!(StageBuilder {
    address: SocketAddr,
    reporter_count: u8,
    shard_id: u8,
    buffer_size: usize,
    recv_buffer_size: Option<usize>,
    send_buffer_size: Option<usize>,
    //authenticator: Option<PasswordAuth>
    handle: StageHandle,
    inbox: StageInbox
});

/// StageHandle to be passed to the children (reporter/s)
pub struct StageHandle {
    tx: mpsc::UnboundedSender<StageEvent>,
}
/// StageInbox is used to recv events
pub struct StageInbox {
    rx: mpsc::UnboundedReceiver<StageEvent>,
}

impl Deref for StageHandle {
    type Target = mpsc::UnboundedSender<StageEvent>;

    fn deref(&self) -> &Self::Target {
        &self.tx
    }
}

impl DerefMut for StageHandle {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.tx
    }
}

/// Stage event enum.
pub enum StageEvent {
    // Connect to a shard.
    // Connect(sender::Sender, sender::Receiver),
    /// Reporter child status change
    Reporter(Service),
    /// Establish connection to scylla shard.
    Connect,
    /// Shutdwon a stage.
    Shutdown,
}
// Stage state
pub struct Stage {
    service: Service,
    address: SocketAddr,
    reporter_count: u8,
    reporters_handles: Option<ReportersHandles>,
    session_id: usize,
    reconnect_requests: u8,
    connected: bool,
    shard_id: u8,
    payloads: Payloads,
    buffer_size: usize,
    recv_buffer_size: Option<usize>,
    send_buffer_size: Option<usize>,
    handle: Option<StageHandle>,
    inbox: StageInbox,
}

#[derive(Default)]
/// The reusable sender payload.
pub struct Reusable {
    value: UnsafeCell<Option<Vec<u8>>>,
}
impl Reusable {
    #[allow(clippy::mut_from_ref)]
    /// Return as mutable sender payload value.
    pub fn as_mut(&self) -> &mut Option<Vec<u8>> {
        unsafe { self.value.get().as_mut().unwrap() }
    }
    /// Return as reference sender payload.
    pub fn as_ref_payload(&self) -> Option<&Vec<u8>> {
        unsafe { self.value.get().as_ref().unwrap().as_ref() }
    }
    /// Return as mutable sender payload.
    pub fn as_mut_payload(&self) -> Option<&mut Vec<u8>> {
        self.as_mut().as_mut()
    }
}
unsafe impl Sync for Reusable {}

impl ActorBuilder<NodeHandle> for StageBuilder {}

/// implementation of builder
impl Builder for StageBuilder {
    type State = Stage;
    fn build(self) -> Self::State {
        let (tx, rx) = mpsc::unbounded_channel::<StageEvent>();
        let handle = Some(StageHandle { tx });
        let inbox = StageInbox { rx };
        // create reusable payloads as giveload
        let vector: Vec<Reusable> = Vec::new();
        let payloads: Payloads = Arc::new(vector);
        let reporter_count = self.reporter_count.unwrap();
        Self::State {
            service: Service::new(),
            address: self.address.unwrap(),
            reporter_count,
            reporters_handles: Some(ReportersHandles(HashMap::with_capacity(reporter_count as usize))),
            session_id: 0,
            reconnect_requests: 0,
            connected: false,
            shard_id: self.shard_id.unwrap(),
            payloads,
            buffer_size: self.buffer_size.unwrap(),
            recv_buffer_size: self.recv_buffer_size.unwrap(),
            send_buffer_size: self.send_buffer_size.unwrap(),
            handle,
            inbox,
        }
        .set_name()
    }
}

/// impl name of the Node
impl Name for Stage {
    fn set_name(mut self) -> Self {
        // create name from the shard_id
        let name = self.shard_id.to_string();
        self.service.update_name(name);
        self
    }
    fn get_name(&self) -> String {
        self.service.get_name()
    }
}

#[async_trait::async_trait]
impl AknShutdown<Stage> for NodeHandle {
    async fn aknowledge_shutdown(self, mut _state: Stage, _status: Result<(), Need>) {
        _state.service.update_status(ServiceStatus::Stopped);
        // let event = ScyllaEvent::Children(ScyllaChild::Cluster(_state.service.clone(), Some(_status)));
        // let _ = self.send(event);
    }
}
