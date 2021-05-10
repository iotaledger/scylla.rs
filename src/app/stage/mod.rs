// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::{
    node::{NodeEvent, NodeHandle},
    *,
};
use receiver::ReceiverBuilder;
use reporter::ReporterBuilder;
pub use reporter::{ReporterEvent, ReporterHandle};
use sender::SenderBuilder;
use std::{
    cell::UnsafeCell,
    collections::HashMap,
    net::SocketAddr,
    ops::{Deref, DerefMut},
    sync::Arc,
};
use tokio::net::TcpStream;

mod init;
mod receiver;
mod reporter;
mod run;
mod sender;
mod shutdown;

/// The reporters of shard id to its corresponding sender of stage reporter events.
#[derive(Clone, Default)]
pub struct ReportersHandles(HashMap<u8, ReporterHandle>);
/// The thread-safe reusable payloads.
pub type Payloads = Arc<Vec<Reusable>>;

impl Deref for ReportersHandles {
    type Target = HashMap<u8, ReporterHandle>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for ReportersHandles {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl EventHandle<ReporterEvent> for ReportersHandles {
    fn send(&mut self, message: ReporterEvent) -> anyhow::Result<()> {
        todo!()
    }

    fn shutdown(mut self) -> Option<Self>
    where
        Self: Sized,
    {
        for reporter_handle in self.values_mut() {
            reporter_handle
                .send(ReporterEvent::Session(reporter::Session::Shutdown))
                .ok();
        }
        None
    }

    fn update_status(&mut self, service: Service) -> anyhow::Result<()>
    where
        Self: Sized,
    {
        self.send(ReporterEvent::Session(reporter::Session::Service(service)))
    }
}

#[build]
#[derive(Debug, Clone)]
pub fn build_stage<NodeEvent, NodeHandle>(
    service: Service,
    address: SocketAddr,
    authenticator: PasswordAuth,
    reporter_count: u8,
    shard_id: u16,
    buffer_size: usize,
    recv_buffer_size: Option<u32>,
    send_buffer_size: Option<u32>,
) -> Stage {
    let (sender, inbox) = mpsc::unbounded_channel::<StageEvent>();
    let handle = StageHandle { sender };
    // create reusable payloads as giveload
    let vector: Vec<Reusable> = Vec::new();
    let payloads: Payloads = Arc::new(vector);
    Stage {
        service,
        address,
        authenticator,
        appends_num: 32767 / (reporter_count as i16),
        reporter_count,
        reporters_handles: ReportersHandles(HashMap::with_capacity(reporter_count as usize)),
        session_id: 0,
        shard_id,
        payloads,
        buffer_size,
        recv_buffer_size,
        send_buffer_size,
        handle,
        inbox,
    }
}

#[derive(Error, Debug)]
pub enum StageError {
    #[error("Cannot acquire access to reusable payloads!")]
    NoReusablePayloads,
    #[error("Failed to create streams!")]
    CannotCreateStreams,
}

impl Into<ActorError> for StageError {
    fn into(self) -> ActorError {
        match self {
            StageError::NoReusablePayloads => ActorError::RuntimeError(ActorRequest::Panic),
            StageError::CannotCreateStreams => ActorError::RuntimeError(ActorRequest::Panic),
        }
    }
}

/// StageHandle to be passed to the children (reporter/s)
#[derive(Debug, Clone)]
pub struct StageHandle {
    sender: mpsc::UnboundedSender<StageEvent>,
}

impl EventHandle<StageEvent> for StageHandle {
    fn send(&mut self, message: StageEvent) -> anyhow::Result<()> {
        self.sender.send(message)?;
        Ok(())
    }

    fn shutdown(mut self) -> Option<Self>
    where
        Self: Sized,
    {
        if let Ok(()) = self.send(StageEvent::Shutdown) {
            None
        } else {
            Some(self)
        }
    }

    fn update_status(&mut self, service: Service) -> anyhow::Result<()>
    where
        Self: Sized,
    {
        self.send(StageEvent::Reporter(service))
    }
}

/// Stage event enum.
#[derive(Debug)]
pub enum StageEvent {
    /// Reporter child status change
    Reporter(Service),
    /// Establish connection to scylla shard.
    Connect,
    /// Shutdwon a stage.
    Shutdown,
}
/// Stage state
pub struct Stage {
    service: Service,
    address: SocketAddr,
    authenticator: PasswordAuth,
    appends_num: i16,
    reporter_count: u8,
    reporters_handles: ReportersHandles,
    session_id: usize,
    shard_id: u16,
    payloads: Payloads,
    buffer_size: usize,
    recv_buffer_size: Option<u32>,
    send_buffer_size: Option<u32>,
    handle: StageHandle,
    inbox: mpsc::UnboundedReceiver<StageEvent>,
}

impl ActorTypes for Stage {
    const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);

    type Error = StageError;

    fn service(&mut self) -> &mut Service {
        &mut self.service
    }
}

impl EventActor<NodeEvent, NodeHandle> for Stage {
    type Event = StageEvent;

    type Handle = StageHandle;

    fn handle(&mut self) -> &mut Self::Handle {
        &mut self.handle
    }
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
