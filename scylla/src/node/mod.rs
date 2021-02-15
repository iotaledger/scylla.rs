// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

/// Import supervisor scope
use super::cluster::*;
/// Import Stage child
use super::stage::*;
/// Import application scope
use crate::application::*;

use std::{
    collections::HashMap,
    net::SocketAddr,
    ops::{Deref, DerefMut},
};

mod event_loop;
mod init;
mod terminating;

// Node builder
builder!(NodeBuilder {
    address: SocketAddr,
    data_center: String,
    reporter_count: u8,
    shard_count: u16,
    buffer_size: usize,
    recv_buffer_size: Option<u32>,
    send_buffer_size: Option<u32>,
    authenticator: PasswordAuth
});

/// NodeHandle to be passed to the children (Stage)
#[derive(Clone)]
pub struct NodeHandle {
    tx: mpsc::UnboundedSender<NodeEvent>,
}
/// NodeInbox is used to recv events
pub struct NodeInbox {
    rx: mpsc::UnboundedReceiver<NodeEvent>,
}

impl Deref for NodeHandle {
    type Target = mpsc::UnboundedSender<NodeEvent>;

    fn deref(&self) -> &Self::Target {
        &self.tx
    }
}

impl DerefMut for NodeHandle {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.tx
    }
}
impl Shutdown for NodeHandle {
    fn shutdown(self) -> Option<Self>
    where
        Self: Sized,
    {
        let _ = self.tx.send(NodeEvent::Shutdown);
        None
    }
}
/// Node event enum.
pub enum NodeEvent {
    /// Shutdown the node.
    Shutdown,
    /// Register the stage reporters.
    RegisterReporters(Service, ReportersHandles),
    /// To keep the node with up to date stage service
    Service(Service),
}
/// Node state
pub struct Node {
    service: Service,
    address: SocketAddr,
    reporters_handles: Option<HashMap<SocketAddr, ReportersHandles>>,
    reporter_count: u8,
    stages: HashMap<u16, StageHandle>,
    shard_count: u16,
    buffer_size: usize,
    recv_buffer_size: Option<u32>,
    send_buffer_size: Option<u32>,
    authenticator: PasswordAuth,
    handle: Option<NodeHandle>,
    inbox: NodeInbox,
}
impl Node {
    pub(crate) fn clone_handle(&self) -> NodeHandle {
        self.handle.clone().unwrap()
    }
}
impl ActorBuilder<ClusterHandle> for NodeBuilder {}

/// implementation of builder
impl Builder for NodeBuilder {
    type State = Node;
    fn build(self) -> Self::State {
        let (tx, rx) = mpsc::unbounded_channel::<NodeEvent>();
        let handle = Some(NodeHandle { tx });
        let inbox = NodeInbox { rx };
        Self::State {
            service: Service::new(),
            address: self.address.unwrap(),
            reporters_handles: Some(HashMap::new()),
            reporter_count: self.reporter_count.unwrap(),
            stages: HashMap::new(),
            shard_count: self.shard_count.unwrap(),
            buffer_size: self.buffer_size.unwrap(),
            recv_buffer_size: self.recv_buffer_size.unwrap(),
            send_buffer_size: self.send_buffer_size.unwrap(),
            authenticator: self.authenticator.unwrap(),
            handle,
            inbox,
        }
        .set_name()
    }
}

/// impl name of the Node
impl Name for Node {
    fn set_name(mut self) -> Self {
        // create name from the address
        let name = self.address.to_string();
        self.service.update_name(name);
        self
    }
    fn get_name(&self) -> String {
        self.service.get_name()
    }
}

#[async_trait::async_trait]
impl AknShutdown<Node> for ClusterHandle {
    async fn aknowledge_shutdown(self, mut _state: Node, _status: Result<(), Need>) {
        _state.service.update_status(ServiceStatus::Stopped);
        let event = ClusterEvent::Service(_state.service.clone());
        let _ = self.send(event);
    }
}
