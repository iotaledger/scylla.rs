// Copyright 2020 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

/// Import supervisor scope
use super::cluster::*;
/// Import Stage child
use super::stage::*;
/// Import application scope
use crate::application::*;

use std::{
    net::SocketAddr,
    ops::{Deref, DerefMut},
};

mod event_loop;
mod init;
mod terminating;

// Node builder
builder!(NodeBuilder {
    address: SocketAddr,
    //node_id: NodeId,
    //data_center: DC,
    reporter_count: u8,
    shard_count: u8,
    buffer_size: usize,
    recv_buffer_size: Option<usize>,
    send_buffer_size: Option<usize>
    //authenticator: Option<PasswordAuth>
});

/// NodeHandle to be passed to the children (Stage)
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

/// Node event enum.
pub enum NodeEvent {
    /// Shutdown the node.
    Shutdown,
    StageService(Service, Option<ReportersHandles>),
    /* Register the node in its corresponding stage.
     * RegisterReporters(u8, stage::supervisor::Reporters), */
}
// Node state
pub struct Node {
    service: Service,
    address: SocketAddr,
    reporter_count: u8,
    shard_count: u8,
    buffer_size: usize,
    recv_buffer_size: Option<usize>,
    send_buffer_size: Option<usize>,
    handle: Option<NodeHandle>,
    inbox: NodeInbox,
}

impl ActorBuilder<ClusterHandle> for NodeBuilder {}

/// implementation of builder
impl Builder for NodeBuilder {
    type State = Node;
    fn build(self) -> Self::State {
        let (tx, rx) = mpsc::unbounded_channel::<NodeEvent>();
        let handle = Some(NodeHandle { tx });
        let inbox = NodeInbox { rx };
        // TODO initialize global_ring

        Self::State {
            service: Service::new(),
            address: self.address.unwrap(),
            reporter_count: self.reporter_count.unwrap(),
            shard_count: self.shard_count.unwrap(),
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
        // let event = ScyllaEvent::Children(ScyllaChild::Cluster(_state.service.clone(), Some(_status)));
        // let _ = self.send(event);
    }
}
