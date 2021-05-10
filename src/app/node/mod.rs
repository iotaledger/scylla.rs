// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::{
    cluster::{ClusterEvent, ClusterHandle},
    stage::{ReportersHandles, StageBuilder, StageEvent, StageHandle},
    *,
};
use std::{borrow::Cow, collections::HashMap, net::SocketAddr};

mod init;
mod run;
mod shutdown;

#[build]
#[derive(Debug, Clone)]
pub fn build_node<ClusterEvent, ClusterHandle>(
    service: Service,
    address: SocketAddr,
    data_center: String,
    reporter_count: u8,
    shard_count: u16,
    buffer_size: usize,
    recv_buffer_size: Option<u32>,
    send_buffer_size: Option<u32>,
    authenticator: PasswordAuth,
) -> Node {
    let (sender, inbox) = mpsc::unbounded_channel::<NodeEvent>();
    let handle = NodeHandle { sender };
    Node {
        service,
        address,
        reporters_handles: Some(HashMap::new()),
        reporter_count,
        stages: HashMap::new(),
        shard_count,
        buffer_size,
        recv_buffer_size,
        send_buffer_size,
        authenticator,
        handle,
        inbox,
    }
}

/// NodeHandle to be passed to the children (Stage)
#[derive(Clone)]
pub struct NodeHandle {
    sender: mpsc::UnboundedSender<NodeEvent>,
}

impl EventHandle<NodeEvent> for NodeHandle {
    fn send(&mut self, message: NodeEvent) -> anyhow::Result<()> {
        self.sender.send(message).ok();
        Ok(())
    }

    fn shutdown(mut self) -> Option<Self>
    where
        Self: Sized,
    {
        if let Ok(()) = self.send(NodeEvent::Shutdown) {
            None
        } else {
            Some(self)
        }
    }

    fn update_status(&mut self, service: Service) -> anyhow::Result<()>
    where
        Self: Sized,
    {
        self.send(NodeEvent::Service(service))
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
    handle: NodeHandle,
    inbox: mpsc::UnboundedReceiver<NodeEvent>,
}

impl ActorTypes for Node {
    const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);

    type Error = Cow<'static, str>;

    fn service(&mut self) -> &mut Service {
        &mut self.service
    }
}

impl EventActor<ClusterEvent, ClusterHandle> for Node {
    type Event = NodeEvent;

    type Handle = NodeHandle;

    fn handle(&mut self) -> &mut Self::Handle {
        &mut self.handle
    }
}
