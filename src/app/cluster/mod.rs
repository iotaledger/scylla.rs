// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::{
    node::{NodeBuilder, NodeHandle},
    *,
};
use crate::app::{
    ring::{build_ring, initialize_ring, ArcRing, Registry, Ring, WeakRing},
    stage::ReportersHandles,
};
use std::{collections::HashMap, net::SocketAddr};

mod init;
mod run;
mod shutdown;

pub(crate) type Nodes = HashMap<SocketAddr, NodeInfo>;

#[build]
#[derive(Debug, Clone)]
pub fn build_cluster<ScyllaEvent, ScyllaHandle>(
    service: Service,
    reporter_count: u8,
    thread_count: usize,
    data_centers: Vec<String>,
    buffer_size: usize,
    recv_buffer_size: Option<u32>,
    send_buffer_size: Option<u32>,
    authenticator: PasswordAuth,
) -> Cluster {
    let (sender, inbox) = mpsc::unbounded_channel::<ClusterEvent>();
    let handle = ClusterHandle { sender };
    let (arc_ring, _none) = initialize_ring(0, false);
    Cluster {
        service,
        reporter_count,
        thread_count,
        data_centers,
        buffer_size,
        recv_buffer_size,
        send_buffer_size,
        authenticator,
        nodes: HashMap::new(),
        should_build: false,
        version: 0,
        registry: HashMap::new(),
        arc_ring: Some(arc_ring),
        weak_rings: Vec::new(),
        handle,
        inbox,
    }
}

#[derive(Error, Debug)]
pub enum ClusterError {}

impl Into<ActorError> for ClusterError {
    fn into(self) -> ActorError {
        todo!()
    }
}

/// ClusterHandle to be passed to the children (Node)
#[derive(Clone)]
pub struct ClusterHandle {
    sender: mpsc::UnboundedSender<ClusterEvent>,
}

impl EventHandle<ClusterEvent> for ClusterHandle {
    fn send(&mut self, message: ClusterEvent) -> anyhow::Result<()> {
        self.sender.send(message).ok();
        Ok(())
    }

    fn shutdown(mut self) -> Option<Self>
    where
        Self: Sized,
    {
        if let Ok(()) = self.send(ClusterEvent::Shutdown) {
            None
        } else {
            Some(self)
        }
    }

    fn update_status(&mut self, service: Service) -> anyhow::Result<()>
    where
        Self: Sized,
    {
        self.send(ClusterEvent::Service(service))
    }
}

/// Cluster state
pub struct Cluster {
    service: Service,
    reporter_count: u8,
    thread_count: usize,
    data_centers: Vec<String>,
    buffer_size: usize,
    recv_buffer_size: Option<u32>,
    send_buffer_size: Option<u32>,
    authenticator: PasswordAuth,
    nodes: Nodes,
    should_build: bool,
    version: u8,
    registry: Registry,
    arc_ring: Option<ArcRing>,
    weak_rings: Vec<Box<WeakRing>>,
    handle: ClusterHandle,
    inbox: mpsc::UnboundedReceiver<ClusterEvent>,
}

impl ActorTypes for Cluster {
    const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);
    type Error = ClusterError;

    fn service(&mut self) -> &mut Service {
        &mut self.service
    }
}

impl EventActor<ScyllaEvent, ScyllaHandle> for Cluster {
    type Event = ClusterEvent;
    type Handle = ClusterHandle;

    fn handle(&mut self) -> &mut Self::Handle {
        &mut self.handle
    }
}

/// Cluster Event type
pub enum ClusterEvent {
    /// Used by the Node to register its reporters with the cluster
    RegisterReporters(Service, HashMap<SocketAddr, ReportersHandles>),
    /// Used by the Node to keep the cluster up to date with its service
    Service(Service),
    /// Used by Scylla/dashboard to add/connect to new scylla node in the cluster
    AddNode(SocketAddr),
    /// Used by Scylla/dashboard to remove/disconnect from existing scylla node in the cluster
    RemoveNode(SocketAddr),
    /// Used by Scylla/dashboard to build new ring and expose the recent cluster topology
    BuildRing(u8),
    /// Used by Scylla/dashboard to shutdown the cluster
    Shutdown,
}

impl From<Topology> for ClusterEvent {
    fn from(topo: Topology) -> Self {
        match topo {
            Topology::AddNode(address) => ClusterEvent::AddNode(address),
            Topology::RemoveNode(address) => ClusterEvent::RemoveNode(address),
            Topology::BuildRing(t) => ClusterEvent::BuildRing(t),
        }
    }
}

/// `NodeInfo` contains the field to identify a ScyllaDB node.
pub struct NodeInfo {
    pub(crate) address: SocketAddr,
    /// in which data_center the scylla node exist
    pub(crate) data_center: String,
    /// it's the node handle for the Node supervisor tree
    pub(crate) node_handle: NodeHandle,
    /// The tokens of all nodes shards.
    pub(crate) tokens: Vec<i64>,
    /// the shard_count in scylla node.
    pub(crate) shard_count: u16,
    /// the most significant bit
    pub(crate) msb: u8,
}
