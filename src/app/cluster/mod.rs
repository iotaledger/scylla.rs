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
use std::{
    collections::HashMap,
    net::SocketAddr,
    ops::{Deref, DerefMut},
};

mod event_loop;
mod init;
mod terminating;

pub(crate) type Nodes = HashMap<SocketAddr, NodeInfo>;

// Cluster builder
builder!(ClusterBuilder {
    reporter_count: u8,
    thread_count: usize,
    data_centers: Vec<String>,
    buffer_size: usize,
    recv_buffer_size: Option<u32>,
    send_buffer_size: Option<u32>,
    authenticator: PasswordAuth
});
/// ClusterHandle to be passed to the children (Node)
#[derive(Clone)]
pub struct ClusterHandle {
    tx: mpsc::UnboundedSender<ClusterEvent>,
}
/// ClusterInbox is used to recv events
pub struct ClusterInbox {
    rx: mpsc::UnboundedReceiver<ClusterEvent>,
}

impl Deref for ClusterHandle {
    type Target = mpsc::UnboundedSender<ClusterEvent>;

    fn deref(&self) -> &Self::Target {
        &self.tx
    }
}

impl DerefMut for ClusterHandle {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.tx
    }
}
impl Shutdown for ClusterHandle {
    fn shutdown(self) -> Option<Self>
    where
        Self: Sized,
    {
        let _ = self.tx.send(ClusterEvent::Shutdown);
        None
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
    handle: Option<ClusterHandle>,
    inbox: ClusterInbox,
}

impl Cluster {
    pub(crate) fn clone_handle(&self) -> Option<ClusterHandle> {
        self.handle.clone()
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

impl<H: ScyllaScope> ActorBuilder<ScyllaHandle<H>> for ClusterBuilder {}

/// implementation of builder
impl Builder for ClusterBuilder {
    type State = Cluster;
    fn build(self) -> Self::State {
        let (tx, rx) = mpsc::unbounded_channel::<ClusterEvent>();
        let handle = Some(ClusterHandle { tx });
        let inbox = ClusterInbox { rx };
        // initialize global_ring
        let (arc_ring, _none) = initialize_ring(0, false);
        Self::State {
            service: Service::new(),
            reporter_count: self.reporter_count.unwrap(),
            thread_count: self.thread_count.unwrap(),
            data_centers: self.data_centers.unwrap(),
            buffer_size: self.buffer_size.unwrap(),
            recv_buffer_size: self.recv_buffer_size.unwrap(),
            send_buffer_size: self.send_buffer_size.unwrap(),
            authenticator: self.authenticator.unwrap(),
            nodes: HashMap::new(),
            should_build: false,
            version: 0,
            registry: HashMap::new(),
            arc_ring: Some(arc_ring),
            weak_rings: Vec::new(),
            handle,
            inbox,
        }
        .set_name()
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

/// impl name of the Cluster
impl Name for Cluster {
    fn set_name(mut self) -> Self {
        self.service.update_name("Cluster".to_string());
        self
    }
    fn get_name(&self) -> String {
        self.service.get_name()
    }
}

#[async_trait::async_trait]
impl<H: ScyllaScope> AknShutdown<Cluster> for ScyllaHandle<H> {
    async fn aknowledge_shutdown(self, mut _state: Cluster, _status: Result<(), Need>) {
        _state.service.update_status(ServiceStatus::Stopped);
        let event = ScyllaEvent::Children(ScyllaChild::Cluster(_state.service.clone()));
        let _ = self.send(event);
    }
}
