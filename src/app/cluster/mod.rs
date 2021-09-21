// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::node::Node;
use crate::{
    app::ring::{
        build_ring,
        initialize_ring,
        ArcRing,
        Registry,
        Ring,
        WeakRing,
    },
    cql::CqlBuilder,
};
use async_trait::async_trait;

use super::Scylla;
use backstage::core::{
    Actor,
    ActorError,
    ActorResult,
    Rt,
    ScopeId,
    Service,
    ServiceStatus,
    Shutdown,
    StreamExt,
    SupHandle,
    UnboundedChannel,
};

use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::Arc,
};
use tokio::sync::RwLock;
pub(crate) type Nodes = HashMap<SocketAddr, NodeInfo>;

/// Cluster state
pub struct Cluster {
    nodes: Nodes,
    version: u8,
    arc_ring: Option<ArcRing>,
    weak_rings: Vec<Box<WeakRing>>,
}

/// Cluster Event type
#[backstage::core::supervise]
pub enum ClusterEvent {
    /// Topology configuration
    Topology(Topology), // todo add responder
    /// Used by the Node to keep the cluster up to date with its service
    #[report]
    #[eol]
    Microservice(ScopeId, Service),
    /// Shutdown signal
    #[shutdown]
    Shutdown,
}

/// Cluster topology event
pub enum Topology {
    /// Used by Scylla/dashboard to add/connect to new scylla node in the cluster
    AddNode(SocketAddr),
    /// Used by Scylla/dashboard to remove/disconnect from existing scylla node in the cluster
    RemoveNode(SocketAddr),
    /// Used by Scylla/dashboard to build new ring and expose the recent cluster topology
    BuildRing(u8),
}

impl Cluster {
    /// Create new cluster with empty state
    pub fn new() -> Self {
        let nodes = HashMap::new();
        let version = Ring::version();
        // initialize global_ring
        let (arc_ring, _none) = initialize_ring(version, false);
        Self {
            nodes,
            version,
            arc_ring: Some(arc_ring),
            weak_rings: Vec::new(),
        }
    }
}

/// `NodeInfo` contains the field to identify a ScyllaDB node.
pub struct NodeInfo {
    /// The scope id of the node
    pub(crate) scope_id: ScopeId,
    /// The address of the node
    pub(crate) address: SocketAddr,
    /// in which data_center the scylla node exist
    pub(crate) data_center: String,
    /// The tokens of all nodes shards.
    pub(crate) tokens: Vec<i64>,
    /// the shard_count in scylla node.
    pub(crate) shard_count: u16,
    /// the most significant bit
    pub(crate) msb: u8,
}

/// The Cluster actor lifecycle implementation
#[async_trait]
impl<S> Actor<S> for Cluster
where
    S: SupHandle<Self>,
{
    type Data = (Scylla, Arc<RwLock<Registry>>);
    type Channel = UnboundedChannel<ClusterEvent>;
    async fn init(&mut self, rt: &mut Rt<Self, S>) -> ActorResult<Self::Data> {
        // add empty registry as resource
        let reporters_registry = Arc::new(RwLock::new(Registry::new()));
        rt.publish(reporters_registry.clone()).await;
        let parent_id = rt
            .parent_id()
            .ok_or_else(|| ActorError::exit_msg("cluster without scylla supervisor"))?;
        let scylla = rt
            .lookup::<Scylla>(parent_id)
            .await
            .ok_or_else(|| ActorError::exit_msg("cluster unables to lookup for scylla as config"))?;
        Ok((scylla, reporters_registry))
    }
    async fn run(&mut self, rt: &mut Rt<Self, S>, (scylla, registry): Self::Data) -> ActorResult<()> {
        // todo remove this for test only
        {
            let add_node = Topology::AddNode(([172, 17, 0, 2], 19042).into());
            rt.handle().send(ClusterEvent::Topology(add_node)).ok();
            let build_ring = Topology::BuildRing(1);
            rt.handle().send(ClusterEvent::Topology(build_ring)).ok();
        }
        let mut data_center = vec![scylla.local_dc];
        while let Some(event) = rt.inbox_mut().next().await {
            match event {
                ClusterEvent::Topology(topology) => {
                    // configure topology only if the cluster is not stopping
                    if rt.service().is_stopping() {
                        // respond to caller with topology error Err(topology)
                        continue;
                    }
                    match topology {
                        Topology::AddNode(address) => {
                            // make sure it doesn't already exist in our cluster
                            if self.nodes.contains_key(&address) {
                                // todo response to caller using the todo responder
                                continue;
                            }
                            // to spawn node we first make sure it's online;
                            let cql = CqlBuilder::new()
                                .address(address)
                                .tokens()
                                .recv_buffer_size(scylla.recv_buffer_size)
                                .send_buffer_size(scylla.send_buffer_size)
                                .authenticator(scylla.authenticator.clone())
                                .build();
                            match cql.await {
                                Ok(mut cqlconn) => {
                                    log::info!("Successfully connected to node!");
                                    let shard_count = cqlconn.shard_count();
                                    if let (Some(dc), Some(tokens)) = (cqlconn.take_dc(), cqlconn.take_tokens()) {
                                        // create node
                                        let node = Node::new(address.clone(), shard_count as usize);
                                        // start the node and ensure it got initialized
                                        match rt.start(address.to_string(), node).await {
                                            Ok(h) => {
                                                // Successfully added/spawned/started node
                                                // create nodeinfo
                                                let node_info = NodeInfo {
                                                    scope_id: h.scope_id(),
                                                    address: address.clone(),
                                                    msb: cqlconn.msb(),
                                                    shard_count,
                                                    data_center: dc,
                                                    tokens,
                                                };
                                                // add node_info to nodes
                                                self.nodes.insert(address, node_info);
                                            }
                                            Err(_err) => {
                                                // tell caller using responder
                                            }
                                        }
                                    } else {
                                        log::error!("Failed to retrieve data from CQL Connection!");
                                    }
                                }
                                Err(_) => {
                                    // tell caller using responder
                                }
                            }
                        }
                        Topology::RemoveNode(address) => {
                            // get and remove node_info
                            if let Some(node_info) = self.nodes.get(&address) {
                                rt.shutdown_child(&node_info.scope_id).await;
                                todo!("tell responder")
                            } else {
                                // Cannot remove non-existing node.
                                todo!("tell responder")
                            };
                        }
                        Topology::BuildRing(uniform_rf) => {
                            // do cleanup on weaks
                            self.cleanup(scylla.thread_count);
                            // re/build
                            let version = self.new_version();
                            if self.nodes.is_empty() {
                                let (new_arc_ring, old_weak_ring) = initialize_ring(version, true);
                                self.arc_ring.replace(new_arc_ring);
                                if let Some(old_weak_ring) = old_weak_ring {
                                    self.weak_rings.push(old_weak_ring);
                                }
                            } else {
                                let (new_arc_ring, old_weak_ring) = build_ring(
                                    &mut data_center,
                                    &self.nodes,
                                    registry.read().await.clone(),
                                    scylla.reporter_count,
                                    uniform_rf as usize,
                                    version,
                                );
                                // replace self.arc_ring
                                self.arc_ring.replace(new_arc_ring);
                                // push weak to weak_rings
                                self.weak_rings.push(old_weak_ring);
                            }
                            Ring::rebuild();
                            // todo respond with success
                        }
                    }
                }
                ClusterEvent::Microservice(scope_id, service) => {
                    if service.is_stopped() {
                        rt.remove_microservice(scope_id);
                        // remove it from node
                        let address: SocketAddr = service
                            .directory()
                            .as_ref()
                            .expect("directory for node")
                            .parse()
                            .expect("address as node directory name");
                        self.nodes.remove(&address);
                    } else {
                        rt.upsert_microservice(scope_id, service);
                    }
                    if !rt.service().is_stopping() {
                        if rt.microservices_all(|node| node.is_running()) {
                            rt.update_status(ServiceStatus::Running).await;
                        } else if rt.microservices_all(|node| node.is_maintenance()) {
                            rt.update_status(ServiceStatus::Maintenance).await;
                        } else {
                            rt.update_status(ServiceStatus::Degraded).await;
                        }
                    } else {
                        rt.update_status(ServiceStatus::Stopping).await;
                        if rt.microservices_stopped() {
                            rt.inbox_mut().close();
                        }
                    }
                }
                ClusterEvent::Shutdown => {
                    self.cleanup(scylla.thread_count);
                    // stop all the children/nodes
                    rt.stop().await;
                    // build empty ring to enable other threads to build empty ring(eventually)
                    let version = self.new_version();
                    let (new_arc_ring, old_weak_ring) = initialize_ring(version, true);
                    self.arc_ring.replace(new_arc_ring);
                    if let Some(old_weak_ring) = old_weak_ring {
                        self.weak_rings.push(old_weak_ring);
                    }
                    Ring::rebuild();
                    // redo self cleanup on weaks
                    self.cleanup(scylla.thread_count);
                    if rt.microservices_stopped() {
                        rt.inbox_mut().close();
                    }
                }
            }
        }
        Ok(())
    }
}

impl Cluster {
    fn cleanup(&mut self, thread_count: usize) {
        // total_weak_count = thread_count + 1(the global weak)
        // so we clear all old weaks once weak_count > thread_count
        if let Some(arc_ring) = self.arc_ring.as_ref() {
            let weak_count = std::sync::Arc::weak_count(arc_ring);
            if weak_count > thread_count {
                self.weak_rings.clear();
            };
        } else {
            log::error!("Cleanup failed!")
        }
    }
    fn new_version(&mut self) -> u8 {
        self.version = self.version.wrapping_add(1);
        self.version
    }
}
