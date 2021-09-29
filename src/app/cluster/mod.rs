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
use backstage::{
    core::{
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
    },
    prefab::websocket::{
        GenericResponder,
        JsonMessage,
        Responder,
    },
};

use std::{
    collections::HashMap,
    convert::TryFrom,
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
    Topology(Topology, Responder),
    /// Used by the Node to keep the cluster up to date with its service
    #[report]
    #[eol]
    Microservice(ScopeId, Service),
    /// Shutdown signal
    #[shutdown]
    Shutdown,
}

/// Cluster topology event
#[derive(serde::Deserialize, serde::Serialize)]
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
        log::info!("Cluster is {}", rt.service().status());
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
        // add route to enable configuring the cluster topology over the ws
        rt.add_route::<(JsonMessage, Responder)>().await.ok();
        // cluster always starts with null nodes, therefore its status is IDLE from service perspective
        rt.update_status(ServiceStatus::Idle).await;
        Ok((scylla, reporters_registry))
    }
    async fn run(&mut self, rt: &mut Rt<Self, S>, (scylla, registry): Self::Data) -> ActorResult<()> {
        log::info!("Cluster is {}", rt.service().status());
        let mut data_center = vec![scylla.local_dc];
        while let Some(event) = rt.inbox_mut().next().await {
            match event {
                ClusterEvent::Topology(topology, responder) => {
                    // configure topology only if the cluster is not stopping
                    if rt.service().is_stopping() {
                        let error_response: Result<Topology, _> = Err(topology);
                        responder.inner_reply(error_response).await.ok();
                        continue;
                    }
                    match topology {
                        Topology::AddNode(address) => {
                            // make sure it doesn't already exist in our cluster
                            if self.nodes.contains_key(&address) {
                                let error_response: Result<Topology, _> = Err(Topology::AddNode(address));
                                responder.inner_reply(error_response).await.ok();
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
                                                let ok_response: Result<_, Topology> = Ok(Topology::AddNode(address));
                                                responder.inner_reply(ok_response).await.ok();
                                            }
                                            Err(_err) => {
                                                let error_response: Result<Topology, _> =
                                                    Err(Topology::AddNode(address));
                                                responder.inner_reply(error_response).await.ok();
                                            }
                                        }
                                    } else {
                                        log::error!("Failed to retrieve data from CQL Connection!");
                                    }
                                }
                                Err(_) => {
                                    let error_response: Result<Topology, _> = Err(Topology::AddNode(address));
                                    responder.inner_reply(error_response).await.ok();
                                }
                            }
                        }
                        Topology::RemoveNode(address) => {
                            // get and remove node_info
                            if let Some(node_info) = self.nodes.get(&address) {
                                if let Some(join_handle) = rt.shutdown_child(&node_info.scope_id).await {
                                    // Await till it gets shutdown, it forces sync shutdown
                                    join_handle.await.ok();
                                };
                            } else {
                                // Cannot remove non-existing node.
                                let error_response: Result<Topology, _> = Err(Topology::RemoveNode(address));
                                responder.inner_reply(error_response).await.ok();
                            };
                        }
                        Topology::BuildRing(uniform_rf) => {
                            // todo check if all nodes/children are in stable status
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
                            let ok_response: Result<_, Topology> = Ok(Topology::BuildRing(uniform_rf));
                            responder.inner_reply(ok_response).await.ok();
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
                            if !rt.service().is_running() {
                                log::info!("Cluster is Running");
                            }
                            rt.update_status(ServiceStatus::Running).await;
                        } else if rt.microservices_all(|node| node.is_maintenance()) {
                            if !rt.service().is_maintenance() {
                                log::info!("Cluster is Maintenance");
                            }
                            rt.update_status(ServiceStatus::Maintenance).await;
                        } else {
                            if !rt.service().is_degraded() {
                                log::info!("Cluster is Degraded");
                            }
                            rt.update_status(ServiceStatus::Degraded).await;
                        }
                    } else {
                        if !rt.service().is_stopping() {
                            log::info!("Cluster is Stopping");
                        }
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

impl TryFrom<(JsonMessage, Responder)> for ClusterEvent {
    type Error = anyhow::Error;
    fn try_from((msg, responder): (JsonMessage, Responder)) -> Result<Self, Self::Error> {
        Ok(ClusterEvent::Topology(serde_json::from_str(msg.0.as_ref())?, responder))
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
