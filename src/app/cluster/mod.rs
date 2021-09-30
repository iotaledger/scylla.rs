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
        ActorRequest,
        ActorResult,
        EolEvent,
        ReportEvent,
        Rt,
        ScopeId,
        Service,
        ServiceStatus,
        Shutdown,
        ShutdownEvent,
        StreamExt,
        SupHandle,
        UnboundedChannel,
        UnboundedHandle,
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
    Topology(Topology, Option<Responder>),
    /// Used by the Node to keep the cluster up to date with its service
    #[report]
    Microservice(ScopeId, Service, Option<ActorResult<()>>),
    /// Shutdown signal
    #[shutdown]
    Shutdown,
}

impl EolEvent<Node> for ClusterEvent {
    fn eol_event(scope_id: ScopeId, service: Service, _: Node, r: ActorResult<()>) -> Self {
        Self::Microservice(scope_id, service, Some(r))
    }
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
                ClusterEvent::Topology(topology, responder_opt) => {
                    // configure topology only if the cluster is not stopping
                    if rt.service().is_stopping() {
                        if let Some(responder) = responder_opt.as_ref() {
                            let error_response: Result<Topology, _> = Err(topology);
                            responder.inner_reply(error_response).await.ok();
                        }
                        continue;
                    }
                    match topology {
                        Topology::AddNode(address) => {
                            if self.nodes.contains_key(&address) {
                                if let Some(responder) = responder_opt.as_ref() {
                                    let error_response: Result<Topology, _> = Err(Topology::AddNode(address));
                                    responder.inner_reply(error_response).await.ok();
                                    continue;
                                }
                            } else if responder_opt.is_none() {
                                // skip re-adding a node, because it got removed
                                continue;
                            }
                            // to spawn node we first make sure it's online
                            let cql = CqlBuilder::new()
                                .address(address)
                                .tokens()
                                .recv_buffer_size(scylla.recv_buffer_size)
                                .send_buffer_size(scylla.send_buffer_size)
                                .authenticator(scylla.authenticator.clone())
                                .build();
                            match cql.await {
                                Ok(mut cqlconn) => {
                                    log::info!("Successfully connected to node {}!", address);
                                    let shard_count = cqlconn.shard_count();
                                    if let (Some(dc), Some(tokens)) = (cqlconn.take_dc(), cqlconn.take_tokens()) {
                                        // create node
                                        let node = Node::new(address.clone(), shard_count as usize);
                                        // start the node and ensure it got initialized
                                        match rt.start(address.to_string(), node).await {
                                            Ok(h) => {
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
                                                if let Some(responder) = responder_opt.as_ref() {
                                                    rt.update_status(ServiceStatus::Maintenance).await;
                                                    log::info!("Cluster is Maintenance");
                                                    let ok_response: Result<_, Topology> =
                                                        Ok(Topology::AddNode(address));
                                                    responder.inner_reply(ok_response).await.ok();
                                                } else {
                                                    if !rt.service().is_maintenance() {
                                                        // todo force rebuild the ring as this was a reconnect event

                                                        // change status to running (if all nodes are running) or
                                                        // degraded (if any is not running)
                                                        self.update_service_status(rt).await;
                                                    } // else the admin supposed to rebuild the ring
                                                }
                                            }
                                            Err(_err) => {
                                                if let Some(responder) = responder_opt.as_ref() {
                                                    let error_response: Result<Topology, _> =
                                                        Err(Topology::AddNode(address));
                                                    responder.inner_reply(error_response).await.ok();
                                                } else {
                                                    let my_handle = rt.handle().clone();
                                                    Self::restart_node(my_handle, address);
                                                }
                                            }
                                        }
                                    } else {
                                        log::error!("Failed to retrieve data from CQL Connection!");
                                        return Err(ActorError::exit_msg(
                                            "Failed to retrieve data from CQL Connection!",
                                        ));
                                    }
                                }
                                Err(_) => {
                                    if let Some(responder) = responder_opt.as_ref() {
                                        let error_response: Result<Topology, _> = Err(Topology::AddNode(address));
                                        responder.inner_reply(error_response).await.ok();
                                    }
                                    let my_handle = rt.handle().clone();
                                    Self::restart_node(my_handle, address);
                                }
                            }
                        }
                        Topology::RemoveNode(address) => {
                            let responder = responder_opt.as_ref().ok_or_else(|| {
                                ActorError::exit_msg("cannot use remove node topology variant without responder")
                            })?;
                            // get and remove node_info
                            if let Some(node_info) = self.nodes.get(&address) {
                                if let Some(join_handle) = rt.shutdown_child(&node_info.scope_id).await {
                                    // update status to maintenance, as this is a topology event
                                    rt.update_status(ServiceStatus::Maintenance).await;
                                    log::info!("Cluster is Maintenance");
                                    // Await till it gets shutdown, it forces sync shutdown
                                    join_handle.await.ok();
                                    self.nodes.remove(&address);
                                    let ok_response: Result<_, Topology> = Ok(Topology::RemoveNode(address));
                                    responder.inner_reply(ok_response).await.ok();
                                };
                            } else {
                                // Cannot remove non-existing node.
                                let error_response: Result<Topology, _> = Err(Topology::RemoveNode(address));
                                responder.inner_reply(error_response).await.ok();
                            };
                        }
                        Topology::BuildRing(uniform_rf) => {
                            let responder = responder_opt
                                .as_ref()
                                .ok_or_else(|| ActorError::exit_msg("cannot build ring without responder"))?;

                            if self.nodes.len() < uniform_rf as usize {
                                let error_response: Result<Topology, _> = Err(Topology::BuildRing(uniform_rf));
                                responder.inner_reply(error_response).await.ok();
                                continue;
                            }
                            // do cleanup on weaks
                            self.cleanup(scylla.thread_count);
                            // re/build
                            let status_change;
                            if self.nodes.is_empty() {
                                let version = self.new_version();
                                let (new_arc_ring, old_weak_ring) = initialize_ring(version, true);
                                self.arc_ring.replace(new_arc_ring);
                                if let Some(old_weak_ring) = old_weak_ring {
                                    self.weak_rings.push(old_weak_ring);
                                }
                                status_change = ServiceStatus::Idle;
                            } else {
                                let registry_snapshot = registry.read().await.clone();
                                // ensure all nodes are running
                                if rt.microservices_any(|node| !node.is_running())
                                    || self.nodes.len() != rt.service().microservices().len()
                                {
                                    log::error!(
                                        "Unstable cluster, cannot build ring!, fix this by removing any dead node(s)"
                                    );
                                    // the cluster in critical state, we cannot rebuild new ring
                                    if let Some(responder) = responder_opt.as_ref() {
                                        let error_response: Result<Topology, _> = Err(Topology::BuildRing(uniform_rf));
                                        responder.inner_reply(error_response).await.ok();
                                    }
                                }
                                let version = self.new_version();
                                let (new_arc_ring, old_weak_ring) = build_ring(
                                    &mut data_center,
                                    &self.nodes,
                                    registry_snapshot,
                                    scylla.reporter_count,
                                    uniform_rf as usize,
                                    version,
                                );
                                // replace self.arc_ring
                                self.arc_ring.replace(new_arc_ring);
                                // push weak to weak_rings
                                self.weak_rings.push(old_weak_ring);
                                status_change = ServiceStatus::Running;
                            }
                            Ring::rebuild();
                            rt.update_status(status_change).await;
                            let ok_response: Result<_, Topology> = Ok(Topology::BuildRing(uniform_rf));
                            responder.inner_reply(ok_response).await.ok();
                        }
                    }
                }
                ClusterEvent::Microservice(scope_id, service, result_opt) => {
                    if service.is_stopped() {
                        let address: SocketAddr = rt
                            .remove_microservice(scope_id)
                            .ok_or_else(|| ActorError::exit_msg("microservice for stopped node"))?
                            .directory()
                            .as_ref()
                            .ok_or_else(|| ActorError::exit_msg("directory microservice for stopped node"))?
                            .parse()
                            .map_err(ActorError::exit)?;
                        if !rt.service().is_stopping() && self.nodes.contains_key(&address) {
                            // todo force to rebuild the ring with healthy/connected nodes

                            if let Err(ActorError {
                                source: _,
                                request: Some(ActorRequest::Restart(_)),
                            }) = result_opt.expect("node with eol service")
                            {
                                let my_handle = rt.handle().clone();
                                Self::restart_node(my_handle, address);
                            }
                        }
                    } else {
                        rt.upsert_microservice(scope_id, service);
                    }
                    if rt.service().is_maintenance() || rt.service().is_stopping() {
                        rt.update_status(rt.service().status().clone()).await;
                        if rt.service().is_stopping() && rt.microservices_stopped() {
                            rt.inbox_mut().close();
                        }
                    } else {
                        self.update_service_status(rt).await;
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
        Ok(ClusterEvent::Topology(
            serde_json::from_str(msg.0.as_ref())?,
            Some(responder),
        ))
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
    fn restart_node(my_handle: UnboundedHandle<ClusterEvent>, address: SocketAddr) {
        let restart_node_task = async move {
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            my_handle
                .send(ClusterEvent::Topology(Topology::AddNode(address), None))
                .ok();
        };
        backstage::spawn_task(&format!("cluster restarting {} node", address), restart_node_task);
    }
    async fn update_service_status<S: SupHandle<Self>>(&self, rt: &mut Rt<Self, S>) {
        if self.nodes.iter().all(|(_address, node_info)| {
            if let Some(ms_node) = rt.service().microservices().get(&node_info.scope_id) {
                ms_node.is_running()
            } else {
                false
            }
        }) {
            rt.update_status(ServiceStatus::Running).await;
        } else {
            if rt.microservices_stopped() {
                log::warn!("Cluster is Idle");
                rt.update_status(ServiceStatus::Idle).await;
            } else {
                log::warn!("Cluster is Degraded");
                rt.update_status(ServiceStatus::Degraded).await;
            }
        }
    }
}
