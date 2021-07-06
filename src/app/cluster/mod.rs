// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use tokio::sync::oneshot;

use super::{
    node::{Node, NodeEvent},
    stage::Reporter,
    websocket::Topology,
    *,
};
use crate::app::ring::{build_ring, initialize_ring, ArcRing, Registry, Ring, WeakRing};
use std::{collections::HashMap, net::SocketAddr};

pub(crate) type Nodes = HashMap<SocketAddr, NodeInfo>;

/// Cluster state
pub struct Cluster {
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
}

#[build]
#[derive(Debug, Clone)]
pub fn build_cluster(
    reporter_count: u8,
    thread_count: usize,
    data_centers: Vec<String>,
    buffer_size: usize,
    recv_buffer_size: Option<u32>,
    send_buffer_size: Option<u32>,
    authenticator: PasswordAuth,
) -> Cluster {
    let (arc_ring, _none) = initialize_ring(0, false);
    Cluster {
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
    }
}

#[async_trait]
impl Actor for Cluster {
    type Dependencies = ();
    type Event = ClusterEvent;
    type Channel = TokioChannel<Self::Event>;

    async fn run<'a, Reg: RegistryAccess + Send + Sync, Sup: EventDriven>(
        &mut self,
        rt: &mut ActorScopedRuntime<'a, Self, Reg, Sup>,
        _deps: Self::Dependencies,
    ) -> Result<(), ActorError>
    where
        Self: Sized,
        Sup::Event: SupervisorEvent,
        <Sup::Event as SupervisorEvent>::Children: From<PhantomData<Self>>,
    {
        rt.update_status(ServiceStatus::Running).await.ok();
        let mut my_handle = rt.my_handle().await;
        let mut reporter_pools: Option<HashMap<SocketAddr, Pool<Reporter, u8>>> = None;
        let mut last_uniform_rf = None;
        while let Some(event) = rt.next_event().await {
            match event {
                // Maybe let the variant to set the PasswordAuth instead of forcing global_auth at the cluster
                // level?
                ClusterEvent::AddNode(address, responder) => {
                    info!("Received add node event!");
                    // make sure it doesn't already exist in our cluster
                    if self.nodes.contains_key(&address) {
                        responder.map(|r| r.send(Err(Topology::AddNode(address))));
                        continue;
                    }
                    // to spawn node we first make sure it's online;
                    let cql = CqlBuilder::new()
                        .address(address)
                        .tokens()
                        .recv_buffer_size(self.recv_buffer_size)
                        .send_buffer_size(self.send_buffer_size)
                        .authenticator(self.authenticator.clone())
                        .build();
                    match cql.await {
                        Ok(mut cqlconn) => {
                            // add it as microservice
                            let shard_count = cqlconn.shard_count();
                            if let (Some(dc), Some(tokens)) = (cqlconn.take_dc(), cqlconn.take_tokens()) {
                                // create node
                                let node = node::NodeBuilder::new()
                                    .address(address.clone())
                                    .reporter_count(self.reporter_count)
                                    .shard_count(shard_count)
                                    .data_center(dc.clone())
                                    .buffer_size(self.buffer_size)
                                    .recv_buffer_size(self.recv_buffer_size)
                                    .send_buffer_size(self.send_buffer_size)
                                    .authenticator(self.authenticator.clone())
                                    .build();
                                // get msb
                                let msb = cqlconn.msb();

                                let (_, node_handle) = rt.spawn_actor(node, my_handle.clone()).await;
                                // create nodeinfo
                                let node_info = NodeInfo {
                                    address: address.clone(),
                                    msb,
                                    shard_count,
                                    node_handle,
                                    data_center: dc,
                                    tokens,
                                };
                                // add node_info to nodes
                                self.nodes.insert(address, node_info);
                                responder.map(|r| r.send(Ok(Topology::AddNode(address))));
                            } else {
                                responder.map(|r| r.send(Err(Topology::AddNode(address))));
                                error!("Failed to retrieve data from CQL Connection!");
                            }
                        }
                        Err(_) => {
                            error!("Scylla node at {} is unavailable!", address);
                            responder.map(|r| r.send(Err(Topology::AddNode(address))));
                        }
                    }
                }
                ClusterEvent::RemoveNode(address, responder) => {
                    // get and remove node_info
                    if let Some(mut node_info) = self.nodes.remove(&address) {
                        // update(remove from) registry
                        for shard_id in 0..node_info.shard_count {
                            // make node_id to reflect the correct shard_id
                            node_info.address.set_port(shard_id);
                            // remove the shard_reporters for "address" node in shard_id from registry
                            self.registry.remove(&node_info.address);
                        }
                        node_info.node_handle.send(NodeEvent::Shutdown).await.ok();
                        // update waiting for build to true
                        self.should_build = true;
                        // note: the node tree will not get shutdown unless we drop the ring
                        // but we cannot drop the ring unless we build a new one and atomically swap it,
                        // therefore dashboard admin supposed to BuildRing
                        responder.map(|r| r.send(Ok(Topology::RemoveNode(address))));
                    } else {
                        // Cannot remove non-existing node.
                        responder.map(|r| r.send(Err(Topology::RemoveNode(address))));
                    };
                }
                ClusterEvent::RegisterReporters(pools) => {
                    match reporter_pools {
                        Some(ref mut map) => {
                            map.extend(pools);
                        }
                        None => {
                            reporter_pools = Some(pools);
                        }
                    }
                    // update waiting for build to true
                    self.should_build = true;
                }
                ClusterEvent::BuildRing(uniform_rf, responder) => {
                    if self.should_build {
                        last_uniform_rf = Some(uniform_rf);
                        for (addr, pool) in reporter_pools.as_ref().unwrap() {
                            let handles = pool
                                .write()
                                .await
                                .iter_with_metrics()
                                .map(|(id, h)| (*id, h.clone().into_inner().into_inner()))
                                .collect::<HashMap<_, _>>();

                            self.registry.insert(*addr, handles);
                        }
                        // do cleanup on weaks
                        self.cleanup();
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
                                &mut self.data_centers,
                                &self.nodes,
                                self.registry.clone(),
                                self.reporter_count,
                                uniform_rf as usize,
                                version,
                            );
                            // replace self.arc_ring
                            self.arc_ring.replace(new_arc_ring);
                            // push weak to weak_rings
                            self.weak_rings.push(old_weak_ring);
                        }
                        Ring::rebuild();
                        // reset should_build state to false becaue we built it and we don't want to rebuild again
                        // incase of another BuildRing event
                        self.should_build = false;
                        // reply to scylla/dashboard
                        responder.map(|r| r.send(Ok(Topology::BuildRing(uniform_rf))));
                    } else {
                        my_handle
                            .send(ClusterEvent::BuildRing(uniform_rf, responder))
                            .await
                            .ok();
                        //responder.send(Err(Topology::BuildRing(uniform_rf)));
                    }
                }
                ClusterEvent::ReportExit(res) => match res {
                    Ok(s) => {
                        my_handle
                            .send(ClusterEvent::RemoveNode(s.state.address, None))
                            .await
                            .ok();
                        my_handle
                            .send(ClusterEvent::BuildRing(last_uniform_rf.unwrap_or(1), None))
                            .await
                            .ok();
                    }
                    Err(e) => match e.error.request() {
                        ActorRequest::Restart => {
                            let address = e.state.address;
                            my_handle.send(ClusterEvent::RemoveNode(address, None)).await.ok();
                            my_handle.send(ClusterEvent::AddNode(address, None)).await.ok();
                            my_handle
                                .send(ClusterEvent::BuildRing(last_uniform_rf.unwrap_or(1), None))
                                .await
                                .ok();
                        }
                        ActorRequest::Reschedule(dur) => {
                            let mut handle_clone = my_handle.clone();
                            let address = e.state.address;
                            my_handle.send(ClusterEvent::RemoveNode(address, None)).await.ok();
                            let dur = *dur;
                            tokio::spawn(async move {
                                tokio::time::sleep(dur).await;
                                handle_clone.send(ClusterEvent::AddNode(address, None)).await.ok();
                                handle_clone
                                    .send(ClusterEvent::BuildRing(last_uniform_rf.unwrap_or(1), None))
                                    .await
                                    .ok();
                            });
                        }
                        ActorRequest::Finish => {
                            error!("{}", e.error);
                            break;
                        }
                        ActorRequest::Panic => panic!("{}", e.error),
                    },
                },
                ClusterEvent::StatusChange(s) => {
                    // TODO
                }
            }
        }
        rt.update_status(ServiceStatus::Stopping).await.ok();
        // do self cleanup on weaks
        self.cleanup();
        // shutdown everything and drop self.tx
        for (_, mut node_info) in self.nodes.drain() {
            for shard_id in 0..node_info.shard_count {
                // make address port to reflect the correct shard_id
                node_info.address.set_port(shard_id);
                // remove the shard_reporters for "address" node in shard_id from registry
                self.registry.remove(&node_info.address);
            }
        }
        // build empty ring to enable other threads to build empty ring(eventually)
        let version = self.new_version();
        let (new_arc_ring, old_weak_ring) = initialize_ring(version, true);
        self.arc_ring.replace(new_arc_ring);
        if let Some(old_weak_ring) = old_weak_ring {
            self.weak_rings.push(old_weak_ring);
        }
        Ring::rebuild();
        // redo self cleanup on weaks
        self.cleanup();
        rt.update_status(ServiceStatus::Stopped).await.ok();
        Ok(())
    }
}

impl Cluster {
    fn cleanup(&mut self) {
        // total_weak_count = thread_count + 1(the global weak)
        // so we clear all old weaks once weak_count > self.thread_count
        if let Some(arc_ring) = self.arc_ring.as_ref() {
            let weak_count = std::sync::Arc::weak_count(arc_ring);
            if weak_count > self.thread_count {
                self.weak_rings.clear();
            };
        } else {
            error!("Cleanup failed!")
        }
    }
    fn new_version(&mut self) -> u8 {
        self.version = self.version.wrapping_add(1);
        self.version
    }
}

/// Cluster Event type
#[supervise(Node)]
pub enum ClusterEvent {
    /// Used by the Node to register its reporters with the cluster
    RegisterReporters(HashMap<SocketAddr, Pool<Reporter, u8>>),
    /// Used by Scylla/dashboard to add/connect to new scylla node in the cluster
    AddNode(SocketAddr, Option<oneshot::Sender<Result<Topology, Topology>>>),
    /// Used by Scylla/dashboard to remove/disconnect from existing scylla node in the cluster
    RemoveNode(SocketAddr, Option<oneshot::Sender<Result<Topology, Topology>>>),
    /// Used by Scylla/dashboard to build new ring and expose the recent cluster topology
    BuildRing(u8, Option<oneshot::Sender<Result<Topology, Topology>>>),
}
/// `NodeInfo` contains the field to identify a ScyllaDB node.
pub struct NodeInfo {
    pub(crate) address: SocketAddr,
    /// in which data_center the scylla node exist
    pub(crate) data_center: String,
    /// it's the node handle for the Node supervisor tree
    pub(crate) node_handle: Act<Node>,
    /// The tokens of all nodes shards.
    pub(crate) tokens: Vec<i64>,
    /// the shard_count in scylla node.
    pub(crate) shard_count: u16,
    /// the most significant bit
    pub(crate) msb: u8,
}
