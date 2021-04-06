// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[async_trait::async_trait]
impl<H: ScyllaScope> EventLoop<ScyllaHandle<H>> for Cluster {
    async fn event_loop(
        &mut self,
        _status: Result<(), Need>,
        supervisor: &mut Option<ScyllaHandle<H>>,
    ) -> Result<(), Need> {
        while let Some(event) = self.inbox.rx.recv().await {
            match event {
                ClusterEvent::Service(microservice) => {
                    self.service.update_microservice(microservice.get_name(), microservice);
                    // keep scylla application up to date with the full tree;
                    // but first let's update the status of the cluster based on the status of the node/s
                    // update the service if it's not already shutting down.
                    if !self.service.is_stopping() {
                        if self.service.microservices.values().all(|ms| ms.is_initializing()) {
                            self.service.update_status(ServiceStatus::Initializing);
                        } else if self.service.microservices.values().all(|ms| ms.is_maintenance()) {
                            self.service.update_status(ServiceStatus::Maintenance);
                        // all the nodes in our Cluster are disconnected and in maintenance,
                        // which is an indicator of an outage
                        // TODO some alerts, logs, call 911 ;)
                        } else if self.service.microservices.values().all(|ms| ms.is_running()) {
                            self.service.update_status(ServiceStatus::Running);
                        } else {
                            self.service.update_status(ServiceStatus::Degraded);
                        }
                    }
                    let event = ScyllaEvent::Children(ScyllaChild::Cluster(self.service.clone()));
                    let _ = supervisor.as_ref().unwrap().send(event);
                }
                // Maybe let the variant to set the PasswordAuth instead of forcing global_auth at the cluster level?
                ClusterEvent::AddNode(address) => {
                    // make sure it doesn't already exist in our cluster
                    if self.nodes.contains_key(&address) {
                        let event = ScyllaEvent::Result(SocketMsg::Scylla(Err(Topology::AddNode(address))));
                        let _ = supervisor.as_ref().unwrap().send(event);
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
                            let node_service = Service::new().set_name(address.to_string());
                            self.service.update_microservice(node_service.get_name(), node_service);
                            let shard_count = cqlconn.shard_count();
                            let dc = cqlconn.take_dc().unwrap();
                            // create node
                            let node = NodeBuilder::new()
                                .address(address.clone())
                                .reporter_count(self.reporter_count)
                                .shard_count(shard_count)
                                .data_center(dc.clone())
                                .buffer_size(self.buffer_size)
                                .recv_buffer_size(self.recv_buffer_size)
                                .send_buffer_size(self.send_buffer_size)
                                .authenticator(self.authenticator.clone())
                                .build();
                            // clone the node_handle
                            let node_handle = node.clone_handle();
                            // take tokens
                            let tokens = cqlconn.take_tokens().unwrap();
                            // get msb
                            let msb = cqlconn.msb();
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
                            tokio::spawn(node.start(self.handle.clone()));
                        }
                        Err(_) => {
                            let event = ScyllaEvent::Result(SocketMsg::Scylla(Err(Topology::AddNode(address))));
                            let _ = supervisor.as_ref().unwrap().send(event);
                        }
                    }
                }
                ClusterEvent::RemoveNode(address) => {
                    // get and remove node_info
                    if let Some(mut node_info) = self.nodes.remove(&address) {
                        // update(remove from) registry
                        for shard_id in 0..node_info.shard_count {
                            // make node_id to reflect the correct shard_id
                            node_info.address.set_port(shard_id);
                            // remove the shard_reporters for "address" node in shard_id from registry
                            self.registry.remove(&node_info.address);
                        }
                        node_info.node_handle.shutdown();
                        // update waiting for build to true
                        self.should_build = true;
                        // note: the node tree will not get shutdown unless we drop the ring
                        // but we cannot drop the ring unless we build a new one and atomically swap it,
                        // therefore dashboard admin supposed to BuildRing
                    } else {
                        // Cannot remove non-existing node.
                        let event = ScyllaEvent::Result(SocketMsg::Scylla(Err(Topology::RemoveNode(address))));
                        let _ = supervisor.as_ref().unwrap().send(event);
                    };
                }
                ClusterEvent::RegisterReporters(microservice, reporters_handles) => {
                    // generate the address of the node we are currently registering its reporters;
                    let address = microservice.get_name().parse().unwrap();
                    // update service
                    self.service.update_microservice(microservice.get_name(), microservice);
                    // merge/add reporters_handles of that node to registry
                    self.registry.extend(reporters_handles);
                    // update waiting for build to true
                    self.should_build = true;
                    // reply to scylla/dashboard
                    let event = ScyllaEvent::Result(SocketMsg::Scylla(Ok(Topology::AddNode(address))));
                    let _ = supervisor.as_ref().unwrap().send(event);
                }
                ClusterEvent::BuildRing(uniform_rf) => {
                    // do cleanup on weaks
                    self.cleanup();
                    let mut microservices = self.service.microservices.values();
                    // make sure non of the nodes is still starting, and ensure should_build is true
                    if !microservices.any(|ms| ms.is_starting()) && self.should_build {
                        // re/build
                        let version = self.new_version();
                        if self.nodes.is_empty() {
                            let (new_arc_ring, old_weak_ring) = initialize_ring(version, true);
                            self.arc_ring.replace(new_arc_ring);
                            self.weak_rings.push(old_weak_ring.unwrap());
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
                        let event = ScyllaEvent::Result(SocketMsg::Scylla(Ok(Topology::BuildRing(uniform_rf))));
                        let _ = supervisor.as_ref().unwrap().send(event);
                    } else {
                        // reply to scylla/dashboard
                        let event = ScyllaEvent::Result(SocketMsg::Scylla(Err(Topology::BuildRing(uniform_rf))));
                        let _ = supervisor.as_ref().unwrap().send(event);
                    }
                }
                ClusterEvent::Shutdown => {
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
                        node_info.node_handle.shutdown();
                    }
                    // build empty ring to enable other threads to build empty ring(eventually)
                    let version = self.new_version();
                    let (new_arc_ring, old_weak_ring) = initialize_ring(version, true);
                    self.arc_ring.replace(new_arc_ring);
                    self.weak_rings.push(old_weak_ring.unwrap());
                    Ring::rebuild();
                    // redo self cleanup on weaks
                    self.cleanup();
                    // drop self.handle
                    self.handle = None;
                }
            }
        }
        Ok(())
    }
}

impl Cluster {
    fn cleanup(&mut self) {
        // total_weak_count = thread_count + 1(the global weak)
        // so we clear all old weaks once weak_count > self.thread_count
        let weak_count = std::sync::Arc::weak_count(self.arc_ring.as_ref().unwrap());
        if weak_count > self.thread_count {
            self.weak_rings.clear();
        };
    }
    fn new_version(&mut self) -> u8 {
        self.version = self.version.wrapping_add(1);
        self.version
    }
}
