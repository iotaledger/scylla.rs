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
                ClusterEvent::RegisterReporters(microservice, reporters_handles) => {}
                ClusterEvent::Service(microservice) => {
                    self.service.update_microservice(microservice.get_name(), microservice);
                    // keep scylla application up to date with the full tree;
                    // but first let's update the status of the cluster based on the status of the node/s
                    let mut microservices = self.service.microservices.values();
                    // update the service if it's not already shutting down.
                    if !self.service.is_stopping() {
                        if microservices.all(|ms| ms.is_initializing()) {
                            self.service.update_status(ServiceStatus::Initializing);
                        } else if microservices.all(|ms| ms.is_maintenance()) {
                            self.service.update_status(ServiceStatus::Maintenance);
                        // all the nodes in our Cluster are disconnected and in maintenance,
                        // which is an indicator of an outage
                        // TODO some alerts, logs, call 911 ;)
                        } else if microservices.all(|ms| ms.is_running()) {
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
                    // to spawn node we first make sure it's online;
                    let cql = CqlBuilder::new()
                        .tokens()
                        .recv_buffer_size(self.recv_buffer_size)
                        .send_buffer_size(self.send_buffer_size)
                        .authenticator(self.authenticator.clone())
                        .build();
                    match cql.await {
                        Ok(mut cqlconn) => {
                            // create node
                            let node = NodeBuilder::new()
                                .address(address.clone())
                                .reporter_count(self.reporter_count)
                                .shard_count(8) // TODO use cqlconn.shard_count()
                                .data_center("TODO".to_string()) // TODO cqlconn.take_dc()
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
                                shard_count: 1, // todo
                                node_handle,
                                data_center: "TODO".to_string(),
                                tokens,
                            };
                            // add node_info to nodes
                            self.nodes.insert(address, node_info);
                            tokio::spawn(node.start(self.handle.clone()));
                        }
                        Err(_) => {
                            // TODO inform scylla app of a unreachable node .
                        }
                    }
                }
                ClusterEvent::RemoveNode(address) => {}
                ClusterEvent::TryBuild(uniform_rf) => {}
                ClusterEvent::Shutdown => {}
            }
        }
        Ok(())
    }
}
