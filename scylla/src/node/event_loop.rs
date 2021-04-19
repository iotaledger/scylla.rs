// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[async_trait::async_trait]
impl EventLoop<ClusterHandle> for Node {
    async fn event_loop(
        &mut self,
        _status: Result<(), Need>,
        supervisor: &mut Option<ClusterHandle>,
    ) -> Result<(), Need> {
        if let Some(supervisor) = supervisor {
            while let Some(event) = self.inbox.rx.recv().await {
                match event {
                    NodeEvent::RegisterReporters(microservice, reporters_handles) => {
                        if let Ok(shard_id) = microservice.get_name().parse::<u16>() {
                            let mut socket_addr = self.address.clone();
                            self.service.update_microservice(microservice.get_name(), microservice);
                            // assign shard_id to socket_addr as it's going be used later as key in registry
                            socket_addr.set_port(shard_id);
                            if let Some(reporters_handles_ref) = self.reporters_handles.as_mut() {
                                reporters_handles_ref.insert(socket_addr, reporters_handles);
                                // check if we pushed all reporters of the node.
                                if reporters_handles_ref.len() == self.shard_count as usize {
                                    // reporters_handles should be passed to cluster supervisor
                                    if let Some(reporters_handles) = self.reporters_handles.take() {
                                        let event =
                                            ClusterEvent::RegisterReporters(self.service.clone(), reporters_handles);
                                        supervisor.send(event).ok();
                                    }
                                } else {
                                    let event = ClusterEvent::Service(self.service.clone());
                                    supervisor.send(event).ok();
                                }
                            } else {
                                error!("Tried to register reporters more than once!")
                            }
                        } else {
                            error!("Failed to parse shard ID!")
                        }
                    }
                    NodeEvent::Service(microservice) => {
                        self.service.update_microservice(microservice.get_name(), microservice);
                        if !self.service.is_stopping() {
                            let microservices_len = self.service.microservices.len();
                            if self.service.microservices.values().all(|ms| ms.is_maintenance())
                                && microservices_len == self.shard_count as usize
                            {
                                self.service.update_status(ServiceStatus::Maintenance);
                            } else if self.service.microservices.values().all(|ms| ms.is_running())
                                && microservices_len == self.shard_count as usize
                            {
                                // all shards are connected/running as expected
                                self.service.update_status(ServiceStatus::Running);
                            } else {
                                // degraded service, probably one of the shard is in Maintenance or degraded
                                self.service.update_status(ServiceStatus::Degraded);
                            }
                        }
                        let event = ClusterEvent::Service(self.service.clone());
                        supervisor.send(event).ok();
                    }
                    NodeEvent::Shutdown => {
                        self.handle = None;
                        self.service.update_status(ServiceStatus::Stopping);
                        // shutdown children (stages)
                        for (_, stage) in self.stages.drain() {
                            let event = StageEvent::Shutdown;
                            let _ = stage.send(event);
                        }
                        let event = ClusterEvent::Service(self.service.clone());
                        supervisor.send(event).ok();
                        // contract design:
                        // the node supervisor will only shutdown when stages drop node_txs(supervisor)
                        // and this will only happen if reporters dropped stage_txs,
                        // still reporters_txs have to go out of the scope (from ring&stages).
                    }
                }
            }
            Ok(())
        } else {
            Err(Need::Abort)
        }
    }
}
