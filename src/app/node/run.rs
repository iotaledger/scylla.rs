// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[async_trait::async_trait]
impl Run<ClusterEvent, ClusterHandle> for Node {
    async fn run(&mut self, supervisor: &mut ClusterHandle) -> Result<(), Self::Error> {
        while let Some(event) = self.inbox.recv().await {
            match event {
                NodeEvent::RegisterReporters(microservice, reporters_handles) => {
                    if let Some(shard_id) = microservice
                        .name
                        .split("_")
                        .nth(1)
                        .and_then(|shard| shard.parse::<u16>().ok())
                    {
                        let mut socket_addr = self.address.clone();
                        self.service.update_microservice(microservice);
                        // assign shard_id to socket_addr as it's going be used later as key in registry
                        socket_addr.set_port(shard_id);
                        if let Some(reporters_handles_ref) = self.reporters_handles.as_mut() {
                            reporters_handles_ref.insert(socket_addr, reporters_handles);
                            // check if we pushed all reporters of the node.
                            if reporters_handles_ref.len() == self.shard_count as usize {
                                // reporters_handles should be passed to cluster supervisor
                                if let Some(reporters_handles) = self.reporters_handles.take() {
                                    info!("Sending register reporters event to cluster!");
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
                    self.service.update_microservice(microservice);
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
                    // shutdown children (stages)
                    for (_, mut stage) in self.stages.drain() {
                        let event = StageEvent::Shutdown;
                        let _ = stage.send(event);
                    }
                    let event = ClusterEvent::Service(self.service.clone());
                    supervisor.send(event).ok();
                    // contract design:
                    // the node supervisor will only shutdown when stages drop node_txs(supervisor)
                    // and this will only happen if reporters dropped stage_txs,
                    // still reporters_txs have to go out of the scope (from ring&stages).
                    break;
                }
            }
        }
        Ok(())
    }
}
