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
        while let Some(event) = self.inbox.rx.recv().await {
            match event {
                NodeEvent::RegisterReporters(microservice, reporters_handles) => {
                    let shard_id: u16 = microservice.get_name().parse().unwrap();
                    let mut socket_addr = self.address.clone();
                    self.service.update_microservice(microservice.get_name(), microservice);
                    // assign shard_id to socket_addr as it's going be used later as key in registry
                    socket_addr.set_port(shard_id);
                    let reporters_handles_ref = self.reporters_handles.as_mut().unwrap();
                    reporters_handles_ref.insert(socket_addr, reporters_handles);
                    // check if we pushed all reporters of the node.
                    if reporters_handles_ref.len() == self.shard_count as usize {
                        // reporters_handles should be passed to cluster supervisor
                        let event = ClusterEvent::RegisterReporters(
                            self.service.clone(),
                            self.reporters_handles.take().unwrap(),
                        );
                        let _ = supervisor.as_ref().unwrap().send(event);
                    } else {
                        let event = ClusterEvent::Service(self.service.clone());
                        let _ = supervisor.as_ref().unwrap().send(event);
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
                    let _ = supervisor.as_ref().unwrap().send(event);
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
                    let _ = supervisor.as_mut().unwrap().send(event);
                    // contract design:
                    // the node supervisor will only shutdown when stages drop node_txs(supervisor)
                    // and this will only happen if reporters dropped stage_txs,
                    // still reporters_txs have to go out of the scope (from ring&stages).
                }
            }
        }
        Ok(())
    }
}
