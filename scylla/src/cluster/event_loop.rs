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
        // TODO event loop
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
                ClusterEvent::SpawnNode(address) => {}
                ClusterEvent::ShutDownNode(address) => {}
                ClusterEvent::TryBuild(uniform_rf) => {}
                ClusterEvent::Shutdown => {}
            }
        }
        Ok(())
    }
}
