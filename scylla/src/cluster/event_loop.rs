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
        self.service.update_status(ServiceStatus::Running);
        let event = ScyllaEvent::Children(ScyllaChild::Cluster(self.service.clone(), None));
        let my_sup = supervisor.as_mut().unwrap();
        let _ = my_sup.send(event);
        // TODO event loop
        while let Some(event) = self.inbox.rx.recv().await {
            match event {
                ClusterEvent::SpawnNode(address) => {}
                ClusterEvent::ShutDownNode(address) => {}
                ClusterEvent::TryBuild(uniform_rf) => {}
                ClusterEvent::Shutdown => {}
            }
        }
        Ok(())
    }
}
