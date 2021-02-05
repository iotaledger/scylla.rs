// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[async_trait::async_trait]
impl Terminating<ClusterHandle> for Node {
    async fn terminating(
        &mut self,
        _status: Result<(), Need>,
        _supervisor: &mut Option<ClusterHandle>,
    ) -> Result<(), Need> {
        self.service.update_status(ServiceStatus::Stopping);
        // let event = ScyllaEvent::Children(ScyllaChild::Cluster(self.service.clone(), None));
        // let _ = _supervisor.as_mut().unwrap().send(event);
        _status
    }
}
