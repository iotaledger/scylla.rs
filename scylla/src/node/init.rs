// Copyright 2020 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[async_trait::async_trait]
impl Init<ClusterHandle> for Node {
    async fn init(&mut self, status: Result<(), Need>, supervisor: &mut Option<ClusterHandle>) -> Result<(), Need> {
        self.service.update_status(ServiceStatus::Initializing);
        // let event = ScyllaEvent::Children(ScyllaChild::Cluster(self.service.clone(), None));
        // let _ = supervisor.as_mut().unwrap().send(event);
        status
    }
}
