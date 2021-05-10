// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[async_trait::async_trait]
impl Init<ScyllaEvent, ScyllaHandle> for Cluster {
    async fn init(&mut self, supervisor: &mut ScyllaHandle) -> Result<(), <Self as ActorTypes>::Error> {
        self.service.update_status(ServiceStatus::Initializing);
        let event = ScyllaEvent::Children(ScyllaChild::Cluster(self.service.clone()));
        supervisor.send(event).ok();
        Ok(())
    }
}
