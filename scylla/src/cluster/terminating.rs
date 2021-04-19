// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[async_trait::async_trait]
impl<H: ScyllaScope> Terminating<ScyllaHandle<H>> for Cluster {
    async fn terminating(
        &mut self,
        status: Result<(), Need>,
        supervisor: &mut Option<ScyllaHandle<H>>,
    ) -> Result<(), Need> {
        self.service.update_status(ServiceStatus::Stopping);
        let event = ScyllaEvent::Children(ScyllaChild::Cluster(self.service.clone()));
        if let Some(supervisor) = supervisor.as_mut() {
            supervisor.send(event).ok();
            status
        } else {
            Err(Need::Abort)
        }
    }
}
