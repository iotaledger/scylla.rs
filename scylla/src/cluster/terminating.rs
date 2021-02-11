// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[async_trait::async_trait]
impl<H: ScyllaScope> Terminating<ScyllaHandle<H>> for Cluster {
    async fn terminating(
        &mut self,
        _status: Result<(), Need>,
        _supervisor: &mut Option<ScyllaHandle<H>>,
    ) -> Result<(), Need> {
        self.service.update_status(ServiceStatus::Stopping);
        let event = ScyllaEvent::Children(ScyllaChild::Cluster(self.service.clone()));
        let _ = _supervisor.as_mut().unwrap().send(event);
        _status
    }
}
