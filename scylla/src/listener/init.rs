// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[async_trait::async_trait]
impl<H: ScyllaScope> Init<ScyllaHandle<H>> for Listener {
    async fn init(&mut self, status: Result<(), Need>, supervisor: &mut Option<ScyllaHandle<H>>) -> Result<(), Need> {
        self.service.update_status(ServiceStatus::Initializing);
        let event = ScyllaEvent::Children(ScyllaChild::Listener(self.service.clone()));
        let _ = supervisor.as_mut().unwrap().send(event);
        status
    }
}
