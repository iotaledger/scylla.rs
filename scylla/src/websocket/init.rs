// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[async_trait::async_trait]
impl<H: ScyllaScope> Init<ScyllaHandle<H>> for Websocket {
    async fn init(&mut self, status: Result<(), Need>, supervisor: &mut Option<ScyllaHandle<H>>) -> Result<(), Need> {
        if let Some(supervisor) = supervisor.as_mut() {
            // todo authenticator using static secret key and noise protocol
            self.service.update_status(ServiceStatus::Initializing);
            let event = ScyllaEvent::Children(ScyllaChild::Websocket(self.service.clone(), self.opt_ws_tx.take()));
            supervisor.send(event).ok();
            status
        } else {
            Err(Need::Abort)
        }
    }
}
