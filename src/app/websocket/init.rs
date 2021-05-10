// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[async_trait::async_trait]
impl Init<ScyllaEvent, ScyllaHandle> for Websocket {
    async fn init(&mut self, supervisor: &mut ScyllaHandle) -> Result<(), <Self as ActorTypes>::Error> {
        supervisor
            .send(ScyllaEvent::Children(ScyllaChild::Websocket(
                self.service.clone(),
                self.opt_ws_tx.take(),
            )))
            .map_err(|_| anyhow!("Failed to send websocket sink to scylla!"))?;
        Ok(())
    }
}
