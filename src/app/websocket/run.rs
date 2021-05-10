// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[async_trait::async_trait]
impl Run<ScyllaEvent, ScyllaHandle> for Websocket {
    async fn run(&mut self, supervisor: &mut ScyllaHandle) -> Result<(), Self::Error> {
        while let Some(Ok(msg)) = self.ws_rx.next().await {
            info!("Websocket received message!");
            match msg {
                Message::Text(msg_txt) => {
                    if let Ok(event) = serde_json::from_str(&msg_txt) {
                        let event = ScyllaEvent::Websocket(event);
                        supervisor
                            .send(event)
                            .map_err(|_| anyhow!("Failed to report text event!"))?;
                    } else {
                        error!("Received a malformatted message!");
                    }
                }
                Message::Close(_) => {
                    break;
                }
                _ => {}
            }
        }
        Ok(())
    }
}
