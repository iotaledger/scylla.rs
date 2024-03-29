// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[async_trait::async_trait]
impl<H: ScyllaScope> EventLoop<ScyllaHandle<H>> for Websocket {
    async fn event_loop(
        &mut self,
        status: Result<(), Need>,
        supervisor: &mut Option<ScyllaHandle<H>>,
    ) -> Result<(), Need> {
        // exit the websocket event_loop if status is Err
        status?;
        // socket running
        if let Some(supervisor) = supervisor.as_mut() {
            self.service.update_status(ServiceStatus::Running);
            let event = ScyllaEvent::Children(ScyllaChild::Websocket(self.service.clone(), None));
            supervisor.send(event).map_err(|_| Need::Abort)?;
            while let Some(Ok(msg)) = self.ws_rx.next().await {
                match msg {
                    Message::Text(msg_txt) => {
                        let apps_events: H::AppsEvents = serde_json::from_str(&msg_txt).map_err(|_| Need::Abort)?;
                        let event = ScyllaEvent::Passthrough(apps_events);
                        supervisor.send(event).map_err(|_| Need::Abort)?;
                    }
                    Message::Close(_) => {
                        self.service.update_status(ServiceStatus::Stopping);
                        let event = ScyllaEvent::Children(ScyllaChild::Websocket(self.service.clone(), None));
                        supervisor.send(event).map_err(|_| Need::Abort)?;
                    }
                    _ => {}
                }
            }
            Ok(())
        } else {
            Err(Need::Abort)
        }
    }
}
