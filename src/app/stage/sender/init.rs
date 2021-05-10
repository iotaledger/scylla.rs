// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[async_trait::async_trait]
impl Init<ReporterEvent, ReportersHandles> for Sender {
    async fn init(&mut self, reporter_handles: &mut ReportersHandles) -> Result<(), <Self as ActorTypes>::Error> {
        self.service.update_status(ServiceStatus::Initializing);
        for reporter_handle in reporter_handles.values_mut() {
            let event = ReporterEvent::Session(Session::New(self.service.clone(), self.handle.clone()));
            reporter_handle.send(event).ok();
        }
        Ok(())
    }
}
