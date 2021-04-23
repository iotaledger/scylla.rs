// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[async_trait::async_trait]
impl Init<ReportersHandles> for Sender {
    async fn init(&mut self, status: Result<(), Need>, supervisor: &mut Option<ReportersHandles>) -> Result<(), Need> {
        self.service.update_status(ServiceStatus::Initializing);
        if let (Some(my_handle), Some(reporter_handles)) = (self.handle.take(), supervisor.as_ref()) {
            for reporter_handle in reporter_handles.values() {
                let event = ReporterEvent::Session(Session::New(self.service.clone(), my_handle.clone()));
                let _ = reporter_handle.send(event);
            }
            status
        } else {
            Err(Need::Abort)
        }
    }
}
