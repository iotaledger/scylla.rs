// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[async_trait::async_trait]
impl Init<StageHandle> for Reporter {
    async fn init(&mut self, status: Result<(), Need>, supervisor: &mut Option<StageHandle>) -> Result<(), Need> {
        self.service.update_status(ServiceStatus::Initializing);
        let event = StageEvent::Reporter(self.service.clone());
        if let Some(supervisor) = supervisor.as_mut() {
            supervisor.send(event).ok();
            status
        } else {
            Err(Need::Abort)
        }
    }
}
