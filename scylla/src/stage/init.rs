// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[async_trait::async_trait]
impl Init<NodeHandle> for Stage {
    async fn init(&mut self, status: Result<(), Need>, supervisor: &mut Option<NodeHandle>) -> Result<(), Need> {
        // Prepare range to later create stream_ids vector per reporter
        let (mut start_range, appends_num): (i16, i16) = (0, 32767 / (self.reporter_count as i16));
        // init Reusable payloads holder to enable reporter/sender/receiver
        // to reuse the payload whenever is possible.
        {
            let last_range = appends_num * (self.reporter_count as i16);
            let payloads = Arc::get_mut(&mut self.payloads).unwrap();
            for _ in 0..last_range {
                payloads.push(Reusable::default())
            }
        }
        // Start reporters
        for reporter_num in 0..self.reporter_count {
            // build reporter

            // clone reporter_handle

            // Add reporter to reporters map

            // Start reporter
        }
        self.service.update_status(ServiceStatus::Initializing);
        let event = NodeEvent::RegisterReporters(self.service.clone(), self.reporters_handles.clone().unwrap());
        let _ = supervisor.as_mut().unwrap().send(event);
        status
    }
}
