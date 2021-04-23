// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[async_trait::async_trait]
impl Init<NodeHandle> for Stage {
    async fn init(&mut self, status: Result<(), Need>, supervisor: &mut Option<NodeHandle>) -> Result<(), Need> {
        if let Some(supervisor) = supervisor.as_mut() {
            // init Reusable payloads holder to enable reporter/sender/receiver
            // to reuse the payload whenever is possible.
            let last_range = self.appends_num * (self.reporter_count as i16);
            {
                if let Some(payloads) = Arc::get_mut(&mut self.payloads) {
                    for _ in 0..last_range {
                        payloads.push(Reusable::default())
                    }
                } else {
                    error!("Cannot acquire access to reusable payloads!");
                    return Err(Need::Abort);
                }
            }
            let streams: Vec<i16> = (0..last_range).collect();
            let mut streams_iter = streams.chunks_exact(self.appends_num as usize);
            if let Some(reporter_handles) = self.reporters_handles.as_mut() {
                // Start reporters
                for reporter_id in 0..self.reporter_count {
                    if let Some(streams) = streams_iter.next() {
                        // build reporter
                        let reporter = ReporterBuilder::new()
                            .session_id(self.session_id)
                            .reporter_id(reporter_id)
                            .shard_id(self.shard_id)
                            .address(self.address.clone())
                            .payloads(self.payloads.clone())
                            .streams(streams.to_owned().into_iter().collect())
                            .build();
                        // clone reporter_handle
                        if let Some(reporter_handle) = reporter.clone_handle() {
                            // Add reporter to reporters map

                            reporter_handles.insert(reporter_id, reporter_handle);

                            // Start reporter
                            tokio::spawn(reporter.start(self.handle.clone()));
                        } else {
                            error!("No reporter handle found!");
                            return Err(Need::Abort);
                        }
                    } else {
                        error!("Failed to create streams!");
                        return Err(Need::Abort);
                    }
                }
                self.service.update_status(ServiceStatus::Initializing);
                let event = NodeEvent::RegisterReporters(self.service.clone(), reporter_handles.clone());
                supervisor.send(event).ok();
                status
            } else {
                error!("No reporter handles container available!");
                return Err(Need::Abort);
            }
        } else {
            Err(Need::Abort)
        }
    }
}
