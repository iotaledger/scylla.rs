// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[async_trait::async_trait]
impl Init<NodeHandle> for Stage {
    async fn init(&mut self, status: Result<(), Need>, supervisor: &mut Option<NodeHandle>) -> Result<(), Need> {
        // init Reusable payloads holder to enable reporter/sender/receiver
        // to reuse the payload whenever is possible.
        let last_range = self.appends_num * (self.reporter_count as i16);
        {
            let payloads = Arc::get_mut(&mut self.payloads).unwrap();
            for _ in 0..last_range {
                payloads.push(Reusable::default())
            }
        }
        let streams: Vec<i16> = (0..last_range).collect();
        let mut streams_iter = streams.chunks_exact(self.appends_num as usize);
        // Start reporters
        for reporter_id in 0..self.reporter_count {
            // build reporter
            let reporter = ReporterBuilder::new()
                .session_id(self.session_id)
                .reporter_id(reporter_id)
                .shard_id(self.shard_id)
                .address(self.address.clone())
                .payloads(self.payloads.clone())
                .streams(streams_iter.next().unwrap().into())
                .build();
            // clone reporter_handle
            let reporter_handle = reporter.clone_handle();
            // Add reporter to reporters map
            self.reporters_handles
                .as_mut()
                .unwrap()
                .insert(reporter_id, reporter_handle);
            // Start reporter
            tokio::spawn(reporter.start(self.handle.clone()));
        }
        self.service.update_status(ServiceStatus::Initializing);
        let event = NodeEvent::RegisterReporters(self.service.clone(), self.reporters_handles.clone().unwrap());
        let _ = supervisor.as_mut().unwrap().send(event);
        status
    }
}
