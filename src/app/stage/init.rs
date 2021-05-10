// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[async_trait::async_trait]
impl Init<NodeEvent, NodeHandle> for Stage {
    async fn init(&mut self, supervisor: &mut NodeHandle) -> Result<(), <Self as ActorTypes>::Error> {
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
                return Err(StageError::NoReusablePayloads);
            }
        }
        let streams: Vec<i16> = (0..last_range).collect();
        let mut streams_iter = streams.chunks_exact(self.appends_num as usize);

        for reporter_id in 0..self.reporter_count {
            if let Some(streams) = streams_iter.next() {
                // build reporter
                let mut reporter = ReporterBuilder::new()
                    .session_id(self.session_id)
                    .reporter_id(reporter_id)
                    .shard_id(self.shard_id)
                    .address(self.address.clone())
                    .payloads(self.payloads.clone())
                    .streams(streams.to_owned().into_iter().collect())
                    .build(self.service.spawn(format!("Reporter_{}", reporter_id)));
                // clone reporter_handle
                let reporter_handle = reporter.handle().clone();
                self.reporters_handles.insert(reporter_id, reporter_handle);
                // Start reporter
                tokio::spawn(reporter.start(self.handle.clone()));
            } else {
                error!("Failed to create streams!");
                return Err(StageError::CannotCreateStreams);
            }
        }
        info!("Sending register reporters event to node!");
        let event = NodeEvent::RegisterReporters(self.service.clone(), self.reporters_handles.clone());
        supervisor.send(event).ok();
        Ok(())
    }
}
