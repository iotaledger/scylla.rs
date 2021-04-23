// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[async_trait::async_trait]
impl Init<ClusterHandle> for Node {
    async fn init(&mut self, status: Result<(), Need>, supervisor: &mut Option<ClusterHandle>) -> Result<(), Need> {
        self.service.update_status(ServiceStatus::Initializing);
        let event = ClusterEvent::Service(self.service.clone());
        if let Some(supervisor) = supervisor.as_mut() {
            supervisor.send(event).ok();
            // spawn stages
            for shard_id in 0..self.shard_count {
                let stage = StageBuilder::new()
                    .address(self.address.clone())
                    .shard_id(shard_id)
                    .reporter_count(self.reporter_count)
                    .buffer_size(self.buffer_size)
                    .recv_buffer_size(self.recv_buffer_size)
                    .send_buffer_size(self.send_buffer_size)
                    .authenticator(self.authenticator.clone())
                    .build();
                if let Some(stage_handle) = stage.clone_handle() {
                    self.stages.insert(shard_id, stage_handle);
                    tokio::spawn(stage.start(self.handle.clone()));
                } else {
                    error!("No stage handle found!");
                    return Err(Need::Abort);
                }
            }

            status
        } else {
            Err(Need::Abort)
        }
    }
}
