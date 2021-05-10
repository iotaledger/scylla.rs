// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[async_trait::async_trait]
impl Init<ClusterEvent, ClusterHandle> for Node {
    async fn init(&mut self, supervisor: &mut ClusterHandle) -> Result<(), <Self as ActorTypes>::Error> {
        // spawn stages
        for shard_id in 0..self.shard_count {
            let mut stage = StageBuilder::new()
                .address(self.address.clone())
                .shard_id(shard_id)
                .reporter_count(self.reporter_count)
                .buffer_size(self.buffer_size)
                .recv_buffer_size(self.recv_buffer_size)
                .send_buffer_size(self.send_buffer_size)
                .authenticator(self.authenticator.clone())
                .build(self.service.spawn(format!("Stage_{}", shard_id)));
            let stage_handle = stage.handle().clone();
            self.stages.insert(shard_id, stage_handle);
            tokio::spawn(stage.start(self.handle.clone()));
        }
        Ok(())
    }
}
