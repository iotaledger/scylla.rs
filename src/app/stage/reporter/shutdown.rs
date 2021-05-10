// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[async_trait::async_trait]
impl Shutdown<StageEvent, StageHandle> for Reporter {
    async fn shutdown(
        &mut self,
        status: Result<(), Self::Error>,
        supervisor: &mut StageHandle,
    ) -> Result<ActorRequest, ActorError> {
        self.force_consistency();
        warn!(
            "reporter_id: {} of shard_id: {} in node: {}, gracefully shutting down.",
            self.reporter_id, self.shard_id, &self.address
        );
        Ok(ActorRequest::Finish)
    }
}
