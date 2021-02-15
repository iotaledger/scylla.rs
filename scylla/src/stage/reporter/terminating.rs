// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[async_trait::async_trait]
impl Terminating<StageHandle> for Reporter {
    async fn terminating(
        &mut self,
        _status: Result<(), Need>,
        _supervisor: &mut Option<StageHandle>,
    ) -> Result<(), Need> {
        self.force_consistency();
        warn!(
            "reporter_id: {} of shard_id: {} in node: {}, gracefully shutting down.",
            self.reporter_id, self.shard_id, &self.address
        );
        _status
    }
}
