// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[async_trait::async_trait]
impl Init<StageEvent, StageHandle> for Reporter {
    async fn init(&mut self, supervisor: &mut StageHandle) -> Result<(), <Self as ActorTypes>::Error> {
        Ok(())
    }
}
