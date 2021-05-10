// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[async_trait::async_trait]
impl Init<ReporterEvent, ReportersHandles> for Receiver {
    async fn init(&mut self, supervisor: &mut ReportersHandles) -> Result<(), <Self as ActorTypes>::Error> {
        Ok(())
    }
}
