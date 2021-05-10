// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[async_trait::async_trait]
impl Init<ScyllaEvent, ScyllaHandle> for Listener {
    async fn init(&mut self, supervisor: &mut ScyllaHandle) -> Result<(), <Self as ActorTypes>::Error> {
        Ok(())
    }
}
