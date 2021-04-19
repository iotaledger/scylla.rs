// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[async_trait::async_trait]
impl Init<ReportersHandles> for Receiver {
    async fn init(&mut self, status: Result<(), Need>, _supervisor: &mut Option<ReportersHandles>) -> Result<(), Need> {
        status
    }
}
