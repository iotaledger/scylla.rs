// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[async_trait::async_trait]
impl Terminating<ReportersHandles> for Sender {
    async fn terminating(
        &mut self,
        _status: Result<(), Need>,
        _supervisor: &mut Option<ReportersHandles>,
    ) -> Result<(), Need> {
        // Ensure socket is closed
        let _ = self.socket.shutdown().await;
        _status
    }
}
