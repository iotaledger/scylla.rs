// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[async_trait::async_trait]
impl Shutdown<NodeEvent, NodeHandle> for Stage {
    async fn shutdown(
        &mut self,
        status: Result<(), Self::Error>,
        supervisor: &mut NodeHandle,
    ) -> Result<ActorRequest, ActorError> {
        Ok(ActorRequest::Finish)
    }
}
