// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[async_trait::async_trait]
impl Shutdown<ReporterEvent, ReportersHandles> for Receiver {
    async fn shutdown(
        &mut self,
        status: Result<(), Self::Error>,
        supervisor: &mut ReportersHandles,
    ) -> Result<ActorRequest, ActorError> {
        Ok(ActorRequest::Finish)
    }
}
