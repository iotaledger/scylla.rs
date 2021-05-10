// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[async_trait::async_trait]
impl Shutdown<ScyllaEvent, ScyllaHandle> for Cluster {
    async fn shutdown(
        &mut self,
        status: Result<(), Self::Error>,
        supervisor: &mut ScyllaHandle,
    ) -> Result<ActorRequest, ActorError> {
        Ok(ActorRequest::Finish)
    }
}
