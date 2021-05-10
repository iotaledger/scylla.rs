// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[async_trait::async_trait]
impl Shutdown<ScyllaEvent, ScyllaHandle> for Websocket {
    async fn shutdown(
        &mut self,
        status: Result<(), Self::Error>,
        supervisor: &mut ScyllaHandle,
    ) -> Result<ActorRequest, ActorError> {
        if let Err(e) = status {
            error!("{}", e);
        }
        Ok(ActorRequest::Finish)
    }
}
