// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[async_trait::async_trait]
impl<E, S> Shutdown<E, S> for Scylla
where
    S: 'static + Send + EventHandle<E>,
{
    async fn shutdown(
        &mut self,
        status: Result<(), Self::Error>,
        supervisor: &mut S,
    ) -> Result<ActorRequest, ActorError> {
        info!("Shutting down Scylla!");
        Ok(ActorRequest::Finish)
    }
}
