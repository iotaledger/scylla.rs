// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[async_trait::async_trait]
impl Shutdown<ClusterEvent, ClusterHandle> for Node {
    async fn shutdown(
        &mut self,
        status: Result<(), Self::Error>,
        supervisor: &mut ClusterHandle,
    ) -> Result<ActorRequest, ActorError> {
        self.service.update_status(ServiceStatus::Stopping);
        let event = ClusterEvent::Service(self.service.clone());
        supervisor.send(event).ok();
        Ok(ActorRequest::Finish)
    }
}
