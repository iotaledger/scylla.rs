// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[async_trait::async_trait]
impl<E, S> Init<E, S> for Scylla
where
    S: 'static + Send + EventHandle<E>,
{
    async fn init(&mut self, supervisor: &mut S) -> Result<(), <Self as ActorTypes>::Error> {
        let listener = self
            .listener_data
            .builder
            .clone()
            .build(self.service.spawn(self.listener_data.name.clone()));
        // add listener handle to the scylla application and build its state
        // create cluster
        let mut cluster = self
            .cluster_data
            .builder
            .clone()
            .build(self.service.spawn(self.cluster_data.name.clone()));
        // clone cluster handle
        self.cluster_data.event_handle = Some(cluster.handle().clone());
        let mut handle = self.handle.clone();
        handle.caller = Caller::Other;
        // start listener in abortable mode
        self.listener_data.join_handle = Some(tokio::spawn(listener.start(handle.clone())));
        // start cluster
        self.cluster_data.join_handle = Some(tokio::spawn(cluster.start(handle)));
        Ok(())
    }
}
