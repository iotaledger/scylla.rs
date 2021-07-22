// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::{stage::Stage, *};
use std::net::SocketAddr;

/// Node state
pub struct Node {
    pub(crate) address: SocketAddr,
    reporter_count: u8,
    shard_count: u16,
    buffer_size: usize,
    recv_buffer_size: Option<u32>,
    send_buffer_size: Option<u32>,
    authenticator: PasswordAuth,
}

#[build]
#[derive(Debug, Clone)]
pub fn build_node(
    address: SocketAddr,
    data_center: String,
    reporter_count: u8,
    shard_count: u16,
    buffer_size: usize,
    recv_buffer_size: Option<u32>,
    send_buffer_size: Option<u32>,
    authenticator: PasswordAuth,
) -> Node {
    Node {
        address,
        reporter_count,
        shard_count,
        buffer_size,
        recv_buffer_size,
        send_buffer_size,
        authenticator,
    }
}

#[async_trait]
impl Actor for Node {
    type Dependencies = ();
    type Event = NodeEvent;
    type Channel = UnboundedTokioChannel<Self::Event>;

    async fn init<Reg: RegistryAccess + Send + Sync, Sup: EventDriven>(
        &mut self,
        rt: &mut ActorScopedRuntime<Self, Reg, Sup>,
    ) -> Result<(), ActorError>
    where
        Self: Sized,
        Sup::Event: SupervisorEvent,
        <Sup::Event as SupervisorEvent>::Children: From<PhantomData<Self>>,
    {
        rt.update_status(ServiceStatus::Initializing).await.ok();
        // spawn stages
        for shard_id in 0..self.shard_count {
            let stage = stage::StageBuilder::new()
                .address(self.address.clone())
                .shard_id(shard_id)
                .reporter_count(self.reporter_count)
                .buffer_size(self.buffer_size)
                .recv_buffer_size(self.recv_buffer_size)
                .send_buffer_size(self.send_buffer_size)
                .authenticator(self.authenticator.clone())
                .build();
            rt.spawn_into_pool_keyed::<MapPool<_, u16>>(shard_id, stage).await?;
        }
        Ok(())
    }

    async fn run<Reg: RegistryAccess + Send + Sync, Sup: EventDriven>(
        &mut self,
        rt: &mut ActorScopedRuntime<Self, Reg, Sup>,
        _: Self::Dependencies,
    ) -> Result<(), ActorError>
    where
        Self: Sized,
        Sup::Event: SupervisorEvent,
        <Sup::Event as SupervisorEvent>::Children: From<PhantomData<Self>>,
    {
        rt.update_status(ServiceStatus::Running).await.ok();
        while let Some(event) = rt.next_event().await {
            match event {
                NodeEvent::ReportExit(res) => match res {
                    Ok(_) => break,
                    Err(e) => return Err(e.error),
                },
                NodeEvent::StatusChange(s) => {
                    if s.service.status == ScyllaStatus::Degraded.as_str() {
                        rt.update_status(ScyllaStatus::Degraded).await.ok();
                    } else {
                        rt.update_status(ServiceStatus::Running).await.ok();
                    }
                }
                NodeEvent::Shutdown => break,
            }
        }
        rt.update_status(ServiceStatus::Stopped).await.ok();
        Ok(())
    }
}

/// Node event enum.
#[supervise(Stage)]
pub enum NodeEvent {
    /// Shutdown this node
    Shutdown,
}
