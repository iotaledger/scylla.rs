// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::{
    cluster::{Cluster, ClusterEvent},
    stage::{Reporter, ReporterId, Stage},
    *,
};
use std::{collections::HashMap, net::SocketAddr};

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
    type Dependencies = Act<Cluster>;
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
        cluster: Self::Dependencies,
    ) -> Result<(), ActorError>
    where
        Self: Sized,
        Sup::Event: SupervisorEvent,
        <Sup::Event as SupervisorEvent>::Children: From<PhantomData<Self>>,
    {
        rt.update_status(ServiceStatus::Running).await.ok();
        let mut reporter_pools = HashMap::new();
        let my_handle = rt.handle();
        while let Some(event) = rt.next_event().await {
            match event {
                NodeEvent::RegisterReporters(shard_id, reporter_pool) => {
                    let mut socket_addr = self.address;
                    // assign shard_id to socket_addr as it's going be used later as key in registry
                    socket_addr.set_port(shard_id);
                    reporter_pools.insert(socket_addr, reporter_pool);
                    if reporter_pools.len() == self.shard_count as usize {
                        debug!("Sending register reporters event to cluster!");
                        let event = ClusterEvent::RegisterReporters(reporter_pools.clone());
                        cluster.send(event).ok();
                    }
                }
                NodeEvent::ReportExit(res) => match res {
                    Ok(_) => break,
                    Err(e) => match e.error.request() {
                        ActorRequest::Restart => {
                            warn!("Respawning stage after error: {}", e.error);
                            rt.print_root().await;
                            rt.spawn_into_pool_keyed::<MapPool<_, u16>>(e.state.shard_id, e.state)
                                .await?;
                        }
                        ActorRequest::Reschedule(dur) => {
                            warn!("Respawning stage after {} ms", dur.as_millis());
                            let handle_clone = my_handle.clone();
                            let evt = Self::Event::report_err(ErrorReport::new(
                                e.state,
                                e.service,
                                ActorError::RuntimeError(ActorRequest::Restart),
                            ));
                            let dur = *dur;
                            tokio::spawn(async move {
                                tokio::time::sleep(dur).await;
                                handle_clone.send(evt).ok();
                            });
                        }
                        ActorRequest::Finish => {
                            error!("{}", e.error);
                            break;
                        }
                        ActorRequest::Panic => panic!("{}", e.error),
                    },
                },
                NodeEvent::StatusChange(_) => {
                    let service_tree = rt.service_tree().await;
                    if service_tree
                        .children
                        .iter()
                        .any(|s| s.status == ScyllaStatus::Degraded.as_str())
                    {
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
    /// Register the stage reporters
    RegisterReporters(u16, Pool<MapPool<Reporter, ReporterId>>),
    /// Shutdown this node
    Shutdown,
}
