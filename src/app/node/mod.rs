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
    stages: HashMap<u16, Act<Stage>>,
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
        stages: HashMap::new(),
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
    type Channel = TokioChannel<Self::Event>;

    async fn run<'a, Reg: RegistryAccess + Send + Sync, Sup: EventDriven>(
        &mut self,
        rt: &mut ActorScopedRuntime<'a, Self, Reg, Sup>,
        mut cluster: Self::Dependencies,
    ) -> Result<(), ActorError>
    where
        Self: Sized,
        Sup::Event: SupervisorEvent,
        <Sup::Event as SupervisorEvent>::Children: From<PhantomData<Self>>,
    {
        rt.update_status(ServiceStatus::Initializing).await.ok();
        let my_handle = rt.my_handle().await;
        let mut reporter_pools = HashMap::new();
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
            let (_, stage_handle) = rt.spawn_into_pool(stage, my_handle.clone()).await;
            self.stages.insert(shard_id, stage_handle);
        }
        rt.update_status(ServiceStatus::Running).await.ok();
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
                        cluster.send(event).await.ok();
                    }
                }
                NodeEvent::ReportExit(res) => match res {
                    Ok(_) => break,
                    Err(e) => match e.error.request() {
                        ActorRequest::Restart => {
                            rt.spawn_into_pool(e.state, my_handle.clone()).await;
                        }
                        ActorRequest::Reschedule(dur) => {
                            let mut handle_clone = my_handle.clone();
                            let evt = Self::Event::report_err(ErrorReport::new(
                                e.state,
                                e.service,
                                ActorError::RuntimeError(ActorRequest::Restart),
                            ));
                            let dur = *dur;
                            tokio::spawn(async move {
                                tokio::time::sleep(dur).await;
                                handle_clone.send(evt).await.ok();
                            });
                        }
                        ActorRequest::Finish => {
                            error!("{}", e.error);
                            break;
                        }
                        ActorRequest::Panic => panic!("{}", e.error),
                    },
                },
                NodeEvent::StatusChange(_) => (),
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
    /// Register the stage reporters.
    RegisterReporters(u16, Pool<Reporter, ReporterId>),
    Shutdown,
}
