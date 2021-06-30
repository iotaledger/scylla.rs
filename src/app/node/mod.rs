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
    address: SocketAddr,
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

    async fn run<'a, Reg: RegistryAccess + Send + Sync>(
        &mut self,
        rt: &mut ActorScopedRuntime<'a, Self, Reg>,
        mut cluster: Self::Dependencies,
    ) -> Result<(), ActorError>
    where
        Self: Sized,
    {
        rt.update_status(ServiceStatus::Initializing).await;
        let my_handle = rt.my_handle().await;
        let mut reporter_pools = Some(HashMap::new());
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
        rt.update_status(ServiceStatus::Running).await;
        while let Some(event) = rt.next_event().await {
            match event {
                NodeEvent::RegisterReporters(shard_id, reporter_pool) => {
                    let mut socket_addr = self.address.clone();
                    // assign shard_id to socket_addr as it's going be used later as key in registry
                    socket_addr.set_port(shard_id);
                    if let Some(pools) = reporter_pools.as_mut() {
                        pools.insert(socket_addr, reporter_pool);
                        if pools.len() == self.shard_count as usize {
                            info!("Sending register reporters event to cluster!");
                            let event = ClusterEvent::RegisterReporters(reporter_pools.take().unwrap());
                            cluster.send(event).await.ok();
                        }
                    } else {
                        error!("Tried to register reporters more than once!")
                    }
                }
                NodeEvent::Report(res) => match res {
                    Ok(s) => break,
                    Err(e) => match e.error.request() {
                        ActorRequest::Restart => {
                            rt.spawn_into_pool(e.state, my_handle.clone()).await;
                        }
                        ActorRequest::Reschedule(dur) => {
                            let mut handle_clone = my_handle.clone();
                            let evt = NodeEvent::report_err(ErrorReport::new(
                                e.state,
                                e.service,
                                ActorError::RuntimeError(ActorRequest::Restart),
                            ))
                            .unwrap();
                            let dur = *dur;
                            tokio::spawn(async move {
                                tokio::time::sleep(dur).await;
                                handle_clone.send(evt).await;
                            });
                        }
                        ActorRequest::Finish => {
                            log::error!("{}", e.error);
                            break;
                        }
                        ActorRequest::Panic => panic!("{}", e.error),
                    },
                },
                NodeEvent::Status(_) => (),
                NodeEvent::Shutdown => break,
            }
        }
        rt.update_status(ServiceStatus::Stopped).await;
        Ok(())
    }
}

/// Node event enum.
pub enum NodeEvent {
    /// Register the stage reporters.
    RegisterReporters(u16, Pool<Reporter, ReporterId>),
    Report(Result<SuccessReport<Stage>, ErrorReport<Stage>>),
    Status(Service),
    Shutdown,
}

impl SupervisorEvent<Stage> for NodeEvent {
    fn report(res: Result<SuccessReport<Stage>, ErrorReport<Stage>>) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Ok(Self::Report(res))
    }

    fn status(service: Service) -> Self {
        Self::Status(service)
    }
}
