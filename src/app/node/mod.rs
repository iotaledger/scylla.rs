// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::stage::Stage;
use async_trait::async_trait;
use backstage::core::{
    Actor,
    ActorResult,
    Rt,
    ScopeId,
    Service,
    ServiceStatus,
    StreamExt,
    SupHandle,
    UnboundedChannel,
};
use std::net::SocketAddr;

/// Node event enum.
#[backstage::core::supervise]
pub enum NodeEvent {
    #[report]
    #[eol]
    /// To keep the node with up to date stage(s) service
    Microservice(ScopeId, Service),
    #[shutdown]
    /// Shutdown signal.
    Shutdown,
}

/// Node state
pub struct Node {
    address: SocketAddr,
    shard_count: usize,
}

impl Node {
    /// Create new Node with the provided socket address
    pub fn new(address: SocketAddr, shard_count: usize) -> Self {
        Self { address, shard_count }
    }
}

/// The Node actor lifecycle implementation
#[async_trait]
impl<S> Actor<S> for Node
where
    S: SupHandle<Self>,
{
    type Data = ();
    type Channel = UnboundedChannel<NodeEvent>;
    async fn init(&mut self, rt: &mut Rt<Self, S>) -> ActorResult<Self::Data> {
        // start stages in sync
        for shard_id in 0..self.shard_count {
            let stage = Stage::new(self.address, shard_id, self.shard_count);
            rt.start(format!("stage_{}", shard_id), stage).await?;
        }
        Ok(())
    }
    async fn run(&mut self, rt: &mut Rt<Self, S>, _: Self::Data) -> ActorResult<Self::Data> {
        log::info!("{} Node is {}", self.address, rt.service().status());
        while let Some(event) = rt.inbox_mut().next().await {
            match event {
                NodeEvent::Microservice(scope_id, service) => {
                    if service.is_stopped() {
                        rt.remove_microservice(scope_id);
                    } else {
                        rt.upsert_microservice(scope_id, service);
                    }
                    if !rt.service().is_stopping() {
                        if rt.microservices_all(|stage| stage.is_running()) {
                            if !rt.service().is_running() {
                                log::info!("{} Node is Running", self.address);
                            }
                            rt.update_status(ServiceStatus::Running).await;
                        } else if rt.microservices_all(|stage| stage.is_maintenance()) {
                            if !rt.service().is_maintenance() {
                                log::info!("{} Node is Maintenance", self.address);
                            }
                            rt.update_status(ServiceStatus::Maintenance).await;
                        } else {
                            if !rt.service().is_degraded() {
                                log::info!("{} Node is Degraded", self.address);
                            }
                            rt.update_status(ServiceStatus::Degraded).await;
                        }
                    } else {
                        if !rt.service().is_stopping() {
                            log::info!("{} Node is Stopping", self.address);
                        }
                        rt.update_status(ServiceStatus::Stopping).await;
                        if rt.microservices_stopped() {
                            rt.inbox_mut().close();
                        }
                    }
                }
                NodeEvent::Shutdown => {
                    rt.stop().await;
                    if rt.microservices_stopped() {
                        rt.inbox_mut().close();
                    }
                }
            }
        }
        Ok(())
    }
}
