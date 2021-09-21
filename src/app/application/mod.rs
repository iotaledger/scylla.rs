// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0
use super::cluster::Cluster;
pub(crate) use crate::cql::PasswordAuth;
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
use serde::{
    Deserialize,
    Serialize,
};

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
/// Application state
pub struct Scylla {
    /// The local data center from the scylla driver perspective
    pub local_dc: String,
    /// The thread count (workers threads in tokio term)
    pub thread_count: usize,
    /// Reporter count per stage
    pub reporter_count: u8,
    /// Optional buffer size used by the stage's connection
    pub buffer_size: Option<usize>,
    /// Optional recv buffer size size used by the stage's connection
    pub recv_buffer_size: Option<u32>,
    /// Optional send buffer size size used by the stage's connection
    pub send_buffer_size: Option<u32>,
    /// Default cql authentication
    pub authenticator: PasswordAuth,
}

impl Default for Scylla {
    fn default() -> Self {
        Self {
            local_dc: "datacenter1".to_string(),
            thread_count: num_cpus::get(),
            reporter_count: 2,
            buffer_size: None,
            recv_buffer_size: None,
            send_buffer_size: None,
            authenticator: PasswordAuth::default(),
        }
    }
}

impl Scylla {
    /// Create new Scylla instance
    pub fn new<T: Into<String>>(
        local_datacenter: T,
        thread_count: usize,
        reporter_count: u8,
        password_auth: PasswordAuth,
    ) -> Self {
        Self {
            local_dc: local_datacenter.into(),
            thread_count,
            reporter_count,
            buffer_size: None,
            recv_buffer_size: None,
            send_buffer_size: None,
            authenticator: password_auth,
        }
    }
}

/// Event type of the Scylla Application
#[backstage::core::supervise]
pub enum ScyllaEvent {
    #[report]
    #[eol]
    /// Used by scylla children to push their service
    Microservice(ScopeId, Service),
    /// Shutdown signal
    #[shutdown]
    Shutdown,
}

/// The Scylla actor lifecycle implementation
#[async_trait]
impl<S> Actor<S> for Scylla
where
    S: SupHandle<Self>,
{
    type Data = ();
    type Channel = UnboundedChannel<ScyllaEvent>;
    async fn init(&mut self, rt: &mut Rt<Self, S>) -> ActorResult<Self::Data> {
        // publish scylla as config
        rt.add_resource(self.clone()).await;
        let cluster = Cluster::new();
        rt.start("cluster".to_string(), cluster).await?;
        Ok(())
    }
    async fn run(&mut self, rt: &mut Rt<Self, S>, _data: Self::Data) -> ActorResult<()> {
        while let Some(event) = rt.inbox_mut().next().await {
            match event {
                ScyllaEvent::Microservice(scope_id, service) => {
                    if service.is_stopped() {
                        rt.remove_microservice(scope_id);
                    } else {
                        rt.upsert_microservice(scope_id, service);
                    }
                    if !rt.service().is_stopping() {
                        if rt.microservices_all(|node| node.is_running()) {
                            rt.update_status(ServiceStatus::Running).await;
                        } else if rt.microservices_all(|node| node.is_maintenance()) {
                            rt.update_status(ServiceStatus::Maintenance).await;
                        } else {
                            rt.update_status(ServiceStatus::Degraded).await;
                        }
                    } else {
                        rt.update_status(ServiceStatus::Stopping).await;
                        if rt.microservices_stopped() {
                            rt.inbox_mut().close();
                        }
                    }
                }
                ScyllaEvent::Shutdown => {
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
