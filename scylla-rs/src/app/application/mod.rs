// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0
use super::cluster::Cluster;
pub(crate) use crate::cql::PasswordAuth;
use crate::{
    app::cluster::ClusterEvent,
    cql::compression::{
        CompressionType,
        Lz4,
        Snappy,
        Uncompressed,
    },
};
use async_trait::async_trait;
use backstage::core::{
    Actor,
    ActorResult,
    EolEvent,
    ReportEvent,
    Rt,
    ScopeId,
    Service,
    ServiceStatus,
    ShutdownEvent,
    StreamExt,
    SupHandle,
    UnboundedChannel,
    UnboundedHandle,
};
use maplit::hashmap;
use serde::{
    Deserialize,
    Serialize,
};
use std::{
    collections::{
        HashMap,
        HashSet,
    },
    net::SocketAddr,
};
/// Scylla handle
pub type ScyllaHandle = UnboundedHandle<ScyllaEvent>;
/// Type alias for datacenter names
pub type DatacenterName = String;
/// Type alias for scylla keysapce names
pub type KeyspaceName = String;

/// Configuration for a scylla datacenter
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct DatacenterConfig {
    /// The scylla replication factor for this datacenter
    pub replication_factor: u8,
}

impl Default for KeyspaceConfig {
    fn default() -> Self {
        Self {
            name: "permanode".to_string(),
            data_centers: hashmap! {
                "datacenter1".to_string() => DatacenterConfig {
                    replication_factor: 2,
                },
            },
        }
    }
}

impl std::hash::Hash for KeyspaceConfig {
    fn hash<H: std::hash::Hasher>(&self, hasher: &mut H) {
        self.name.hash(hasher)
    }
}

/// Configuration for a scylla keyspace
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct KeyspaceConfig {
    /// The name of the keyspace
    pub name: KeyspaceName,
    /// Datacenters configured for this keyspace, keyed by name
    pub data_centers: HashMap<DatacenterName, DatacenterConfig>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
/// Application state
pub struct Scylla {
    /// The local data center from the scylla driver perspective
    pub local_dc: String,
    /// The initial scylla nodes
    pub nodes: HashSet<SocketAddr>,
    /// Keyspace definition for this cluster, keyed by the network
    /// they will pull data from
    pub keyspaces: HashSet<KeyspaceConfig>,
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
    pub compression: Option<CompressionType>,
}

impl Default for Scylla {
    fn default() -> Self {
        Self {
            local_dc: "datacenter1".to_string(),
            nodes: HashSet::new(),
            keyspaces: HashSet::new(),
            reporter_count: 2,
            buffer_size: None,
            recv_buffer_size: None,
            send_buffer_size: None,
            authenticator: PasswordAuth::default(),
            compression: None,
        }
    }
}

impl Scylla {
    /// Create new Scylla instance with empty nodes and keyspaces
    pub fn new<T: Into<String>>(
        local_datacenter: T,
        reporter_count: u8,
        password_auth: PasswordAuth,
        compression: Option<CompressionType>,
    ) -> Self {
        Self {
            local_dc: local_datacenter.into(),
            nodes: HashSet::new(),
            keyspaces: HashSet::new(),
            reporter_count,
            buffer_size: None,
            recv_buffer_size: None,
            send_buffer_size: None,
            authenticator: password_auth,
            compression,
        }
    }
    pub fn insert_node(&mut self, node: SocketAddr) -> &mut Self {
        self.nodes.insert(node);
        self
    }
    pub fn remove_node(&mut self, node: &SocketAddr) -> &mut Self {
        self.nodes.remove(&node);
        self
    }
    pub fn insert_keyspace(&mut self, keyspace: KeyspaceConfig) -> &mut Self {
        self.keyspaces.insert(keyspace);
        self
    }
    pub fn remove_keyspace(&mut self, keyspace: &str) -> &mut Self {
        self.keyspaces.retain(|k| k.name != keyspace);
        self
    }
}

/// Event type of the Scylla Application
#[backstage::core::supervise]
pub enum ScyllaEvent {
    /// Request the cluster handle
    GetClusterHandle(tokio::sync::oneshot::Sender<UnboundedHandle<ClusterEvent>>),
    #[report]
    #[eol]
    /// Used by scylla children to push their service
    Microservice(ScopeId, Service),
    /// Update state
    UpdateState(Scylla),
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
    type Data = UnboundedHandle<ClusterEvent>;
    type Channel = UnboundedChannel<ScyllaEvent>;
    async fn init(&mut self, rt: &mut Rt<Self, S>) -> ActorResult<Self::Data> {
        log::info!("Scylla is {}", rt.service().status());
        // todo add scylla banner
        // publish scylla as config
        rt.add_resource(self.clone()).await;
        let cluster_handle = match &self.compression {
            Some(kind) => match kind {
                CompressionType::Lz4 => {
                    let cluster = Cluster::<Lz4>::new();
                    rt.start("cluster".to_string(), cluster).await?
                }
                CompressionType::Snappy => {
                    let cluster = Cluster::<Snappy>::new();
                    rt.start("cluster".to_string(), cluster).await?
                }
            },
            None => {
                let cluster = Cluster::<Uncompressed>::new();
                rt.start("cluster".to_string(), cluster).await?
            }
        };

        if rt.microservices_all(|ms| ms.is_idle()) {
            rt.update_status(ServiceStatus::Idle).await;
        }
        Ok(cluster_handle)
    }
    async fn run(&mut self, rt: &mut Rt<Self, S>, cluster_handle: Self::Data) -> ActorResult<()> {
        log::info!("Scylla is {}", rt.service().status());
        while let Some(event) = rt.inbox_mut().next().await {
            match event {
                ScyllaEvent::UpdateState(new_state) => {
                    *self = new_state;
                    rt.publish(self.clone()).await;
                }
                ScyllaEvent::GetClusterHandle(oneshot) => {
                    oneshot.send(cluster_handle.clone());
                }
                ScyllaEvent::Microservice(scope_id, service) => {
                    if service.is_stopped() {
                        rt.remove_microservice(scope_id);
                    } else {
                        rt.upsert_microservice(scope_id, service);
                    }
                    if !rt.service().is_stopping() {
                        if rt.microservices_all(|cluster| cluster.is_running()) {
                            if !rt.service().is_running() {
                                log::info!("Scylla is Running");
                            }
                            rt.update_status(ServiceStatus::Running).await;
                        } else if rt.microservices_all(|cluster| cluster.is_maintenance()) {
                            if !rt.service().is_maintenance() {
                                log::info!("Scylla is Maintenance");
                            }
                            rt.update_status(ServiceStatus::Maintenance).await;
                        } else if rt.microservices_all(|cluster| cluster.is_idle()) {
                            if !rt.service().is_idle() {
                                log::info!("Scylla is Idle");
                            }
                            rt.update_status(ServiceStatus::Idle).await;
                        } else if rt.microservices_all(|cluster| cluster.is_outage()) {
                            if !rt.service().is_outage() {
                                log::info!("Scylla is experiencing an Outage");
                            }
                            rt.update_status(ServiceStatus::Outage).await;
                        } else {
                            if !rt.service().is_degraded() {
                                log::info!("Scylla is Degraded");
                            }
                            rt.update_status(ServiceStatus::Degraded).await;
                        }
                    } else {
                        if !rt.service().is_stopping() {
                            log::info!("Scylla is Stopping");
                        }
                        rt.update_status(ServiceStatus::Stopping).await;
                        if rt.microservices_stopped() {
                            rt.inbox_mut().close();
                        }
                    }
                }
                ScyllaEvent::Shutdown => {
                    log::warn!("Scylla is Stopping");
                    rt.stop().await;
                    if rt.microservices_stopped() {
                        rt.inbox_mut().close();
                    }
                }
            }
        }
        log::info!("Scylla gracefully shutdown");
        Ok(())
    }
}

#[async_trait]
/// The public interface of scylla handle, it provides access to the cluster handle
pub trait ScyllaHandleExt {
    /// Get the cluster handle
    async fn cluster_handle(&self) -> Option<UnboundedHandle<ClusterEvent>>;
}

#[async_trait]
impl ScyllaHandleExt for UnboundedHandle<ScyllaEvent> {
    async fn cluster_handle(&self) -> Option<UnboundedHandle<ClusterEvent>> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        if let Ok(_) = self.send(ScyllaEvent::GetClusterHandle(tx)) {
            if let Ok(handle) = rx.await {
                Some(handle)
            } else {
                None
            }
        } else {
            None
        }
    }
}

#[async_trait]
impl ScyllaHandleExt for backstage::core::Runtime<UnboundedHandle<ScyllaEvent>> {
    async fn cluster_handle(&self) -> Option<UnboundedHandle<ClusterEvent>> {
        self.handle().cluster_handle().await
    }
}
