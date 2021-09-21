// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::{
    ring::Registry,
    Scylla,
};
use crate::cql::CqlBuilder;
use async_trait::async_trait;
use backstage::core::{
    Actor,
    ActorError,
    ActorResult,
    IoChannel,
    Rt,
    ScopeId,
    Service,
    ServiceStatus,
    StreamExt,
    SupHandle,
    UnboundedChannel,
    UnboundedHandle,
};
use std::{
    cell::UnsafeCell,
    collections::HashMap,
    net::SocketAddr,
    ops::{
        Deref,
        DerefMut,
    },
    sync::Arc,
};
use tokio::{
    net::TcpStream,
    sync::RwLock,
};

mod receiver;
pub mod reporter;
mod sender;
/// The max number
static MAX_STREAM_IDS: i16 = 32767;
/// The thread-safe reusable payloads.
pub type Payloads = Arc<Vec<Reusable>>;
pub type ReportersHandles = HashMap<u8, UnboundedHandle<reporter::ReporterEvent>>;
/// Stage event enum.
#[backstage::core::supervise]
pub enum StageEvent {
    /// Child status change
    #[report]
    #[eol]
    Microservice(ScopeId, Service),
    /// Shutdwon a stage.
    #[shutdown]
    Shutdown,
}

/// Stage state
pub struct Stage {
    address: SocketAddr,
    session_id: usize,
    shard_id: usize,
    shard_count: usize,
    // payloads: Payloads, todo add it as resource
}

impl Stage {
    pub fn new(address: SocketAddr, shard_id: usize, shard_count: usize) -> Self {
        Self {
            shard_count,
            address,
            shard_id,
            session_id: 0,
        }
    }
}

/// The Stage actor lifecycle implementation
#[async_trait]
impl<S> Actor<S> for Stage
where
    S: SupHandle<Self>,
{
    type Data = (SocketAddr, Arc<RwLock<Registry>>);
    type Channel = UnboundedChannel<StageEvent>;
    async fn init(&mut self, rt: &mut Rt<Self, S>) -> ActorResult<Self::Data> {
        let scylla_resource_scope_id = rt
            .highest_scope_id::<Scylla>()
            .await
            .ok_or_else(|| ActorError::exit_msg("stage unables to get scylla resource scope id"))?;
        let scylla: Scylla = rt
            .lookup(scylla_resource_scope_id)
            .await
            .ok_or_else(|| ActorError::exit_msg("stage unables to lookup for scylla as config"))?;
        let cluster_scope_id = rt
            .grandparent()
            .scope_id()
            .await
            .ok_or_else(|| ActorError::exit_msg("stage doesn't have cluster as grandparent"))?;
        // create payload resource
        let mut payloads = Vec::new();
        let appends_num = appends_num(scylla.reporter_count);
        let last_range = appends_num * (scylla.reporter_count as i16);
        (0..last_range).for_each(|_| payloads.push(Reusable::default()));
        let payloads = Arc::new(payloads);
        rt.publish(payloads).await;
        let all_stage_streams: Vec<i16> = (0..last_range).collect();
        let mut streams_iter = all_stage_streams.chunks_exact(appends_num as usize);
        // start sender first to let it awaits reporters_handles resources
        let cql = CqlBuilder::new()
            .address(self.address)
            .tokens()
            .recv_buffer_size(scylla.recv_buffer_size)
            .send_buffer_size(scylla.send_buffer_size)
            .authenticator(scylla.authenticator.clone())
            .build();
        let cql_conn = cql.await.map_err(|e| ActorError::restart(e, None))?;
        // verify shard_count, (as in very rare condition scylla might get restarted with different shard count )
        if self.shard_count != cql_conn.shard_count() as usize {
            return Err(ActorError::exit_msg("scylla changed its shard count"));
        };
        let (socket_rx, socket_tx) = cql_conn.split();
        let sender = sender::Sender::new(socket_tx, appends_num);
        rt.spawn("sender".to_string(), sender).await?;
        // spawn receiver
        let receiver = receiver::Receiver::new(scylla.buffer_size, appends_num);
        rt.spawn_with_channel("receiver".to_string(), receiver, IoChannel(socket_rx))
            .await?;
        let reporters_registry: Arc<RwLock<Registry>> = rt.lookup(cluster_scope_id).await.ok_or_else(|| {
            ActorError::exit_msg("stage unable to lookup for reporters registry in cluster data store")
        })?;
        // start reporters where they can directly link to the sender handle
        let mut reporters_handles = HashMap::new();
        let mut reporter_id: u8 = 0;
        for reporter_streams_ids in streams_iter.next() {
            let reporter = reporter::Reporter::new(reporter_streams_ids.into());
            let reporter_handle = rt.start(format!("reporter_{}", reporter_id), reporter).await?;
            reporters_handles.insert(reporter_id, reporter_handle);
            reporter_id += 1;
        }
        rt.publish(reporters_handles.clone()).await;
        let mut shard_socket_addr = self.address.clone();
        shard_socket_addr.set_port(self.shard_id as u16);
        reporters_registry
            .write()
            .await
            .insert(shard_socket_addr.clone(), reporters_handles);
        Ok((shard_socket_addr, reporters_registry))
    }
    async fn run(
        &mut self,
        rt: &mut Rt<Self, S>,
        (shard_socket_addr, reporters_registry): Self::Data,
    ) -> ActorResult<()> {
        while let Some(event) = rt.inbox_mut().next().await {
            match event {
                StageEvent::Microservice(scope_id, service) => {
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
                StageEvent::Shutdown => {
                    rt.stop().await;
                    if rt.microservices_stopped() {
                        rt.inbox_mut().close();
                    }
                }
            }
        }
        reporters_registry.write().await.remove(&shard_socket_addr);
        Ok(())
    }
}

#[derive(Default)]
/// The reusable sender payload.
pub struct Reusable {
    value: UnsafeCell<Option<Vec<u8>>>,
}

impl Reusable {
    #[allow(clippy::mut_from_ref)]
    /// Return as mutable sender payload value.
    pub fn as_mut(&self) -> &mut Option<Vec<u8>> {
        unsafe { self.value.get().as_mut().unwrap() }
    }
    /// Return as reference sender payload.
    pub fn as_ref_payload(&self) -> Option<&Vec<u8>> {
        unsafe { self.value.get().as_ref().unwrap().as_ref() }
    }
    /// Return as mutable sender payload.
    pub fn as_mut_payload(&self) -> Option<&mut Vec<u8>> {
        self.as_mut().as_mut()
    }
}

unsafe impl Sync for Reusable {}

pub(super) fn appends_num(reporter_count: u8) -> i16 {
    MAX_STREAM_IDS / (reporter_count as i16)
}

pub(super) fn compute_reporter_num(stream_id: i16, appends_num: i16) -> u8 {
    (stream_id / appends_num) as u8
}
