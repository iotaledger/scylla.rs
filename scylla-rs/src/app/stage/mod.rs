// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::{
    ring::Registry,
    Scylla,
};
use crate::cql::{
    Compression,
    CqlBuilder,
};
use async_trait::async_trait;
use backstage::core::{
    Actor,
    ActorError,
    ActorResult,
    EolEvent,
    IoChannel,
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
use std::{
    cell::UnsafeCell,
    collections::HashMap,
    marker::PhantomData,
    net::SocketAddr,
    sync::Arc,
};
use tokio::sync::RwLock;

mod receiver;
/// The reporter module
pub mod reporter;
mod sender;
/// The max number
static MAX_STREAM_IDS: u16 = 32767;
/// The thread-safe reusable payloads.
pub type Payloads = Arc<Vec<Reusable>>;
/// Reporter handles at the stage level
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
pub struct Stage<C: Compression> {
    address: SocketAddr,
    shard_id: usize,
    shard_count: usize,
    _compression: PhantomData<fn(C) -> C>,
}

impl<C: Compression> Stage<C> {
    pub(super) fn new(address: SocketAddr, shard_id: usize, shard_count: usize) -> Self {
        Self {
            shard_count,
            address,
            shard_id,
            _compression: PhantomData,
        }
    }
}

/// The Stage actor lifecycle implementation
#[async_trait]
impl<S, C: 'static + Compression> Actor<S> for Stage<C>
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
        let last_range = appends_num * (scylla.reporter_count as u16);
        (0..last_range).for_each(|_| payloads.push(Reusable::default()));
        let payloads = Arc::new(payloads);
        rt.publish(payloads).await;
        let all_stage_streams: Vec<u16> = (0..last_range).collect();
        let streams_iter = all_stage_streams.chunks_exact(appends_num as usize);
        // start sender first to let it awaits reporters_handles resources
        let cql = CqlBuilder::<_, C>::new()
            .address(self.address)
            .tokens()
            .recv_buffer_size(scylla.recv_buffer_size)
            .send_buffer_size(scylla.send_buffer_size)
            .authenticator(scylla.authenticator.clone())
            .build();
        let cql_conn = cql.await.map_err(|e| ActorError::restart(e, None))?;
        // verify shard_count, (as in very rare condition scylla might get restarted with different shard count )
        if self.shard_count != cql_conn.shard_count() as usize {
            return Err(ActorError::restart_msg("scylla changed its shard count", None));
        };
        let (socket_rx, socket_tx) = cql_conn.split();
        let sender = sender::Sender::new(socket_tx, appends_num);
        let (_, sender_init_signal) = rt.spawn("sender".to_string(), sender).await?;
        // spawn receiver
        let receiver = receiver::Receiver::<C>::new(appends_num);
        let (_, receiver_init_signal) = rt
            .spawn_with_channel("receiver".to_string(), receiver, IoChannel(socket_rx))
            .await?;
        let reporters_registry: Arc<RwLock<Registry>> = rt.lookup(cluster_scope_id).await.ok_or_else(|| {
            ActorError::exit_msg("stage unable to lookup for reporters registry in cluster data store")
        })?;
        // start reporters where they can directly link to the sender handle
        let mut reporters_handles = HashMap::new();
        let mut reporter_id: u8 = 0;
        for reporter_streams_ids in streams_iter {
            let reporter = reporter::Reporter::<C>::new(reporter_streams_ids.into());
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
        let (scope_id, receiver_service) = receiver_init_signal.initialized().await?;
        rt.upsert_microservice(scope_id, receiver_service);
        let (scope_id, sender_service) = sender_init_signal.initialized().await?;
        rt.upsert_microservice(scope_id, sender_service);
        Ok((shard_socket_addr, reporters_registry))
    }
    async fn run(
        &mut self,
        rt: &mut Rt<Self, S>,
        (shard_socket_addr, reporters_registry): Self::Data,
    ) -> ActorResult<()> {
        log::info!("{} Stage is {}", shard_socket_addr, rt.service().status());
        while let Some(event) = rt.inbox_mut().next().await {
            match event {
                StageEvent::Microservice(scope_id, service) => {
                    if service.is_stopped() {
                        rt.remove_microservice(scope_id);
                        rt.shutdown_children().await;
                        if !rt.service().is_stopping() {
                            if rt.microservices_stopped() {
                                return Err(ActorError::restart_msg("disconnected", None));
                            } else {
                                rt.update_status(ServiceStatus::Degraded).await;
                            }
                        } else {
                            rt.update_status(ServiceStatus::Stopping).await;
                            if rt.microservices_stopped() {
                                rt.inbox_mut().close();
                            }
                        }
                    } else {
                        rt.upsert_microservice(scope_id, service);
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

pub(super) fn appends_num(reporter_count: u8) -> u16 {
    MAX_STREAM_IDS / (reporter_count as u16)
}

pub(super) fn compute_reporter_num(stream_id: u16, appends_num: u16) -> u8 {
    (stream_id / appends_num) as u8
}
