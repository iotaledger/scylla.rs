// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use crate::{cluster::*, listener::*, websocket::*};

pub use chronicle::*;
pub use log::*;
pub use tokio::{spawn, sync::mpsc};

pub(crate) use scylla_cql::{CqlBuilder, PasswordAuth};

use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    net::SocketAddr,
    ops::{Deref, DerefMut},
};

mod event_loop;
mod init;
mod starter;
mod terminating;

/// Define the application scope trait
pub trait ScyllaScope: LauncherSender<ScyllaBuilder<Self>> {}
impl<H: LauncherSender<ScyllaBuilder<H>>> ScyllaScope for H {}

// Scylla builder
builder!(
    #[derive(Clone)]
    ScyllaBuilder<H> {
        listen_address: String,
        reporter_count: u8,
        thread_count: usize,
        local_dc: String,
        buffer_size: usize,
        recv_buffer_size: u32,
        send_buffer_size: u32,
        listener_handle: ListenerHandle,
        cluster_handle: ClusterHandle,
        authenticator: PasswordAuth
});

#[derive(Deserialize, Serialize)]
pub enum ScyllaThrough {
    Shutdown,
    AddNode(SocketAddr),
    RemoveNode(SocketAddr),
    BuildRing(u8),
}

/// ScyllaHandle to be passed to the children (Listener and Cluster)
pub struct ScyllaHandle<H: ScyllaScope> {
    tx: tokio::sync::mpsc::UnboundedSender<ScyllaEvent<H::AppsEvents>>,
}
/// ScyllaInbox used to recv events
pub struct ScyllaInbox<H: ScyllaScope> {
    rx: tokio::sync::mpsc::UnboundedReceiver<ScyllaEvent<H::AppsEvents>>,
}

impl<H: ScyllaScope> Clone for ScyllaHandle<H> {
    fn clone(&self) -> Self {
        ScyllaHandle::<H> { tx: self.tx.clone() }
    }
}

/// Application state
pub struct Scylla<H: ScyllaScope> {
    service: Service,
    listener_handle: Option<ListenerHandle>,
    cluster_handle: Option<ClusterHandle>,
    websockets: HashMap<String, WsTx>,
    handle: Option<ScyllaHandle<H>>,
    inbox: ScyllaInbox<H>,
}

/// SubEvent type, indicated the children
pub enum ScyllaChild {
    Listener(Service),
    Cluster(Service),
    Websocket(Service, Option<WsTx>),
}

/// Event type of the Scylla Application
pub enum ScyllaEvent<T> {
    Passthrough(T),
    Children(ScyllaChild),
    Result(SocketMsg),
}

#[derive(Deserialize, Serialize)]
pub enum Topology {
    AddingNode(SocketAddr),
    RemovingNode(SocketAddr),
    BuiltRing,
}

#[derive(Deserialize, Serialize)]
// use Scylla to indicate to the websocket recipient that the json msg from Scylla application
pub enum SocketMsg {
    Scylla(Result<Topology, Topology>),
}

/// implementation of the AppBuilder
impl<H: ScyllaScope> AppBuilder<H> for ScyllaBuilder<H> {}

/// implementation of through type
impl<H: ScyllaScope> ThroughType for ScyllaBuilder<H> {
    type Through = ScyllaThrough;
}

/// implementation of builder
impl<H: ScyllaScope> Builder for ScyllaBuilder<H> {
    type State = Scylla<H>;
    fn build(self) -> Self::State {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let handle = Some(ScyllaHandle { tx });
        let inbox = ScyllaInbox { rx };
        Scylla::<H> {
            service: Service::new(),
            listener_handle: Some(self.listener_handle.expect("Expected Listener handle")),
            cluster_handle: self.cluster_handle,
            websockets: HashMap::new(),
            handle,
            inbox,
        }
        .set_name()
    }
}

/// implementation of passthrough functionality
impl<H: ScyllaScope> Passthrough<ScyllaThrough> for ScyllaHandle<H> {
    fn launcher_status_change(&mut self, service: &Service) {}
    fn app_status_change(&mut self, service: &Service) {}
    fn passthrough(&mut self, event: ScyllaThrough, from_app_name: String) {}
    fn service(&mut self, service: &Service) {}
}

/// implementation of shutdown functionality
impl<H: ScyllaScope> Shutdown for ScyllaHandle<H> {
    fn shutdown(self) -> Option<Self>
    where
        Self: Sized,
    {
        let scylla_shutdown: H::AppsEvents = serde_json::from_str("{\"Scylla\": \"Shutdown\"}").unwrap();
        let _ = self.send(ScyllaEvent::Passthrough(scylla_shutdown));
        None
    }
}

impl<H: ScyllaScope> Deref for ScyllaHandle<H> {
    type Target = tokio::sync::mpsc::UnboundedSender<ScyllaEvent<H::AppsEvents>>;

    fn deref(&self) -> &Self::Target {
        &self.tx
    }
}

impl<H: ScyllaScope> DerefMut for ScyllaHandle<H> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.tx
    }
}

/// impl name of the application
impl<H: ScyllaScope> Name for Scylla<H> {
    fn set_name(mut self) -> Self {
        self.service.update_name("Scylla".to_string());
        self
    }
    fn get_name(&self) -> String {
        self.service.get_name()
    }
}
