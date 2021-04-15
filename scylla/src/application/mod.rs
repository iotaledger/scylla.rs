// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use crate::{cluster::*, listener::*, websocket::*};

use anyhow::{anyhow, bail};
pub use backstage::*;
pub use client::add_nodes::add_nodes;
pub use log::*;
pub(crate) use scylla_cql::{CqlBuilder, PasswordAuth};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    net::SocketAddr,
    ops::{Deref, DerefMut},
};
pub use tokio::{spawn, sync::mpsc};

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
/// It's the Interface the scylla app to dynamiclly configure the application during runtime
pub enum ScyllaThrough {
    /// Shutdown json to gracefully shutdown scylla app
    Shutdown,
    Topology(Topology),
}

#[repr(u8)]
#[derive(PartialEq)]
pub enum Caller {
    Launcher = 0,
    Other = 1,
}

/// ScyllaHandle to be passed to the children (Listener and Cluster)
pub struct ScyllaHandle<H: ScyllaScope> {
    pub(crate) caller: Caller,
    tx: tokio::sync::mpsc::UnboundedSender<ScyllaEvent<H::AppsEvents>>,
}
/// ScyllaInbox used to recv events
pub struct ScyllaInbox<H: ScyllaScope> {
    rx: tokio::sync::mpsc::UnboundedReceiver<ScyllaEvent<H::AppsEvents>>,
}

impl<H: ScyllaScope> Clone for ScyllaHandle<H> {
    fn clone(&self) -> Self {
        ScyllaHandle::<H> {
            caller: Caller::Other,
            tx: self.tx.clone(),
        }
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
    /// Used by Listener to keep scylla up to date with its service
    Listener(Service),
    /// Used by Cluster to keep scylla up to date with its service
    Cluster(Service),
    /// Used by Websocket to keep scylla up to date with its service
    Websocket(Service, Option<WsTx>),
}

/// Event type of the Scylla Application
pub enum ScyllaEvent<T> {
    /// It's the passthrough event, which the scylla application will receive from
    Passthrough(T),
    /// Used by scylla children to push their service
    Children(ScyllaChild),
    /// Used by cluster to inform scylla in order to inform the sockets with the result of topology events
    Result(SocketMsg<Result<Topology, Topology>>),
    /// Abort the scylla app, sent by launcher
    Abort,
}

#[derive(Deserialize, Serialize, Debug)]
/// Topology event
pub enum Topology {
    /// AddNode json to add new scylla node
    AddNode(SocketAddr),
    /// RemoveNode json to remove an existing scylla node
    RemoveNode(SocketAddr),
    /// BuildRing json to re/build the cluster topology,
    /// Current limitation: for now the admin supposed to define uniform replication factor among all DataCenter and
    /// all keyspaces
    BuildRing(u8),
}

#[derive(Deserialize, Serialize)]
// use Scylla to indicate to the msg is from/to Scylla
pub enum SocketMsg<T> {
    Scylla(T),
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
        let handle = Some(ScyllaHandle {
            caller: Caller::Other,
            tx,
        });
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

// TODO integrate well with other services;
/// implementation of passthrough functionality
impl<H: ScyllaScope> Passthrough<ScyllaThrough> for ScyllaHandle<H> {
    fn launcher_status_change(&mut self, _service: &Service) {}
    fn app_status_change(&mut self, _service: &Service) {}
    fn passthrough(&mut self, _event: ScyllaThrough, _from_app_name: String) {}
    fn service(&mut self, _service: &Service) {}
}

/// implementation of shutdown functionality
impl<H: ScyllaScope> Shutdown for ScyllaHandle<H> {
    fn shutdown(self) -> Option<Self>
    where
        Self: Sized,
    {
        let scylla_shutdown: H::AppsEvents = serde_json::from_str("{\"Scylla\": \"Shutdown\"}").unwrap();
        let _ = self.send(ScyllaEvent::Passthrough(scylla_shutdown));
        // if the caller is launcher we abort scylla app. better solution will be implemented in future
        if self.caller == Caller::Launcher {
            let _ = self.send(ScyllaEvent::Abort);
        }
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
