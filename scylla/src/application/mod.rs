// Copyright 2020 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use crate::{cluster::*, listener::*, websocket::*};

pub use chronicle::*;
pub use log::*;
pub use tokio::{spawn, sync::mpsc};

use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
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
        thread_count: u16,
        local_dc: String,
        buffer_size: usize,
        recv_buffer_size: usize,
        send_buffer_size: usize,
        listener_handle: ListenerHandle,
        authenticator: usize
});

#[derive(Deserialize, Serialize)]
pub enum ScyllaThrough {
    Shutdown,
    AddNode(String),
    RemoveNode(String),
    TryBuild(u8),
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
    // cluster_handle: u8,
    websockets: HashMap<String, WsTx>,
    handle: Option<ScyllaHandle<H>>,
    inbox: ScyllaInbox<H>,
    /*    tcp_listener:
     *    listener: None,
     *    sockets: HashMap::new(),
     *    tx: Sender(tx),
     *    rx, */
}

/// SubEvent type, indicated the children
pub enum ScyllaChild {
    Listener(Service, Option<Result<(), Need>>),
    Cluster(Service, Option<Result<(), Need>>),
    Websocket(Service, Option<WsTx>),
}

/// Event type of the Scylla Application
pub enum ScyllaEvent<T> {
    Passthrough(T),
    Children(ScyllaChild),
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
            // cluster_handle: u8,
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
