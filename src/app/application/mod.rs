// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::{
    cluster::{Cluster, ClusterBuilder},
    listener::{Listener, ListenerBuilder},
    websocket::WsTx,
    *,
};
pub(crate) use crate::cql::{CqlBuilder, PasswordAuth};
use backstage::launcher::BuilderData;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, marker::PhantomData, net::SocketAddr};
use tokio::task::JoinHandle;

mod init;
mod run;
mod shutdown;

/// Defines resources needed to spawn a non-evented actor and manage it
pub struct NonEventedData<A, B, E, S>
where
    A: Actor<E, S>,
    B: ActorBuilder<A, E, S> + Clone,
    S: 'static + Send + EventHandle<E>,
{
    /// The name of the actor
    pub name: String,
    /// The actor's builder, used to spawn new `Actor`s
    pub builder: B,
    /// The actor's thread's join handle, used to await the completion of
    /// and actor which is terminating.
    pub join_handle: Option<JoinHandle<Result<ActorRequest, ActorError>>>,
    pub _data: PhantomData<(A, E, S)>,
}

impl<A, B, E, S> NonEventedData<A, B, E, S>
where
    A: Actor<E, S>,
    B: ActorBuilder<A, E, S> + Clone,
    S: 'static + Send + EventHandle<E>,
{
    pub fn new(name: String, builder: B) -> Self {
        Self {
            name,
            builder,
            join_handle: None,
            _data: Default::default(),
        }
    }
}

#[build]
#[derive(Debug, Clone)]
pub fn build_scylla(
    service: Service,
    listen_address: SocketAddr,
    reporter_count: u8,
    thread_count: usize,
    local_dc: String,
    buffer_size: Option<usize>,
    recv_buffer_size: Option<u32>,
    send_buffer_size: Option<u32>,
    authenticator: Option<PasswordAuth>,
) -> Scylla {
    if reporter_count == 0 {
        panic!("reporter_count must be greater than zero, ensure your config is correct");
    }
    if local_dc == "" {
        panic!("local_datacenter must be non-empty string, ensure your config is correct");
    }
    let (sender, inbox) = tokio::sync::mpsc::unbounded_channel();
    let handle = ScyllaHandle {
        caller: Caller::Launcher,
        sender,
    };
    let listener_builder = ListenerBuilder::new().listen_address(listen_address);
    let cluster_builder = ClusterBuilder::new()
        .reporter_count(reporter_count)
        .thread_count(thread_count)
        .data_centers(vec![local_dc])
        .recv_buffer_size(recv_buffer_size)
        .send_buffer_size(send_buffer_size)
        .buffer_size(buffer_size.unwrap_or(1024000))
        .authenticator(authenticator.unwrap_or(PasswordAuth::default()));

    Scylla {
        service,
        listener_data: NonEventedData::new("Listener".into(), listener_builder),
        cluster_data: BuilderData::new("Cluster".into(), cluster_builder),
        websockets: HashMap::new(),
        handle,
        inbox,
    }
}

#[allow(missing_docs)]
#[repr(u8)]
#[derive(PartialEq)]
pub enum Caller {
    Launcher = 0,
    Other = 1,
}

/// ScyllaHandle to be passed to the children (Listener and Cluster)
pub struct ScyllaHandle {
    pub(crate) caller: Caller,
    sender: tokio::sync::mpsc::UnboundedSender<ScyllaEvent>,
}

impl Clone for ScyllaHandle {
    fn clone(&self) -> Self {
        ScyllaHandle {
            caller: Caller::Other,
            sender: self.sender.clone(),
        }
    }
}

#[derive(Error, Debug)]
pub enum ScyllaError {
    #[error("No cluster available!")]
    NoCluster,
    #[error(transparent)]
    Other {
        #[from]
        source: anyhow::Error,
    },
}

impl Into<ActorError> for ScyllaError {
    fn into(self) -> ActorError {
        match self {
            ScyllaError::NoCluster => ActorError::InvalidData(self.to_string()),
            ScyllaError::Other { source } => ActorError::Other {
                source,
                request: ActorRequest::Finish,
            },
        }
    }
}

/// Application state
pub struct Scylla {
    service: Service,
    listener_data: NonEventedData<Listener, ListenerBuilder, ScyllaEvent, ScyllaHandle>,
    cluster_data: BuilderData<Cluster, ClusterBuilder, ScyllaEvent, ScyllaHandle>,
    websockets: HashMap<String, WsTx>,
    handle: ScyllaHandle,
    inbox: tokio::sync::mpsc::UnboundedReceiver<ScyllaEvent>,
}

impl ActorTypes for Scylla {
    const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(60);

    type Error = ScyllaError;

    fn service(&mut self) -> &mut Service {
        &mut self.service
    }
}

impl<E, S> EventActor<E, S> for Scylla
where
    S: 'static + Send + EventHandle<E>,
{
    type Event = ScyllaEvent;

    type Handle = ScyllaHandle;

    fn handle(&mut self) -> &mut Self::Handle {
        &mut self.handle
    }
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
pub enum ScyllaEvent {
    Shutdown,
    /// Used by scylla children to push their service
    Children(ScyllaChild),
    /// Used by cluster to inform scylla in order to inform the sockets with the result of topology events
    Result(ScyllaSocketMsg<Result<Topology, Topology>>),
    Websocket(ScyllaWebsocketEvent),
}

#[derive(Serialize, Deserialize, Debug)]
pub enum ScyllaWebsocketEvent {
    Topology(Topology),
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
/// Indicates which app this message is for
pub enum ScyllaSocketMsg<T> {
    /// Message for Scylla app
    Scylla(T),
}

impl EventHandle<ScyllaEvent> for ScyllaHandle {
    fn send(&mut self, message: ScyllaEvent) -> anyhow::Result<()> {
        self.sender.send(message).ok();
        Ok(())
    }

    fn shutdown(mut self) -> Option<Self>
    where
        Self: Sized,
    {
        if let Ok(()) = self.send(ScyllaEvent::Shutdown) {
            None
        } else {
            Some(self)
        }
    }

    fn update_status(&mut self, service: Service) -> anyhow::Result<()>
    where
        Self: Sized,
    {
        match service.name.as_str() {
            "Listener" => self.send(ScyllaEvent::Children(ScyllaChild::Listener(service))),
            "Cluster" => self.send(ScyllaEvent::Children(ScyllaChild::Cluster(service))),
            "Websocket" => self.send(ScyllaEvent::Children(ScyllaChild::Websocket(service, None))),
            _ => Ok(()),
        }
    }
}
