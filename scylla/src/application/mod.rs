pub use chronicle::*;
use serde::{Deserialize, Serialize};
use std::ops::{Deref, DerefMut};

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
        nodes: Vec<String>,
        authenticator: usize
});

#[derive(Deserialize, Serialize)]
pub enum ScyllaThrough {
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
    listener_handle: u8,
    cluster_handle: u8,
    websockets: usize,
    handle: ScyllaHandle<H>,
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
    Websocket(Service, Option<Result<(), Need>>),
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
        todo!()
        // build Scylla app here
    }
}

/// implementation of passthrough functionality
impl<H: ScyllaScope> Passthrough<ScyllaThrough> for ScyllaHandle<H> {
    fn launcher_status_change(&mut self, service: &Service) {
        todo!()
    }
    fn app_status_change(&mut self, service: &Service) {
        todo!()
    }
    fn send_event(&mut self, event: ScyllaThrough, from_app_name: String) {
        todo!()
    }
    fn service(&mut self, service: &Service) {
        todo!()
    }
}

/// implementation of shutdown functionality
impl<H: ScyllaScope> Shutdown for ScyllaHandle<H> {
    fn shutdown(self) -> Option<Self>
    where
        Self: Sized,
    {
        todo!()
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

/// blanket actor implementation
impl<H: ScyllaScope> Actor<H> for Scylla<H> {}

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
