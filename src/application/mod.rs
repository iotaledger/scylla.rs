pub use chronicle::*;
use serde::{Deserialize, Serialize};
use std::ops::Deref;
use std::ops::DerefMut;

mod starter;
mod init;
mod event_loop;
mod terminating;

/// Define the application scope trait
pub trait ScyllaScope: LauncherSender<ScyllaThrough> + AknShutdown<Scylla> {}

// Scylla builder
builder!(
    #[derive(Clone)]
    ScyllaBuilder {
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

}

/// ScyllaHandle to be passed to the children (Listener and Cluster)
pub struct ScyllaHandle<T> {
    phantom: std::marker::PhantomData<T>,
    tx: tokio::sync::mpsc::UnboundedSender<ScyllaEvent>,
}

/// Application state
pub struct Scylla {
    service: Service,
//    tcp_listener:
//    listener: None,
//    sockets: HashMap::new(),
//    tx: Sender(tx),
//    rx,
}

/// SubEvent type, indicated the children
pub enum ScyllaChild {
    Listener(Service, Option<Result<(), Need>>),
    Cluster(Service, Option<Result<(), Need>>),
}

/// Event type of the Scylla Application
pub enum ScyllaEvent {
    Children(ScyllaChild),
}

/// implementation of the AppBuilder
impl<H: ScyllaScope> AppBuilder<H> for ScyllaBuilder {}

/// implementation of through type
impl ThroughType for ScyllaBuilder {
    type Through = ScyllaThrough;
}

/// implementation of builder
impl Builder for ScyllaBuilder {
    type State = Scylla;
    fn build(self) -> Self::State {
        todo!()
        // build Scylla app here
    }
}

/// implementation of passthrough functionality
impl<T: Appsthrough<ScyllaThrough>> Passthrough<ScyllaThrough> for ScyllaHandle<T> {
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
impl<T: Appsthrough<ScyllaThrough>> Shutdown for ScyllaHandle<T> {
    fn shutdown(self) -> Option<Self>
    where Self: Sized {
        todo!()
    }
}

impl<T: Appsthrough<ScyllaThrough>> Deref for ScyllaHandle<T> {
    type Target = tokio::sync::mpsc::UnboundedSender<ScyllaEvent>;

    fn deref(&self) -> &Self::Target {
        &self.tx
    }
}

impl<T: Appsthrough<ScyllaThrough>> DerefMut for ScyllaHandle<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.tx
    }
}

/// blanket actor implementation
impl<H: ScyllaScope> Actor<H> for Scylla {}

/// impl name of the application
impl Name for Scylla {
    fn set_name(mut self) -> Self {
        self.service.update_name("Scylla".to_string());
        self
    }
    fn get_name(&self) -> String {
        self.service.get_name()
    }
}
