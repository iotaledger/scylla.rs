// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::{websocket::WebsocketBuilder, *};
use futures::future::AbortHandle;
use tokio::net::TcpListener;
mod event_loop;
mod init;
mod terminating;

// Listener builder
builder!(ListenerBuilder {
    tcp_listener: TcpListener
});

#[derive(Clone)]
/// ListenerHandle to be passed to the application/Scylla in order to shutdown/abort the listener
pub struct ListenerHandle {
    abort_handle: AbortHandle,
}
impl ListenerHandle {
    /// Create a new listener handle
    pub fn new(abort_handle: AbortHandle) -> Self {
        Self { abort_handle }
    }
}
/// Listener state
pub struct Listener {
    service: Service,
    tcp_listener: TcpListener,
}
impl<H: ScyllaScope> ActorBuilder<ScyllaHandle<H>> for ListenerBuilder {}

/// implementation of builder
impl Builder for ListenerBuilder {
    type State = Listener;
    fn build(self) -> Self::State {
        Self::State {
            service: Service::new(),
            tcp_listener: self.tcp_listener.expect("Expected tcp_listener in ListenerBuilder"),
        }
        .set_name()
    }
}

/// impl name of the Listener
impl Name for Listener {
    fn set_name(mut self) -> Self {
        self.service.update_name("Listener".to_string());
        self
    }
    fn get_name(&self) -> String {
        self.service.get_name()
    }
}

impl Shutdown for ListenerHandle {
    fn shutdown(self) -> Option<Self>
    where
        Self: Sized,
    {
        // abortable actor just require abort()
        self.abort_handle.abort();
        None
    }
}

#[async_trait::async_trait]
impl<H: ScyllaScope> AknShutdown<Listener> for ScyllaHandle<H> {
    async fn aknowledge_shutdown(self, mut _state: Listener, _status: Result<(), Need>) {
        _state.service.update_status(ServiceStatus::Stopped);
        let event = ScyllaEvent::Children(ScyllaChild::Listener(_state.service.clone()));
        let _ = self.send(event);
    }
}
