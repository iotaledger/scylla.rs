use crate::application::*;

use futures::future::AbortHandle;
use tokio::net::{TcpListener, TcpStream};
mod event_loop;
mod init;
mod terminating;

// Listener builder
builder!(ListenerBuilder { tcp_listener: TcpListener });

/// ListenerHandle to be passed to the application/Scylla in order to shutdown/abort the listener
pub struct ListenerHandle {
    abort_handle: AbortHandle,
}

// Listener state
pub struct Listener {
    service: Service,
    tcp_listener: TcpListener,
}

impl<T: Appsthrough<ScyllaThrough>> ActorBuilder<ScyllaHandle<T>> for ListenerBuilder {}

/// implementation of builder
impl Builder for ListenerBuilder {
    type State = Listener;
    fn build(self) -> Self::State {
        todo!()
    }
}

impl<T: Appsthrough<ScyllaThrough>> Actor<ScyllaHandle<T>> for Listener {}

/// impl name of the Listener
impl Name for Listener {
    fn set_name(self) -> Self {
        todo!()
    }
    fn get_name(&self) -> String {
        todo!()
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
impl<T: Appsthrough<ScyllaThrough>> AknShutdown<Listener> for ScyllaHandle<T> {
    async fn aknowledge_shutdown(self, mut _state: Listener, _status: Result<(), Need>) {
        _state.service.update_status(ServiceStatus::Stopped);
        let event = ScyllaEvent::Children(ScyllaChild::Listener(_state.service.clone(), Some(_status)));
        let _ = self.send(event);
    }
}
