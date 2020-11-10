use crate::application::*;
use futures::{
    stream::{SplitSink, SplitStream},
    StreamExt,
};
use std::net::SocketAddr;
use tokio::net::TcpStream;
use tokio_tungstenite::{tungstenite::Message, WebSocketStream};
mod event_loop;
mod init;
mod terminating;

builder!(
    WebsocketdBuilder {
        peer: SocketAddr,
        stream: WebSocketStream<TcpStream>
});

// Listener state
pub struct Websocket {
    service: Service,
    peer: SocketAddr,
    ws_rx: SplitStream<WebSocketStream<TcpStream>>,
    ws_tx: Option<SplitSink<WebSocketStream<TcpStream>, Message>>,
}

impl<T: Appsthrough<ScyllaThrough>> ActorBuilder<ScyllaHandle<T>> for WebsocketdBuilder {}
impl<T: Appsthrough<ScyllaThrough>> Actor<ScyllaHandle<T>> for Websocket {}

impl Builder for WebsocketdBuilder {
    type State = Websocket;
    fn build(self) -> Self::State {
        // split the websocket stream
        let (ws_tx, ws_rx) = self.stream.unwrap().split();
        Websocket {
            service: Service::new(),
            peer: self.peer.unwrap(),
            ws_rx,
            ws_tx: Some(ws_tx),
        }
        .set_name()
    }
}

/// impl name of the Websocket
impl Name for Websocket {
    fn set_name(mut self) -> Self {
        let name: String = self.peer.to_string();
        self.service.update_name(name);
        self
    }
    fn get_name(&self) -> String {
        self.service.get_name()
    }
}

#[async_trait::async_trait]
impl<T: Appsthrough<ScyllaThrough>> AknShutdown<Websocket> for ScyllaHandle<T> {
    async fn aknowledge_shutdown(mut self, mut _state: Websocket, _status: Result<(), Need>) {
        _state.service.update_status(ServiceStatus::Stopped);
        let event = ScyllaEvent::Children(ScyllaChild::Websocket(_state.service.clone(), Some(_status)));
        let _ = self.send(event);
    }
}
