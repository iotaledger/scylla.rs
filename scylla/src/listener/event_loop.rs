use super::*;
use crate::websocket::WebsocketdBuilder;
use tokio_tungstenite::accept_async;

#[async_trait::async_trait]
impl<H: ScyllaScope> EventLoop<ScyllaHandle<H>> for Listener {
    async fn event_loop(&mut self, _status: Result<(), Need>, supervisor: &mut Option<ScyllaHandle<H>>) -> Result<(), Need> {
        self.service.update_status(ServiceStatus::Running);
        let event = ScyllaEvent::Children(ScyllaChild::Listener(self.service.clone(), None));
        let _ = supervisor.as_mut().unwrap().send(event);
        loop {
            if let Ok((socket, peer)) = self.tcp_listener.accept().await {
                let peer = socket.peer_addr().unwrap_or(peer);
                if let Ok(ws_stream) = accept_async(socket).await {
                    // build websocket
                    let websocket = WebsocketdBuilder::new().peer(peer).stream(ws_stream).build();
                    // spawn websocket
                    tokio::spawn(websocket.start(supervisor.clone()));
                }
            }
        }
    }
}
