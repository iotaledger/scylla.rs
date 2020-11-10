use super::*;
use tokio_tungstenite::accept_async;

#[async_trait::async_trait]
impl<T: Appsthrough<ScyllaThrough>> EventLoop<ScyllaHandle<T>> for Listener {
    async fn event_loop(&mut self, status: Result<(), Need>, supervisor: &mut Option<ScyllaHandle<T>>) -> Result<(), Need> {
        todo!()
    }

}

async fn accept_connection(stream: TcpStream) {
    let addr = stream
        .peer_addr()
        .expect("connected streams should have a peer address");
    //info!("Peer address: {}", addr);

    let ws_stream = accept_async(stream)
        .await
        .expect("Error during the websocket handshake occurred");

    //info!("New WebSocket connection: {}", addr);

}
