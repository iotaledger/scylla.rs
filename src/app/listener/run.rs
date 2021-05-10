// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;
use tokio::net::TcpListener;
use tokio_tungstenite::accept_async;

#[async_trait::async_trait]
impl Run<ScyllaEvent, ScyllaHandle> for Listener {
    async fn run(&mut self, supervisor: &mut ScyllaHandle) -> Result<(), Self::Error> {
        let tcp_listener = TcpListener::bind(self.listen_address)
            .await
            .map_err(|e| Self::Error::from(e.to_string()))?;
        loop {
            if let Ok((socket, peer)) = tcp_listener.accept().await {
                let peer = socket.peer_addr().unwrap_or(peer);
                if let Ok(ws_stream) = accept_async(socket).await {
                    // build websocket
                    let websocket = WebsocketBuilder::new()
                        .peer(peer)
                        .stream(ws_stream)
                        .build(self.service.spawn("Websocket"));
                    // spawn websocket
                    tokio::spawn(websocket.start(supervisor.clone()));
                }
            }
        }
    }
}
