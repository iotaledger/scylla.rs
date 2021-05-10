// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;
use futures::{
    stream::{SplitSink, SplitStream},
    StreamExt,
};
use std::{borrow::Cow, net::SocketAddr};
use tokio::net::TcpStream;
pub(crate) use tokio_tungstenite::{connect_async, tungstenite::Message, WebSocketStream};

pub(crate) mod client;
mod init;
mod run;
mod shutdown;

#[build]
#[derive(Debug)]
pub fn build_websocket<ScyllaEvent, ScyllaHandle>(
    service: Service,
    peer: SocketAddr,
    stream: WebSocketStream<TcpStream>,
) -> Websocket {
    let (ws_tx, ws_rx) = stream.split();
    Websocket {
        service,
        peer,
        ws_rx,
        opt_ws_tx: Some(ws_tx),
    }
}

/// The writehalf of the webssocket
pub type WsTx = SplitSink<WebSocketStream<TcpStream>, Message>;
/// The readhalf of the webssocket
pub type WsRx = SplitStream<WebSocketStream<TcpStream>>;

/// Client Websocket struct
pub struct Websocket {
    service: Service,
    peer: SocketAddr,
    ws_rx: WsRx,
    opt_ws_tx: Option<WsTx>,
}

impl ActorTypes for Websocket {
    const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);

    type Error = anyhow::Error;

    fn service(&mut self) -> &mut Service {
        &mut self.service
    }
}
