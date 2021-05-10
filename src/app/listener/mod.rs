// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::{websocket::WebsocketBuilder, *};
use std::{borrow::Cow, net::SocketAddr};

mod init;
mod run;
mod shutdown;

#[build]
#[derive(Debug, Clone)]
pub fn build_listener<ScyllaEvent, ScyllaHandle>(service: Service, listen_address: SocketAddr) -> Listener {
    Listener {
        service,
        listen_address,
    }
}
/// Listener state
pub struct Listener {
    service: Service,
    listen_address: SocketAddr,
}

impl ActorTypes for Listener {
    const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);
    type Error = Cow<'static, str>;

    fn service(&mut self) -> &mut Service {
        &mut self.service
    }
}
