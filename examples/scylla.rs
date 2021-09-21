// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

#[cfg(feature = "app")]
use scylla_rs::prelude::*;

#[tokio::main]
async fn main() {
    env_logger::init();
    let scylla = Scylla::default();
    let runtime = Runtime::new(None, scylla).await.expect("runtime to run");
    // todo make server address as env var in backstage
    #[cfg(feature = "websocket_server")]
    runtime.websocket_server(None, "127.0.0.1:10000").await.expect("websocket server to run");
    // todo launch ws_client to add scylla node
    runtime.block_on().await.expect("runtime to gracefully shutdown")
}
