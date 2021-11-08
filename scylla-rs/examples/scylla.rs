// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

#[cfg(feature = "app")]
use scylla_rs::prelude::*;

#[tokio::main]
async fn main() {
    std::env::set_var("RUST_LOG", "info");
    env_logger::init();
    let mut scylla = Scylla::default();
    // note: you can use the handle to add the node later
    scylla.insert_node(example_scylla_node());
    Runtime::new(None, scylla)
        .await
        .expect("Runtime failed to start!")
        .block_on()
        .await
        .expect("Runtime failed to shutdown gracefully!")
}

fn example_scylla_node() -> std::net::SocketAddr {
    std::env::var("SCYLLA_NODE").map_or_else(
        |_| ([127, 0, 0, 1], 19042).into(),
        |n| {
            n.parse()
                .expect("Invalid SCYLLA_NODE env, use this format '127.0.0.1:19042' ")
        },
    )
}
