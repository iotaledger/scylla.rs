// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

#[cfg(feature = "app")]
use scylla_rs::prelude::*;

#[tokio::main]
async fn main() {
    env_logger::init();
    let scylla = Scylla::default();
    let runtime = Runtime::new(None, scylla)
        .await
        .expect("runtime to run")
        .backserver("127.0.0.1:10000".parse().unwrap())
        .await
        .expect("backserver to run");
    let cluster_handle = runtime
        .handle()
        .cluster_handle()
        .await
        .expect("running scylla application");
    cluster_handle
        .add_node(example_scylla_node())
        .await
        .expect("to add node");
    cluster_handle.build_ring(1).await.expect("to build ring");
    runtime.block_on().await.expect("runtime to gracefully shutdown")
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
