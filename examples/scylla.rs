// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0
use backstage::launcher::*;
use scylla_rs::prelude::*;

#[launcher]
pub struct Apps {
    #[Scylla]
    scylla: ScyllaBuilder,
}

#[tokio::main]
async fn main() {
    std::env::set_var("RUST_LOG", "info");
    // start the logger
    env_logger::init();
    let scylla_builder = ScyllaBuilder::new()
        .listen_address(([127, 0, 0, 1], 8080).into())
        .thread_count(num_cpus::get())
        .reporter_count(2)
        .local_dc("datacenter1".to_owned());
    // create apps_builder and build apps
    let join_handle = tokio::spawn(Apps::new(scylla_builder).launch());

    let ws = format!("ws://{}/", "127.0.0.1:8080");
    let nodes = vec![([127, 0, 0, 1], 9042).into()];
    add_nodes(&ws, nodes, 1).await.expect("unable to add nodes");

    join_handle.await.unwrap().unwrap();
}
