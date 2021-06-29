// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0
use backstage::prelude::*;
use futures::FutureExt;
use scylla_rs::prelude::*;

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
    RuntimeScope::<ActorRegistry>::launch(|scope| {
        async move {
            scope.spawn_actor_unsupervised(scylla_builder.build()).await;
            let ws = format!("ws://{}/", "127.0.0.1:8080");
            let nodes = vec![([127, 0, 0, 1], 9042).into()];
            add_nodes(&ws, nodes, 1).await.expect("unable to add nodes");
        }
        .boxed()
    })
    .await
    .unwrap();
}
