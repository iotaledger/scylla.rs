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
        .expect("Runtime failed to start!")
        .backserver("127.0.0.1:10000".parse().unwrap())
        .await
        .expect("Failed to start backserver!");
    backstage::spawn_task("adding node task", ws_client());
    runtime
        .block_on()
        .await
        .expect("Runtime failed to shutdown gracefully!")
}

async fn ws_client() {
    use backstage::prefab::websocket::*;
    use futures::SinkExt;
    use scylla_rs::app::cluster::Topology;
    let (mut stream, _) = tokio_tungstenite::connect_async(url::Url::parse("ws://127.0.0.1:10000/").unwrap())
        .await
        .unwrap();
    let actor_path = ActorPath::new().push("cluster".into());
    let add_node_event = Topology::AddNode("172.17.0.2:19042".parse().unwrap());
    let add_node_json = serde_json::to_string(&add_node_event).expect("Failed to serialize Add Node event!");
    let request = Interface::new(actor_path.clone(), Event::Call(add_node_json.into()));
    stream.send(request.to_message()).await.unwrap();
    let build_ring_event = Topology::BuildRing;
    let build_ring_json = serde_json::to_string(&build_ring_event).expect("Failed to serialize Build Node event!");
    let request = Interface::new(actor_path.clone(), Event::Call(build_ring_json.into()));
    stream.send(request.to_message()).await.unwrap();
    while let Some(Ok(msg)) = stream.next().await {
        log::info!("Response from websocket: {}", msg);
    }
}
