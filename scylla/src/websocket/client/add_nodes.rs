// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[allow(irrefutable_let_patterns)]
/// Add new ScyllaDB nodes.
pub async fn add_nodes(ws: &str, addresses: Vec<SocketAddr>, uniform_rf: u8) -> Result<(), String> {
    let request = Url::parse(ws).unwrap();
    // connect to dashboard
    match connect_async(request).await {
        Ok((mut ws_stream, _)) => {
            // add scylla nodes
            for address in addresses {
                // add node
                let msg = SocketMsg::Scylla(ScyllaThrough::Topology(Topology::AddNode(address)));
                let j = serde_json::to_string(&msg).expect("invalid AddNode event");
                let m = Message::text(j);
                ws_stream.send(m).await.unwrap();
                // await till the node is added
                while let Some(msg) = ws_stream.next().await {
                    let msg = msg.expect("Expected message from the WebSocketStream while adding a node");
                    let msg = msg.to_text().unwrap();
                    if let Ok(event) = serde_json::from_str::<SocketMsg<Result<Topology, Topology>>>(msg) {
                        if let SocketMsg::Scylla(Ok(Topology::AddNode(_))) = event {
                            info!("Added scylla node: {}", address);
                            break;
                        } else {
                            // TODO (handle parallel admins) it's possible other admin is managing the cluster in
                            // parallel.
                            ws_stream.close(None).await.unwrap();
                            return Err("unable to reach scylla node(s)".to_string());
                        }
                    } else {
                        // ensure it's running
                    }
                }
            }
            // build the ring
            let msg = SocketMsg::Scylla(ScyllaThrough::Topology(Topology::BuildRing(uniform_rf)));
            let j = serde_json::to_string(&msg).unwrap();
            let m = Message::text(j);
            ws_stream.send(m).await.unwrap();
            // await till the ring is built
            let mut break_once_ready: bool = false;
            while let Some(msg) = ws_stream.next().await {
                let msg = msg.expect("Expected message from the WebSocketStream while building a ring");
                let msg = msg.to_text().unwrap();
                if let Ok(event) = serde_json::from_str::<SocketMsg<Result<Topology, Topology>>>(msg) {
                    if let SocketMsg::<Result<Topology, Topology>>::Scylla(result) = event {
                        match result {
                            Ok(Topology::BuildRing(_)) => {
                                info!("Succesfully Added Nodes and built cluster topology");
                                break_once_ready = true;
                            }
                            Err(Topology::BuildRing(_)) => {
                                error!("Unable to build cluster topology, please try again");
                                break_once_ready = true;
                            }
                            _ => {
                                error!(
                                    "Currently we don't support concurrent admins managing the cluster simultaneously"
                                );
                                break;
                            }
                        }
                    }
                } else if let Ok(SocketMsg::Scylla(service)) = serde_json::from_str::<SocketMsg<Service>>(msg) {
                    if break_once_ready && service.is_running() {
                        info!("All Nodes in Scylla Cluster are connected");
                        break;
                    }
                }
            }
            // close socket and return true.
            let _ = ws_stream.close(None).await;
            Ok(())
        }
        Err(_) => Err("unable to connect the websocket server".to_string()),
    }
}
