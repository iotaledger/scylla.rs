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
                let msg = SocketMsg::Scylla(ScyllaThrough::AddNode(address));
                let j = serde_json::to_string(&msg).expect("invalid AddNode event");
                let m = Message::text(j);
                ws_stream.send(m).await.unwrap();
                // await till the node is added
                if let Some(msg) = ws_stream.next().await {
                    let event: SocketMsg<Result<Topology, Topology>> =
                        serde_json::from_str(msg.unwrap().to_text().unwrap()).unwrap();
                    if let SocketMsg::Scylla(Ok(Topology::AddingNode(_))) = event {
                    } else {
                        // TODO (handle parallel admins) it's possible other admin is managing the cluster in parallel.
                        ws_stream.close(None).await.unwrap();
                        return Err("unable to reach scylla node(s)".to_string());
                    }
                } else {
                    ws_stream.close(None).await.unwrap();
                    return Err("unable to reach the websocket server".to_string());
                };
            }
            // build the ring
            let msg = SocketMsg::Scylla(ScyllaThrough::BuildRing(uniform_rf));
            let j = serde_json::to_string(&msg).unwrap();
            let m = Message::text(j);
            ws_stream.send(m).await.unwrap();
            // await till the ring is built
            if let Some(msg) = ws_stream.next().await {
                if let SocketMsg::<Result<Topology, Topology>>::Scylla(result) =
                    serde_json::from_str(msg.unwrap().to_text().unwrap()).unwrap()
                {
                    match result {
                        Ok(Topology::BuiltRing) => info!("Succesfully Added Nodes and built cluster topology"),
                        Err(Topology::BuiltRing) => error!("Unable to build cluster topology, please try again"),
                        _ => error!("Currently we don't support concurrent admins managing the cluster simultaneously"),
                    }
                } else {
                    error!("AddNodes Client received invalid SocketMsg");
                };
            };
            // close socket and return true.
            let _ = ws_stream.close(None).await;
            Ok(())
        }
        Err(_) => Err("unable to connect the websocket server".to_string()),
    }
}
