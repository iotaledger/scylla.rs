// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;
use anyhow::{anyhow, bail};
use std::net::SocketAddr;
use tokio_tungstenite::{connect_async, tungstenite::Message};

#[allow(irrefutable_let_patterns)]
/// Add new ScyllaDB nodes.
pub async fn add_nodes(ws: &str, addresses: Vec<SocketAddr>, uniform_rf: u8) -> anyhow::Result<()> {
    let request = Url::parse(ws)?;
    // connect to dashboard
    let (mut ws_stream, _) = connect_async(request).await?;
    // add scylla nodes
    for address in addresses {
        // add node
        let msg = ScyllaWebsocketEvent::Topology(Topology::AddNode(address));
        let j = serde_json::to_string(&msg).map_err(|_| anyhow!("Invalid AddNode event"))?;
        let m = Message::text(j);
        ws_stream.send(m).await?;
        // await till the node is added
        while let Some(msg) = ws_stream.next().await {
            let msg = msg.map_err(|_| anyhow!("Expected message from the WebSocketStream while building a ring"))?;
            let msg = msg.to_text()?;
            if let Ok(event) = serde_json::from_str::<Result<Topology, Topology>>(msg) {
                if let Ok(Topology::AddNode(_)) = event {
                    info!("Added scylla node: {}", address);
                    break;
                } else {
                    // TODO (handle parallel admins) it's possible other admin is managing the cluster in
                    // parallel.
                    ws_stream.close(None).await?;
                    bail!("Unable to reach scylla node(s)");
                }
            }
        }
    }
    // build the ring
    let msg = ScyllaWebsocketEvent::Topology(Topology::BuildRing(uniform_rf));
    let j = serde_json::to_string(&msg)?;
    let m = Message::text(j);
    ws_stream.send(m).await?;
    while let Some(msg) = ws_stream.next().await {
        let msg = msg.map_err(|_| anyhow!("Expected message from the WebSocketStream while building a ring"))?;
        let msg = msg.to_text()?;
        if let Ok(result) = serde_json::from_str::<Result<Topology, Topology>>(msg) {
            match result {
                Ok(Topology::BuildRing(_)) => {
                    info!("Succesfully Added Nodes and built cluster topology");
                    break;
                }
                Err(Topology::BuildRing(_)) => {
                    error!("Unable to build cluster topology, please try again");
                    break;
                }
                _ => {
                    error!("Currently we don't support concurrent admins managing the cluster simultaneously");
                    break;
                }
            }
        }
    }
    // close socket and return success
    ws_stream.close(None).await?;
    Ok(())
}
