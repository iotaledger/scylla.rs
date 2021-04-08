// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use anyhow::bail;

use super::*;

#[allow(irrefutable_let_patterns)]
/// Add new ScyllaDB nodes.
pub async fn add_nodes(ws: &str, addresses: Vec<SocketAddr>, uniform_rf: u8) -> anyhow::Result<()> {
    let request = Url::parse(ws)?;
    // connect to dashboard
    let (mut ws_stream, _) = connect_async(request).await?;
    // add scylla nodes
    for address in addresses {
        // add node
        let msg = SocketMsg::Scylla(ScyllaThrough::Topology(Topology::AddNode(address)));
        let j = serde_json::to_string(&msg).expect("invalid AddNode event");
        let m = Message::text(j);
        ws_stream.send(m).await?;
        // await till the node is added
        while let Some(msg) = ws_stream.next().await {
            if let Ok(event) = serde_json::from_str::<SocketMsg<Result<Topology, Topology>>>(msg?.to_text()?) {
                if let SocketMsg::Scylla(Ok(Topology::AddNode(_))) = event {
                    info!("Added scylla node: {}", address);
                    break;
                } else {
                    // TODO (handle parallel admins) it's possible other admin is managing the cluster in
                    // parallel.
                    ws_stream.close(None).await?;
                    bail!("unable to reach scylla node(s)");
                }
            }
        }
    }
    // build the ring
    let msg = SocketMsg::Scylla(ScyllaThrough::Topology(Topology::BuildRing(uniform_rf)));
    let j = serde_json::to_string(&msg)?;
    let m = Message::text(j);
    ws_stream.send(m).await?;
    // await till the ring is built
    while let Some(msg) = ws_stream.next().await {
        if let Ok(event) = serde_json::from_str::<SocketMsg<Result<Topology, Topology>>>(msg?.to_text()?) {
            if let SocketMsg::<Result<Topology, Topology>>::Scylla(result) = event {
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
    }
    // close socket and return success
    ws_stream.close(None).await?;
    Ok(())
}
