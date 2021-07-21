// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::{
    cluster::{Cluster, ClusterEvent},
    *,
};
use backstage::prefabs::websocket::WebsocketChildren;
use futures::{SinkExt, StreamExt};
use std::net::SocketAddr;
use tokio_tungstenite::tungstenite::Message;
use url::Url;

pub(crate) mod add_nodes;

pub struct Websocket {
    pub listen_address: SocketAddr,
}

#[async_trait]
impl Actor for Websocket {
    type Dependencies = Act<Cluster>;
    type Event = WebsocketRequest;
    type Channel = UnboundedTokioChannel<Self::Event>;

    async fn init<Reg: RegistryAccess + Send + Sync, Sup: EventDriven>(
        &mut self,
        rt: &mut ActorScopedRuntime<Self, Reg, Sup>,
    ) -> Result<(), ActorError>
    where
        Self: Sized,
        Sup::Event: SupervisorEvent,
        <Sup::Event as SupervisorEvent>::Children: From<PhantomData<Self>>,
    {
        rt.update_status(ServiceStatus::Initializing).await.ok();
        let my_handle = rt.handle();
        let websocket = backstage::prefabs::websocket::WebsocketBuilder::new()
            .listen_address(self.listen_address)
            .supervisor_handle(my_handle)
            .build();
        rt.spawn_actor_unsupervised(websocket).await?;
        Ok(())
    }

    async fn run<Reg: RegistryAccess + Send + Sync, Sup: EventDriven>(
        &mut self,
        rt: &mut ActorScopedRuntime<Self, Reg, Sup>,
        cluster: Self::Dependencies,
    ) -> Result<(), ActorError>
    where
        Self: Sized,
        Sup::Event: SupervisorEvent,
        <Sup::Event as SupervisorEvent>::Children: From<PhantomData<Self>>,
    {
        let websocket = rt
            .actor_event_handle::<backstage::prefabs::websocket::Websocket<Self>>()
            .await
            .ok_or_else(|| anyhow::anyhow!("No websocket!"))?;
        rt.update_status(ServiceStatus::Running).await.ok();
        while let Some(WebsocketRequest(addr, msg)) = rt.next_event().await {
            debug!("Received message {} from {}", msg, addr);
            if let Some(msg) = {
                if let Message::Text(t) = msg {
                    serde_json::from_str::<ScyllaWebsocketEvent>(&t).ok()
                } else {
                    None
                }
            } {
                let (sender, receiver) = tokio::sync::oneshot::channel();
                match msg {
                    ScyllaWebsocketEvent::Topology(t) => match t {
                        Topology::AddNode(addr) => {
                            cluster.send(ClusterEvent::AddNode(addr, Some(sender))).ok();
                        }
                        Topology::RemoveNode(addr) => {
                            cluster.send(ClusterEvent::RemoveNode(addr, Some(sender))).ok();
                        }
                        Topology::BuildRing(uniform_rf) => {
                            cluster.send(ClusterEvent::BuildRing(uniform_rf, Some(sender))).ok();
                        }
                    },
                }
                // This will wait for the response before processing the next websocket event
                if let Ok(res) = receiver.await {
                    websocket
                        .send(WebsocketChildren::Response(
                            addr,
                            serde_json::to_string(&res).unwrap().into(),
                        ))
                        .map_err(|e| anyhow::anyhow!("Websocket error: {}", e))?;
                }
            }
        }
        rt.update_status(ServiceStatus::Stopped).await.ok();
        Ok(())
    }
}

pub struct WebsocketRequest(SocketAddr, Message);

#[derive(Serialize, Deserialize, Debug)]
pub enum ScyllaWebsocketEvent {
    Topology(Topology),
}

#[derive(Deserialize, Serialize, Debug)]
/// Topology event
pub enum Topology {
    /// AddNode json to add new scylla node
    AddNode(SocketAddr),
    /// RemoveNode json to remove an existing scylla node
    RemoveNode(SocketAddr),
    /// BuildRing json to re/build the cluster topology,
    /// Current limitation: for now the admin supposed to define uniform replication factor among all DataCenter and
    /// all keyspaces
    BuildRing(u8),
}

impl From<(SocketAddr, Message)> for WebsocketRequest {
    fn from((addr, msg): (SocketAddr, Message)) -> Self {
        WebsocketRequest(addr, msg)
    }
}
