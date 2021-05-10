// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;
use futures::SinkExt;
use tokio_tungstenite::tungstenite::Message;

#[async_trait::async_trait]
impl<E, S> Run<E, S> for Scylla
where
    S: 'static + Send + EventHandle<E>,
{
    async fn run(&mut self, supervisor: &mut S) -> Result<(), Self::Error> {
        info!("Scylla is running!");
        while let Some(event) = self.inbox.recv().await {
            match event {
                ScyllaEvent::Children(scylla_child) => {
                    match scylla_child {
                        ScyllaChild::Listener(microservice) => {
                            self.service.update_microservice(microservice);
                            // tell all the active applications about status_change
                            let socket_msg = ScyllaSocketMsg::Scylla(self.service.clone());
                            self.response_to_sockets(&socket_msg).await;
                            let ScyllaSocketMsg::Scylla(service) = socket_msg;
                            supervisor.update_status(service);
                        }
                        ScyllaChild::Cluster(microservice) => {
                            self.service.update_microservice(microservice);
                            // tell all the active applications about status_change
                            ///////// update the scylla application status //////
                            if !self.service.is_stopping() {
                                if self.service.microservices.values().all(|ms| ms.is_initializing()) {
                                    self.update_status(ServiceStatus::Initializing, supervisor);
                                } else if self.service.microservices.values().all(|ms| ms.is_maintenance()) {
                                    self.update_status(ServiceStatus::Maintenance, supervisor);
                                } else if self.service.microservices.values().all(|ms| ms.is_running()) {
                                    self.update_status(ServiceStatus::Running, supervisor);
                                } else {
                                    self.update_status(ServiceStatus::Degraded, supervisor);
                                }
                            }
                            ///////////////////////////////////////////////////////
                            let socket_msg = ScyllaSocketMsg::Scylla(self.service.clone());
                            self.response_to_sockets(&socket_msg).await;
                            let ScyllaSocketMsg::Scylla(service) = socket_msg;
                            supervisor.update_status(service);
                        }
                        ScyllaChild::Websocket(microservice, opt_ws_tx) => {
                            if microservice.is_stopped() {
                                // remove it from websockets
                                if let Some(mut ws_tx) = self.websockets.remove(&microservice.name) {
                                    // make sure to close the websocket stream (optional)
                                    ws_tx.close().await.ok();
                                }
                            } else if let Some(tx) = opt_ws_tx {
                                self.websockets.insert(microservice.name.clone(), tx);
                            }

                            self.service.update_microservice(microservice);
                            supervisor.update_status(self.service.clone());
                        }
                    }
                }
                ScyllaEvent::Result(socket_msg) => {
                    // TODO add logs based on the socket_msg
                    self.response_to_sockets(&socket_msg).await;
                    // update the stream with the service status
                    let service_socket_msg = ScyllaSocketMsg::Scylla(self.service.clone());
                    self.response_to_sockets(&service_socket_msg).await;
                }
                ScyllaEvent::Shutdown => {
                    info!("Scylla received shutdown signal!");
                    // if service is_stopping do nothing
                    if !self.service.is_stopping() {
                        // shutdown children
                        // Listener
                        if let Some(join_handle) = self.listener_data.join_handle.take() {
                            join_handle.abort();
                        }
                        // shutdown cluster
                        if let (Some(event_handle), Some(join_handle)) = (
                            self.cluster_data.event_handle.take(),
                            self.cluster_data.join_handle.take(),
                        ) {
                            event_handle.shutdown();
                            tokio::time::timeout(<Cluster as Actor<_, _>>::SHUTDOWN_TIMEOUT, join_handle).await;
                        }
                        // Shutdown the websockets
                        for (_, ws) in &mut self.websockets {
                            ws.close().await.ok();
                        }
                        break;
                    }
                }
                ScyllaEvent::Websocket(websocket_evt) => match websocket_evt {
                    ScyllaWebsocketEvent::Topology(topology) => {
                        if let Some(event_handle) = self.cluster_data.event_handle.as_mut() {
                            info!("Sending topology request to cluster!");
                            if let Err(_) = event_handle.send(topology.into()) {
                                error!("No cluster available!");
                                return Err(ScyllaError::NoCluster);
                            }
                        }
                    }
                },
            }
        }
        Ok(())
    }
}

impl Scylla {
    async fn response_to_sockets<T: Serialize>(&mut self, msg: &ScyllaSocketMsg<T>) {
        for socket in self.websockets.values_mut() {
            let j = serde_json::to_string(&msg).unwrap();
            let m = Message::text(j);
            let _ = socket.send(m).await;
        }
    }
}
