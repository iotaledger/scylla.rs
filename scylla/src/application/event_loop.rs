// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;
use futures::SinkExt;

#[async_trait::async_trait]
impl<H: ScyllaScope> EventLoop<H> for Scylla<H> {
    async fn event_loop(&mut self, _status: Result<(), Need>, supervisor: &mut Option<H>) -> Result<(), Need> {
        let my_sup = supervisor.as_mut().unwrap();
        self.service.update_status(ServiceStatus::Running);
        while let Some(event) = self.inbox.rx.recv().await {
            match event {
                ScyllaEvent::Children(scylla_child) => {
                    match scylla_child {
                        ScyllaChild::Listener(microservice) => {
                            self.service.update_microservice(microservice.get_name(), microservice);
                            // tell all the active applications about status_change
                            let socket_msg = SocketMsg::Scylla(self.service.clone());
                            self.response_to_sockets(&socket_msg).await;
                            let SocketMsg::Scylla(service) = socket_msg;
                            my_sup.status_change(service);
                        }
                        ScyllaChild::Cluster(microservice) => {
                            self.service.update_microservice(microservice.get_name(), microservice);
                            // tell all the active applications about status_change
                            let socket_msg = SocketMsg::Scylla(self.service.clone());
                            self.response_to_sockets(&socket_msg).await;
                            let SocketMsg::Scylla(service) = socket_msg;
                            my_sup.status_change(service);
                        }
                        ScyllaChild::Websocket(microservice, opt_ws_tx) => {
                            if microservice.is_initializing() {
                                // add ws_tx to websockets
                                self.websockets.insert(microservice.name, opt_ws_tx.unwrap());
                            } else if microservice.is_stopped() {
                                // remove it from websockets
                                let mut ws_tx = self.websockets.remove(&microservice.name).unwrap();
                                // make sure to close the websocket stream (optional)
                                let _ = ws_tx.close().await;
                            }
                        }
                    }
                }
                ScyllaEvent::Result(socket_msg) => {
                    // TODO add logs based on the socket_msg
                    self.response_to_sockets(&socket_msg).await;
                }
                ScyllaEvent::Abort => return Err(Need::Abort),
                ScyllaEvent::Passthrough(apps_events) => {
                    match apps_events.try_get_my_event() {
                        Ok(my_event) => {
                            match my_event {
                                ScyllaThrough::Shutdown => {
                                    // if service is_stopping do nothing
                                    if !self.service.is_stopping() {
                                        // Ask launcher to shutdown scylla application, in order to drop scylla handle.
                                        // this is usefull in case the shutdown event sent by the websocket
                                        // client.
                                        my_sup.shutdown_app(&self.get_name());
                                        // shutdown children
                                        // Listener
                                        let listener_handle = self.listener_handle.take().unwrap();
                                        listener_handle.shutdown();
                                        // shutdown cluster
                                        let cluster_handle = self.cluster_handle.take().unwrap();
                                        cluster_handle.shutdown();
                                        // Shutdown the websockets
                                        for (_, ws) in &mut self.websockets {
                                            let _ = ws.close().await;
                                        }
                                        // drop self sender/handle to enable graceful shutdown
                                        self.handle.take();
                                        // set service to stopping
                                        self.service.update_status(ServiceStatus::Stopping);
                                    }
                                }
                                ScyllaThrough::Topology(topology) => match topology {
                                    Topology::AddNode(address) => {
                                        let event = ClusterEvent::AddNode(address);
                                        let _ = self.cluster_handle.as_ref().unwrap().send(event);
                                    }
                                    Topology::RemoveNode(address) => {
                                        let event = ClusterEvent::RemoveNode(address);
                                        let _ = self.cluster_handle.as_ref().unwrap().send(event);
                                    }
                                    Topology::BuildRing(rf) => {
                                        let event = ClusterEvent::BuildRing(rf);
                                        let _ = self.cluster_handle.as_ref().unwrap().send(event);
                                    }
                                },
                            }
                        }
                        Err(apps_events) => {
                            my_sup.passthrough(apps_events, self.get_name());
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

impl<H: ScyllaScope> Scylla<H> {
    async fn response_to_sockets<T: Serialize>(&mut self, msg: &SocketMsg<T>) {
        for socket in self.websockets.values_mut() {
            let j = serde_json::to_string(&msg).unwrap();
            let m = Message::text(j);
            let _ = socket.send(m).await;
        }
    }
}
