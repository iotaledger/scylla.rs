// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;
use futures::SinkExt;

#[async_trait::async_trait]
impl<H: ScyllaScope> EventLoop<H> for Scylla<H> {
    async fn event_loop(&mut self, _status: Result<(), Need>, supervisor: &mut Option<H>) -> Result<(), Need> {
        if let Some(my_sup) = supervisor.as_mut() {
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
                                ///////// update the scylla application status //////
                                if !self.service.is_stopping() {
                                    if self.service.microservices.values().all(|ms| ms.is_initializing()) {
                                        self.service.update_status(ServiceStatus::Initializing);
                                    } else if self.service.microservices.values().all(|ms| ms.is_maintenance()) {
                                        self.service.update_status(ServiceStatus::Maintenance);
                                    } else if self.service.microservices.values().all(|ms| ms.is_running()) {
                                        self.service.update_status(ServiceStatus::Running);
                                    } else {
                                        self.service.update_status(ServiceStatus::Degraded);
                                    }
                                }
                                ///////////////////////////////////////////////////////
                                let socket_msg = SocketMsg::Scylla(self.service.clone());
                                self.response_to_sockets(&socket_msg).await;
                                let SocketMsg::Scylla(service) = socket_msg;
                                my_sup.status_change(service);
                            }
                            ScyllaChild::Websocket(microservice, opt_ws_tx) => {
                                if microservice.is_initializing() {
                                    // add ws_tx to websockets
                                    if let Some(tx) = opt_ws_tx {
                                        self.websockets.insert(microservice.name, tx);
                                    }
                                } else if microservice.is_stopped() {
                                    // remove it from websockets
                                    if let Some(mut ws_tx) = self.websockets.remove(&microservice.name) {
                                        // make sure to close the websocket stream (optional)
                                        ws_tx.close().await.ok();
                                    }
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
                                            // Ask launcher to shutdown scylla application, in order to drop scylla
                                            // handle. this is usefull in
                                            // case the shutdown event sent by the websocket
                                            // client.
                                            my_sup.shutdown_app(&self.get_name());
                                            // shutdown children
                                            // Listener
                                            if let Some(listener_handle) = self.listener_handle.take() {
                                                listener_handle.shutdown();
                                            }
                                            // shutdown cluster
                                            if let Some(cluster_handle) = self.cluster_handle.take() {
                                                cluster_handle.shutdown();
                                            }
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
                                    ScyllaThrough::Topology(topology) => {
                                        if let Some(cluster) = self.cluster_handle.as_ref() {
                                            if let Err(_) = cluster.send(topology.into()) {
                                                error!("No cluster available!");
                                                return Err(Need::Abort);
                                            }
                                        }
                                    }
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
        } else {
            Err(Need::Abort)
        }
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
