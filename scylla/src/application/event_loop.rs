// Copyright 2020 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;
use futures::SinkExt;

#[async_trait::async_trait]
impl<H: ScyllaScope> EventLoop<H> for Scylla<H> {
    async fn event_loop(&mut self, status: Result<(), Need>, supervisor: &mut Option<H>) -> Result<(), Need> {
        let my_sup = supervisor.as_mut().unwrap();
        self.service.update_status(ServiceStatus::Running);
        while let Some(event) = self.inbox.rx.recv().await {
            match event {
                ScyllaEvent::Children(scylla_child) => {
                    match scylla_child {
                        ScyllaChild::Listener(microservice, _) => {
                            self.service.update_microservice(microservice.get_name(), microservice);
                            // tell all the active applications about status_change
                            my_sup.status_change(self.service.clone());
                        }
                        ScyllaChild::Cluster(microservice, _) => {
                            self.service.update_microservice(microservice.get_name(), microservice);
                            // tell all the active applications about status_change
                            my_sup.status_change(self.service.clone());
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
                ScyllaEvent::Passthrough(apps_events) => {
                    match apps_events.try_get_my_event() {
                        Ok(my_event) => {
                            match my_event {
                                Shutdown => {
                                    // if service is_stopping do nothing
                                    if !self.service.is_stopping() {
                                        // Ask launcher to shutdown scylla application,
                                        // this is usefull in case the shutdown event sent by the websocket
                                        // client.
                                        my_sup.shutdown_app(&self.get_name());
                                        // shutdown children
                                        // Listener
                                        let listener_handle = self.listener_handle.take().unwrap();
                                        listener_handle.shutdown();
                                        // TODO shutdown cluster

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
                                _ => {
                                    // TODO Add node, remove node, build ring
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
    }
}
