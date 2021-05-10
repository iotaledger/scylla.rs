// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;
use std::time::Duration;

#[async_trait::async_trait]
impl Run<NodeEvent, NodeHandle> for Stage {
    async fn run(&mut self, supervisor: &mut NodeHandle) -> Result<(), Self::Error> {
        while let Some(event) = self.inbox.recv().await {
            match event {
                StageEvent::Reporter(service) => {
                    self.service.update_microservice(service);
                    let microservices_len = self.service.microservices.len();
                    // update the service if it's not already shutting down.
                    if !self.service.is_stopping() {
                        if self.service.microservices.values().all(|ms| ms.is_initializing())
                            && microservices_len == self.reporter_count as usize
                        {
                            self.service.update_status(ServiceStatus::Degraded);
                            // need to connect for the first time
                            self.handle.send(StageEvent::Connect).ok();
                        } else if self.service.microservices.values().all(|ms| ms.is_maintenance()) {
                            self.service.update_status(ServiceStatus::Maintenance);
                            // need to reconnect
                            self.handle.send(StageEvent::Connect).ok();
                        } else if self.service.microservices.values().all(|ms| ms.is_running())
                            && microservices_len == self.reporter_count as usize
                        {
                            self.service.update_status(ServiceStatus::Running);
                        }
                    }
                    supervisor.update_status(self.service.clone());
                }

                StageEvent::Shutdown => {
                    // shutdown children
                    std::mem::take(&mut self.reporters_handles).shutdown();
                    break;
                }
                StageEvent::Connect => {
                    // ensure the service is not stopping
                    if !self.service.is_stopping() {
                        // cql connect
                        let cql_builder = CqlBuilder::new()
                            .authenticator(self.authenticator.clone())
                            .address(self.address)
                            .shard_id(self.shard_id)
                            .recv_buffer_size(self.recv_buffer_size)
                            .send_buffer_size(self.send_buffer_size)
                            .build();
                        match cql_builder.await {
                            Ok(cql_conn) => {
                                self.session_id += 1;
                                // Split the stream
                                let stream: TcpStream = cql_conn.into();
                                let (socket_rx, socket_tx) = stream.into_split();
                                // spawn sender
                                let sender = SenderBuilder::new()
                                    .socket(socket_tx)
                                    .appends_num(self.appends_num)
                                    .payloads(self.payloads.clone())
                                    .build(self.service.spawn("Sender"));
                                tokio::spawn(sender.start(self.reporters_handles.clone()));
                                // spawn receiver
                                let receiver = ReceiverBuilder::new()
                                    .socket(socket_rx)
                                    .appends_num(self.appends_num)
                                    .payloads(self.payloads.clone())
                                    .session_id(self.session_id)
                                    .buffer_size(self.buffer_size)
                                    .build(self.service.spawn("Receiver"));
                                tokio::spawn(receiver.start(self.reporters_handles.clone()));
                            }
                            Err(_) => {
                                tokio::time::sleep(Duration::from_millis(5000)).await;
                                // try to reconnent
                                self.handle.send(StageEvent::Connect).ok();
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }
}
