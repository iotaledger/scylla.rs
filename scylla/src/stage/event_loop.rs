// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;
use std::time::Duration;

#[async_trait::async_trait]
impl EventLoop<NodeHandle> for Stage {
    async fn event_loop(&mut self, _status: Result<(), Need>, supervisor: &mut Option<NodeHandle>) -> Result<(), Need> {
        while let Some(event) = self.inbox.rx.recv().await {
            match event {
                StageEvent::Reporter(service) => {
                    self.service.update_microservice(service.get_name(), service);
                    let mut microservices = self.service.microservices.values();
                    if microservices.all(|ms| ms.is_initializing())
                        && microservices.len() == self.reporter_count as usize
                    {
                        if !self.service.is_stopping() {
                            self.service.update_status(ServiceStatus::Degraded);
                            // need to connect for the first time
                            let _ = self.handle.as_mut().unwrap().send(StageEvent::Connect);
                        }
                    } else if microservices.all(|ms| ms.is_maintenance()) {
                        if !self.service.is_stopping() {
                            self.service.update_status(ServiceStatus::Maintenance);
                            // need to reconnect
                            let _ = self.handle.as_mut().unwrap().send(StageEvent::Connect);
                        }
                    } else if microservices.all(|ms| ms.is_running())
                        && microservices.len() == self.reporter_count as usize
                    {
                        if !self.service.is_stopping() {
                            self.service.update_status(ServiceStatus::Running);
                        }
                    }
                    let event = NodeEvent::Service(self.service.clone());
                    let _ = supervisor.as_ref().unwrap().send(event);
                }

                StageEvent::Shutdown => {
                    self.handle = None;
                    self.service.update_status(ServiceStatus::Stopping);
                    // shutdown children
                    if let Some(reporters_handles) = self.reporters_handles.take() {
                        reporters_handles.shutdown();
                    };
                    let event = NodeEvent::Service(self.service.clone());
                    let _ = supervisor.as_ref().unwrap().send(event);
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
                        // Split the stream
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
                                    .build();
                                tokio::spawn(sender.start(self.reporters_handles.clone()));
                                // spawn receiver
                                let receiver = ReceiverBuilder::new()
                                    .socket(socket_rx)
                                    .appends_num(self.appends_num)
                                    .payloads(self.payloads.clone())
                                    .session_id(self.session_id)
                                    .buffer_size(1024000) // TODO make this configurable
                                    .build();
                                tokio::spawn(receiver.start(self.reporters_handles.clone()));
                            }
                            Err(_) => {
                                tokio::time::sleep(Duration::from_millis(5000)).await;
                                // try to reconnent
                                let _ = self.handle.as_ref().unwrap().send(StageEvent::Connect);
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }
}
