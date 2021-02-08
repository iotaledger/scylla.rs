// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

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
                }
                StageEvent::Connect => {
                    // cql connect // TODO replace it with cqlbuilder and make use of PasswordAuth
                    let mut cql_builder = Cql::new().address(self.address).shard_id(self.shard_id);
                    // Split the stream
                    if let Some(recv_buf) = self.recv_buffer_size {
                        cql_builder = cql_builder.recv_buffer_size(recv_buf);
                    }
                    if let Some(send_buf) = self.send_buffer_size {
                        cql_builder = cql_builder.send_buffer_size(send_buf);
                    }
                    match cql_builder.build().await {
                        Ok(mut cql_conn) => {
                            self.session_id += 1;
                            
                        }
                        Err(_) => {

                        }
                    }

                    // let (socket_rx, socket_tx) = tokio::io::split(cqlconn.take_stream());
                }
            }
        }
        Ok(())
    }
}
