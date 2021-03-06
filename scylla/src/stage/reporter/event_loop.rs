// Copyright 2020 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[async_trait::async_trait]
impl EventLoop<StageHandle> for Reporter {
    async fn event_loop(
        &mut self,
        _status: Result<(), Need>,
        supervisor: &mut Option<StageHandle>,
    ) -> Result<(), Need> {
        self.service.update_status(ServiceStatus::Degraded);
        let event = StageEvent::Reporter(self.service.clone());
        let _ = supervisor.as_ref().unwrap().send(event);
        while let Some(event) = self.inbox.rx.recv().await {
            match event {
                ReporterEvent::Request { worker, mut payload } => {
                    if let Some(stream) = self.streams.pop() {
                        // Send the event
                        match &self.sender_handle {
                            Some(sender) => {
                                // Assign stream_id to the payload
                                assign_stream_to_payload(stream, &mut payload);
                                // store payload as reusable at payloads[stream]
                                self.payloads[stream as usize].as_mut().replace(payload);
                                sender.send(stream).unwrap();
                                self.workers.insert(stream, worker);
                            }
                            None => {
                                // return the stream_id
                                self.streams.push(stream);
                                // This means the sender_tx had been droped as a result of checkpoint from
                                // receiver
                                worker.send_error(WorkerError::Io(Error::new(ErrorKind::Other, "No Sender!")));
                            }
                        }
                    } else {
                        // Send overload to the worker in-case we don't have anymore streams
                        worker.send_error(WorkerError::Overload);
                    }
                }
                ReporterEvent::Response { stream_id } => {
                    self.handle_response(stream_id);
                }
                ReporterEvent::Err(io_error, stream_id) => {
                    self.handle_error(stream_id, WorkerError::Io(io_error));
                }
                ReporterEvent::Session(session) => {
                    match session {
                        Session::New(service, sender_handle) => {
                            self.session_id += 1;
                            self.sender_handle = Some(sender_handle);
                            // update microservice
                            self.service.update_microservice(service.get_name(), service);
                            info!(
                                "address: {}, shard_id: {}, reporter_id: {}, received session: {:?}",
                                &self.address, self.shard_id, self.reporter_id, self.session_id
                            );
                            if !self.service.is_stopping() {
                                // degraded service
                                self.service.update_status(ServiceStatus::Degraded);
                            }
                        }
                        Session::Service(service) => {
                            if service.is_stopped() {
                                // drop the sender_handle
                                self.sender_handle = None;
                            } else {
                                // do not ate the status if service is_stopping
                                if !self.service.is_stopping() {
                                    // degraded service
                                    self.service.update_status(ServiceStatus::Degraded);
                                }
                            }
                            // update microservices
                            self.service.update_microservice(service.get_name(), service);
                            let mut microservices = self.service.microservices.values();
                            // check if all microservices are stopped
                            if microservices.all(|ms| ms.is_stopped()) && microservices.len() == 2 {
                                // first we drain workers map from stucked requests, to force_consistency of
                                // the old_session requests
                                force_consistency(&mut self.streams, &mut self.workers);
                                warn!(
                                    "address: {}, shard_id: {}, reporter_id: {}, closing session: {:?}",
                                    &self.address, self.shard_id, self.reporter_id, self.session_id
                                );
                                if !self.service.is_stopping() {
                                    // Maintenance service mode
                                    self.service.update_status(ServiceStatus::Maintenance);
                                }
                            } else if microservices.all(|ms| ms.is_running()) && microservices.len() == 2 {
                                if !self.service.is_stopping() {
                                    // running service
                                    self.service.update_status(ServiceStatus::Running);
                                }
                            };
                        }
                        Session::Shutdown => {
                            // drop the sender_handle to gracefully shut it down
                            self.sender_handle = None;
                            // drop self handler, otherwise reporter never shutdown.
                            self.handle = None;
                            self.service.update_status(ServiceStatus::Stopping);
                        }
                    }
                    let event = StageEvent::Reporter(self.service.clone());
                    let _ = supervisor.as_ref().unwrap().send(event);
                }
            }
        }
        Ok(())
    }
}

impl Reporter {
    fn handle_response(&mut self, stream: i16) {
        // remove the worker from workers and send response.
        let worker = self.workers.remove(&stream).unwrap();
        worker.send_response(&self.handle, self.payloads[stream as usize].as_mut().take().unwrap());
        // push the stream_id back to streams vector.
        self.streams.push(stream);
    }
    fn handle_error(&mut self, stream: i16, error: WorkerError) {
        // remove the worker from workers and send error.
        let worker = self.workers.remove(&stream).unwrap();
        // drop payload.
        self.payloads[stream as usize].as_mut().take().unwrap();
        worker.send_error(error);
        // push the stream_id back to streams vector.
        self.streams.push(stream);
    }
}
