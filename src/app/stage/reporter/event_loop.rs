// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[async_trait::async_trait]
impl EventLoop<StageHandle> for Reporter {
    async fn event_loop(
        &mut self,
        _status: Result<(), Need>,
        supervisor: &mut Option<StageHandle>,
    ) -> Result<(), Need> {
        if let Some(supervisor) = supervisor.as_ref() {
            while let Some(event) = self.inbox.rx.recv().await {
                match event {
                    ReporterEvent::Request { worker, mut payload } => {
                        if let Some(stream) = self.streams.iter().next().cloned() {
                            // Send the event
                            match &self.sender_handle {
                                Some(sender) => {
                                    self.streams.remove(&stream);
                                    // Assign stream_id to the payload
                                    assign_stream_to_payload(stream, &mut payload);
                                    // store payload as reusable at payloads[stream]
                                    self.payloads[stream as usize].as_mut().replace(payload);
                                    self.workers.insert(stream, worker);
                                    sender.send(stream).unwrap_or_else(|e| error!("{}", e));
                                }
                                None => {
                                    // This means the sender_tx had been droped as a result of checkpoint from
                                    // receiver
                                    worker
                                        .handle_error(WorkerError::Other(anyhow!("No Sender!")), &self.handle)
                                        .unwrap_or_else(|e| error!("{}", e));
                                }
                            }
                        } else {
                            // Send overload to the worker in-case we don't have anymore streams
                            worker
                                .handle_error(WorkerError::Overload, &self.handle)
                                .unwrap_or_else(|e| error!("{}", e));
                        }
                    }
                    ReporterEvent::Response { stream_id } => {
                        self.handle_response(stream_id).unwrap_or_else(|e| error!("{}", e));
                    }
                    ReporterEvent::Err(io_error, stream_id) => {
                        self.handle_error(stream_id, WorkerError::Other(io_error))
                            .unwrap_or_else(|e| error!("{}", e));
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
                                    // do not update the status if service is_stopping
                                    if !self.service.is_stopping() {
                                        // degraded service
                                        self.service.update_status(ServiceStatus::Degraded);
                                    }
                                }
                                // update microservices
                                self.service.update_microservice(service.get_name(), service);
                                let microservices_len = self.service.microservices.len();
                                // check if all microservices are stopped
                                if self.service.microservices.values().all(|ms| ms.is_stopped())
                                    && microservices_len == 2
                                {
                                    // first we drain workers map from stucked requests, to force_consistency of
                                    // the old_session requests
                                    self.force_consistency();
                                    warn!(
                                        "address: {}, shard_id: {}, reporter_id: {}, closing session: {:?}",
                                        &self.address, self.shard_id, self.reporter_id, self.session_id
                                    );
                                    if !self.service.is_stopping() {
                                        // Maintenance service mode
                                        self.service.update_status(ServiceStatus::Maintenance);
                                    }
                                } else if self.service.microservices.values().all(|ms| ms.is_running())
                                    && microservices_len == 2
                                {
                                    if !self.service.is_stopping() {
                                        // running service
                                        self.service.update_status(ServiceStatus::Running);
                                    }
                                } else {
                                    if !self.service.is_stopping() {
                                        // degraded service
                                        self.service.update_status(ServiceStatus::Degraded);
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
                        supervisor.send(event).ok();
                    }
                }
            }
            Ok(())
        } else {
            Err(Need::Abort)
        }
    }
}

impl Reporter {
    fn handle_response(&mut self, stream: i16) -> anyhow::Result<()> {
        // push the stream_id back to streams vector.
        self.streams.insert(stream);
        // remove the worker from workers.
        if let Some(worker) = self.workers.remove(&stream) {
            if let Some(payload) = self.payloads[stream as usize].as_mut().take() {
                if is_cql_error(&payload) {
                    let error = Decoder::try_from(payload)
                        .and_then(|decoder| CqlError::new(&decoder).map(|e| WorkerError::Cql(e)))
                        .unwrap_or_else(|e| WorkerError::Other(e));
                    worker.handle_error(error, &self.handle)?;
                } else {
                    worker.handle_response(payload)?;
                }
            } else {
                error!("No payload found while handling response for stream {}!", stream);
            }
        } else {
            error!("No worker found while handling response for stream {}!", stream);
        }
        Ok(())
    }
    fn handle_error(&mut self, stream: i16, error: WorkerError) -> anyhow::Result<()> {
        // push the stream_id back to streams vector.
        self.streams.insert(stream);
        // remove the worker from workers and send error.
        if let Some(worker) = self.workers.remove(&stream) {
            // drop payload.
            if let Some(_payload) = self.payloads[stream as usize].as_mut().take() {
                worker.handle_error(error, &self.handle)?;
            } else {
                error!("No payload found while handling error for stream {}!", stream);
            }
        } else {
            error!("No worker found while handling error for stream {}!", stream);
        }
        Ok(())
    }
}
