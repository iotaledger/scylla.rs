// Copyright 2020 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[async_trait::async_trait]
impl EventLoop<ReportersHandles> for Sender {
    async fn event_loop(
        &mut self,
        _status: Result<(), Need>,
        supervisor: &mut Option<ReportersHandles>,
    ) -> Result<(), Need> {
        self.service.update_status(ServiceStatus::Running);
        let reporters_handles = supervisor.as_ref().unwrap();
        for (_, reporter_handle) in reporters_handles {
            let event = ReporterEvent::Session(Session::Service(self.service.clone()));
            let _ = reporter_handle.send(event);
        }
        while let Some(stream_id) = self.inbox.rx.recv().await {
            // write the payload to the socket, make sure the result is valid
            if let Err(io_error) = self
                .socket
                .write_all(self.payloads[stream_id as usize].as_ref_payload().unwrap())
                .await
            {
                // send to reporter ReporterEvent::Err(io_error, stream_id)
                let _ = reporters_handles
                    .get(&compute_reporter_num(stream_id, self.appends_num))
                    .unwrap()
                    .send(ReporterEvent::Err(io_error, stream_id));
            }
        }
        Ok(())
    }
}
