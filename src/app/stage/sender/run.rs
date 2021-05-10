// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[async_trait::async_trait]
impl Run<ReporterEvent, ReportersHandles> for Sender {
    async fn run(&mut self, reporter_handles: &mut ReportersHandles) -> Result<(), Self::Error> {
        for reporter_handle in reporter_handles.values_mut() {
            let event = ReporterEvent::Session(Session::Service(self.service.clone()));
            reporter_handle.send(event).ok();
        }
        while let Some(stream_id) = self.inbox.recv().await {
            // write the payload to the socket, make sure the result is valid
            if let Some(payload) = self.payloads[stream_id as usize].as_ref_payload() {
                if let Err(io_error) = self.socket.write_all(payload).await {
                    // send to reporter ReporterEvent::Err(io_error, stream_id)
                    if let Some(reporter_handle) =
                        reporter_handles.get_mut(&compute_reporter_num(stream_id, self.appends_num))
                    {
                        reporter_handle
                            .send(ReporterEvent::Err(anyhow!(io_error), stream_id))
                            .unwrap_or_else(|e| error!("{}", e))
                    } else {
                        error!("No reporter found for stream {}!", stream_id);
                    }
                }
            } else {
                error!("No payload found for stream {}!", stream_id);
            }
        }
        Ok(())
    }
}
