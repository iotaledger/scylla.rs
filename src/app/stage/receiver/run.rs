// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[async_trait::async_trait]
impl Run<ReporterEvent, ReportersHandles> for Receiver {
    async fn run(&mut self, reporter_handles: &mut ReportersHandles) -> Result<(), Self::Error> {
        for reporter_handle in reporter_handles.values_mut() {
            let event = ReporterEvent::Session(Session::Service(self.service.clone()));
            reporter_handle.send(event).ok();
        }
        while let Ok(n) = self.socket.read(&mut self.buffer[self.i..]).await {
            if n != 0 {
                self.current_length += n;
                if self.current_length < CQL_FRAME_HEADER_BYTES_LENGTH {
                    self.i = self.current_length;
                } else {
                    self.handle_frame_header(0)
                        .and_then(|_| self.handle_frame(n, 0, reporter_handles))
                        .map_err(|e| {
                            error!("{}", e);
                            format!("{}", e)
                        })?;
                }
            } else {
                break;
            }
        }
        Ok(())
    }
}

impl Receiver {
    fn handle_remaining_buffer(&mut self, i: usize, reporters_handles: &mut ReportersHandles) -> anyhow::Result<()> {
        if self.current_length < CQL_FRAME_HEADER_BYTES_LENGTH {
            self.buffer.copy_within(i..(i + self.current_length), self.i);
            self.i = self.current_length;
        } else {
            self.handle_frame_header(i)
                .and_then(|_| self.handle_frame(self.current_length, i, reporters_handles))?;
        }
        Ok(())
    }
    fn handle_frame_header(&mut self, padding: usize) -> anyhow::Result<()> {
        // if no-header decode the header and resize the payload(if needed).
        if !self.header {
            // decode total_length(HEADER_LENGTH + frame_body_length)
            let buf = &self.buffer[padding..];
            self.total_length = get_total_length_usize(&buf);
            // decode stream_id
            self.stream_id = get_stream_id(&buf);
            // get mut ref to payload for stream_id
            let payload = self.payloads[self.stream_id as usize]
                .as_mut_payload()
                .ok_or_else(|| anyhow!("No payload for stream {}!", self.stream_id))?;
            // resize payload only if total_length is larger than the payload length
            if self.total_length > payload.len() {
                // resize the len of the payload.
                payload.resize(self.total_length, 0);
            }
            // set header to true
            self.header = true;
        }
        Ok(())
    }
    fn handle_frame(
        &mut self,
        n: usize,
        mut padding: usize,
        reporters_handles: &mut ReportersHandles,
    ) -> anyhow::Result<()> {
        let start = self.current_length - n - self.i;
        if self.current_length >= self.total_length {
            // get mut ref to payload for stream_id as giveload
            let giveload = self.payloads[self.stream_id as usize]
                .as_mut_payload()
                .ok_or_else(|| anyhow!("No payload for stream {}!", self.stream_id))?;
            // memcpy the current bytes from self.buffer into payload
            let old_padding = padding;
            // update padding
            padding += self.total_length - start;
            giveload[start..self.total_length].copy_from_slice(&self.buffer[old_padding..padding]);
            // tell reporter that giveload is ready.
            let reporter_handle = reporters_handles
                .get_mut(&compute_reporter_num(self.stream_id, self.appends_num))
                .ok_or_else(|| anyhow!("No reporter handle for stream {}!", self.stream_id))?;

            reporter_handle
                .send(ReporterEvent::Response {
                    stream_id: self.stream_id,
                })
                .unwrap_or_else(|e| error!("{}", e));
            // set header to false
            self.header = false;
            // update current_length
            self.current_length -= self.total_length;
            // set self.i to zero
            self.i = 0;
            self.handle_remaining_buffer(padding, reporters_handles)?;
        } else {
            // get mut ref to payload for stream_id
            let payload = self.payloads[self.stream_id as usize]
                .as_mut_payload()
                .ok_or_else(|| anyhow!("No payload for stream {}!", self.stream_id))?;
            // memcpy the current bytes from self.buffer into payload
            payload[start..self.current_length].copy_from_slice(&self.buffer[padding..(padding + n + self.i)]);
            // set self.i to zero
            self.i = 0;
        }
        Ok(())
    }
}
