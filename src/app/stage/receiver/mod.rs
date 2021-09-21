// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::{
    reporter::*,
    *,
};
use anyhow::anyhow;
use backstage::core::{
    Actor,
    ActorError,
    ActorResult,
    IoChannel,
    Rt,
    StreamExt,
    SupHandle,
    UnboundedHandle,
};
use tokio::{
    io::AsyncReadExt,
    net::tcp::OwnedReadHalf,
};

const CQL_FRAME_HEADER_BYTES_LENGTH: usize = 9;

/// Receiver state
pub(super) struct Receiver {
    stream_id: i16,
    total_length: usize,
    current_length: usize,
    header: bool,
    buffer: Vec<u8>,
    i: usize,
    appends_num: i16,
}

impl Receiver {
    pub(super) fn new(buffer_size: Option<usize>, appends_num: i16) -> Self {
        Self {
            stream_id: 0,
            total_length: 0,
            current_length: 0,
            header: false,
            buffer: vec![0; buffer_size.unwrap_or(1024000)],
            i: 0,
            appends_num,
        }
    }
}

/// The Receiver actor lifecycle implementation
#[async_trait]
impl<S> Actor<S> for Receiver
where
    S: SupHandle<Self>,
{
    type Data = (Payloads, ReportersHandles);
    type Channel = IoChannel<OwnedReadHalf>;
    async fn init(&mut self, rt: &mut Rt<Self, S>) -> ActorResult<Self::Data> {
        let parent_id = rt
            .parent_id()
            .ok_or_else(|| ActorError::exit_msg("receiver without stage supervisor"))?;
        let payloads = rt
            .lookup(parent_id)
            .await
            .ok_or_else(|| ActorError::exit_msg("receiver unables to lookup for payloads"))?;
        let reporters_handles = rt.depends_on(parent_id).await.map_err(ActorError::exit)?;
        Ok((payloads, reporters_handles))
    }
    async fn run(&mut self, rt: &mut Rt<Self, S>, (mut payloads, reporters_handles): Self::Data) -> ActorResult<()> {
        while let Ok(n) = rt.inbox_mut().read(&mut self.buffer[self.i..]).await {
            if n != 0 {
                self.current_length += n;
                if self.current_length < CQL_FRAME_HEADER_BYTES_LENGTH {
                    self.i = self.current_length;
                } else {
                    self.handle_frame_header(0, &mut payloads)
                        .and_then(|_| self.handle_frame(n, 0, &mut payloads, &reporters_handles))
                        .map_err(|e| {
                            log::error!("{}", e);
                            ActorError::exit(e)
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
    fn handle_remaining_buffer(
        &mut self,
        i: usize,
        payloads: &mut Payloads,
        reporters_handles: &ReportersHandles,
    ) -> anyhow::Result<()> {
        if self.current_length < CQL_FRAME_HEADER_BYTES_LENGTH {
            self.buffer.copy_within(i..(i + self.current_length), self.i);
            self.i = self.current_length;
        } else {
            self.handle_frame_header(i, payloads)
                .and_then(|_| self.handle_frame(self.current_length, i, payloads, reporters_handles))?;
        }
        Ok(())
    }
    fn handle_frame_header(&mut self, padding: usize, payloads: &mut Payloads) -> anyhow::Result<()> {
        // if no-header decode the header and resize the payload(if needed).
        if !self.header {
            // decode total_length(HEADER_LENGTH + frame_body_length)
            let buf = &self.buffer[padding..];
            self.total_length = get_total_length_usize(&buf);
            // decode stream_id
            self.stream_id = get_stream_id(&buf);
            // get mut ref to payload for stream_id
            let payload = payloads[self.stream_id as usize]
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
        payloads: &mut Payloads,
        reporters_handles: &ReportersHandles,
    ) -> anyhow::Result<()> {
        let start = self.current_length - n - self.i;
        if self.current_length >= self.total_length {
            // get mut ref to payload for stream_id as giveload
            let giveload = payloads[self.stream_id as usize]
                .as_mut_payload()
                .ok_or_else(|| anyhow!("No payload for stream {}!", self.stream_id))?;
            // memcpy the current bytes from self.buffer into payload
            let old_padding = padding;
            // update padding
            padding += self.total_length - start;
            giveload[start..self.total_length].copy_from_slice(&self.buffer[old_padding..padding]);
            // tell reporter that giveload is ready.
            let reporter_handle = reporters_handles
                .get(&compute_reporter_num(self.stream_id, self.appends_num))
                .ok_or_else(|| anyhow!("No reporter handle for stream {}!", self.stream_id))?;

            reporter_handle
                .send(ReporterEvent::Response {
                    stream_id: self.stream_id,
                })
                .unwrap_or_else(|e| log::error!("{}", e));
            // set header to false
            self.header = false;
            // update current_length
            self.current_length -= self.total_length;
            // set self.i to zero
            self.i = 0;
            self.handle_remaining_buffer(padding, payloads, reporters_handles)?;
        } else {
            // get mut ref to payload for stream_id
            let payload = payloads[self.stream_id as usize]
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

fn get_total_length_usize(buffer: &[u8]) -> usize {
    CQL_FRAME_HEADER_BYTES_LENGTH +
    // plus body length
    ((buffer[5] as usize) << 24) +
    ((buffer[6] as usize) << 16) +
    ((buffer[7] as usize) <<  8) +
    (buffer[8] as usize)
}

fn get_stream_id(buffer: &[u8]) -> i16 {
    ((buffer[2] as i16) << 8) | buffer[3] as i16
}
