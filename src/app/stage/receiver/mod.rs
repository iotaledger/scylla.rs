// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::{reporter::*, *};
use anyhow::anyhow;
use async_recursion::async_recursion;
use futures::TryFutureExt;
use tokio::{io::AsyncReadExt, net::tcp::OwnedReadHalf};

const CQL_FRAME_HEADER_BYTES_LENGTH: usize = 9;

/// Receiver state
pub struct Receiver {
    socket: OwnedReadHalf,
    stream_id: i16,
    total_length: usize,
    current_length: usize,
    header: bool,
    buffer: Vec<u8>,
    i: usize,
    appends_num: i16,
    payloads: Payloads,
}

#[build]
pub fn build_receiver(
    socket: OwnedReadHalf,
    session_id: usize,
    payloads: Payloads,
    buffer_size: usize,
    appends_num: i16,
) -> Receiver {
    Receiver {
        socket,
        stream_id: 0,
        total_length: 0,
        current_length: 0,
        header: false,
        buffer: vec![0; buffer_size],
        i: 0,
        appends_num,
        payloads,
    }
}

#[async_trait]
impl Actor for Receiver {
    type Dependencies = (Pool<Reporter>, Res<Arc<RwLock<HashMap<u8, Act<Reporter>>>>>);
    type Event = ();
    type Channel = TokioChannel<()>;

    async fn run<'a, Reg: RegistryAccess + Send + Sync>(
        &mut self,
        rt: &mut ActorScopedRuntime<'a, Self, Reg>,
        (reporter_pool, reporter_handles): Self::Dependencies,
    ) -> Result<(), ActorError>
    where
        Self: Sized,
    {
        rt.update_status(ServiceStatus::Running).await;
        while let Ok(n) = self.socket.read(&mut self.buffer[self.i..]).await {
            if n != 0 {
                self.current_length += n;
                if self.current_length < CQL_FRAME_HEADER_BYTES_LENGTH {
                    self.i = self.current_length;
                } else {
                    self.handle_frame_header(0)
                        .map_err(|e| {
                            error!("{}", e);
                            e
                        })
                        .await?;
                    self.handle_frame(n, 0, &reporter_handles)
                        .map_err(|e| {
                            error!("{}", e);
                            e
                        })
                        .await?;
                }
            } else {
                break;
            }
        }
        rt.update_status(ServiceStatus::Stopped).await;
        Ok(())
    }
}

impl Receiver {
    async fn handle_remaining_buffer(
        &mut self,
        i: usize,
        reporters_handles: &Res<Arc<RwLock<HashMap<u8, Act<Reporter>>>>>,
    ) -> anyhow::Result<()> {
        if self.current_length < CQL_FRAME_HEADER_BYTES_LENGTH {
            self.buffer.copy_within(i..(i + self.current_length), self.i);
            self.i = self.current_length;
        } else {
            self.handle_frame_header(i).await?;
            self.handle_frame(self.current_length, i, reporters_handles).await?;
        }
        Ok(())
    }

    async fn handle_frame_header(&mut self, padding: usize) -> anyhow::Result<()> {
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

    #[async_recursion]
    async fn handle_frame(
        &mut self,
        n: usize,
        mut padding: usize,
        reporters_handles: &Res<Arc<RwLock<HashMap<u8, Act<Reporter>>>>>,
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
            let mut handles = reporters_handles.write().await;
            let reporter_handle = handles
                .get_mut(&compute_reporter_num(self.stream_id, self.appends_num))
                .ok_or_else(|| anyhow!("No reporter handle for stream {}!", self.stream_id))?;

            reporter_handle
                .send(ReporterEvent::Response {
                    stream_id: self.stream_id,
                })
                .await
                .unwrap_or_else(|e| error!("{}", e));
            // set header to false
            self.header = false;
            // update current_length
            self.current_length -= self.total_length;
            // set self.i to zero
            self.i = 0;
            self.handle_remaining_buffer(padding, reporters_handles).await?;
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
