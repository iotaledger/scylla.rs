// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::{
    compute_reporter_num,
    reporter::ReporterEvent,
    Payloads,
    ReportersHandles,
};
use anyhow::anyhow;
use async_trait::async_trait;
use backstage::core::{
    AbortableUnboundedChannel,
    Actor,
    ActorError,
    ActorResult,
    Rt,
    StreamExt,
    SupHandle,
};
use tokio::{
    io::AsyncWriteExt,
    net::tcp::OwnedWriteHalf,
};
/// Sender state
pub struct Sender {
    socket: OwnedWriteHalf,
    appends_num: u16,
}

impl Sender {
    pub(super) fn new(split_sink_owned: OwnedWriteHalf, appends_num: u16) -> Self {
        Self {
            socket: split_sink_owned,
            appends_num,
        }
    }
}

/// The sender's event type
pub(super) type SenderEvent = u16;

/// The Sender actor lifecycle implementation
#[async_trait]
impl<S> Actor<S> for Sender
where
    S: SupHandle<Self>,
{
    type Data = (Payloads, ReportersHandles);
    type Channel = AbortableUnboundedChannel<SenderEvent>;
    async fn init(&mut self, rt: &mut Rt<Self, S>) -> ActorResult<Self::Data> {
        let parent_id = rt
            .parent_id()
            .ok_or_else(|| ActorError::exit_msg("sender without stage supervisor"))?;
        let payloads = rt
            .lookup(parent_id)
            .await
            .ok_or_else(|| ActorError::exit_msg("sender unables to lookup for payloads"))?;
        let reporters_handles = rt.depends_on(parent_id).await.map_err(ActorError::exit)?;
        Ok((payloads, reporters_handles))
    }
    async fn run(&mut self, rt: &mut Rt<Self, S>, (payloads, reporters_handles): Self::Data) -> ActorResult<()> {
        while let Some(stream_id) = rt.inbox_mut().next().await {
            if let Some(payload) = payloads[stream_id as usize].as_ref_payload() {
                if let Err(io_error) = self.socket.write_all(payload).await {
                    if let Some(reporter_handle) =
                        reporters_handles.get(&compute_reporter_num(stream_id, self.appends_num))
                    {
                        reporter_handle
                            .send(ReporterEvent::Err(anyhow!(io_error), stream_id))
                            .unwrap_or_else(|e| log::error!("{}", e))
                    } else {
                        log::error!("No reporter found for stream {}!", stream_id);
                    }
                }
            } else {
                log::error!("No payload found for stream {}!", stream_id);
            }
        }
        Ok(())
    }
}
