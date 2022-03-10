// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::{
    reporter::*,
    *,
};
use crate::prelude::{
    Header,
    ResponseFrame,
};
use anyhow::anyhow;
use backstage::core::{
    Actor,
    ActorError,
    ActorResult,
    IoChannel,
    Rt,
    SupHandle,
};
use std::convert::TryFrom;
use tokio::{
    io::AsyncReadExt,
    net::tcp::OwnedReadHalf,
};

const CQL_FRAME_HEADER_BYTES_LENGTH: usize = 9;

/// Receiver state
pub(super) struct Receiver<C: Compression> {
    appends_num: u16,
    buffer_size: usize,
    _compression: PhantomData<fn(C) -> C>,
}

impl<C: Compression> Receiver<C> {
    pub(super) fn new(buffer_size: Option<usize>, appends_num: u16) -> Self {
        Self {
            buffer_size: buffer_size.unwrap_or(1024000),
            appends_num,
            _compression: PhantomData,
        }
    }
}

/// The Receiver actor lifecycle implementation
#[async_trait]
impl<S, C: 'static + Compression> Actor<S> for Receiver<C>
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
    async fn run(&mut self, rt: &mut Rt<Self, S>, (payloads, reporters_handles): Self::Data) -> ActorResult<()> {
        let mut idx = 0;
        let mut header = None;
        let mut buffer = vec![0; self.buffer_size];
        while let Ok(n) = rt.inbox_mut().read(&mut buffer[idx..]).await {
            if n == 0 {
                break;
            }
            idx += n;
            while idx > CQL_FRAME_HEADER_BYTES_LENGTH {
                if header.is_none() {
                    header.replace(Header::try_from(&buffer[..CQL_FRAME_HEADER_BYTES_LENGTH]).map_err(|e| {
                        log::error!("{}", e);
                        ActorError::exit(e)
                    })?);
                }
                if let Some(h) = header {
                    let frame_len = h.body_len() as usize + CQL_FRAME_HEADER_BYTES_LENGTH;
                    if idx >= frame_len {
                        // get mut ref to payload for stream_id
                        let mut payload = payloads[h.stream() as usize]
                            .as_mut()
                            .take()
                            .ok_or_else(|| anyhow!("No payload for stream {}!", h.stream()))?;
                        payload.resize(frame_len, 0);
                        payload.copy_from_slice(&buffer[..frame_len]);
                        // tell reporter that the payload is ready.
                        let reporter_handle = reporters_handles
                            .get(&compute_reporter_num(h.stream(), self.appends_num))
                            .ok_or_else(|| anyhow!("No reporter handle for stream {}!", h.stream()))?;

                        match ResponseFrame::decode::<C>(payload) {
                            Ok(frame) => {
                                reporter_handle
                                    .send(ReporterEvent::Response { frame })
                                    .unwrap_or_else(|e| log::error!("{}", e));
                            }
                            Err(e) => {
                                reporter_handle
                                    .send(ReporterEvent::Err(anyhow::anyhow!(e), h.stream()))
                                    .unwrap_or_else(|e| log::error!("{}", e));
                            }
                        }

                        header = None;
                        if idx > frame_len {
                            buffer.copy_within(frame_len..idx, 0)
                        }
                        idx -= frame_len;
                    } else {
                        break;
                    }
                }
            }
        }
        Ok(())
    }
}
