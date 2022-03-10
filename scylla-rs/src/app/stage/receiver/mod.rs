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

const FRAME_HEADER_LEN: usize = 9;

/// Receiver state
pub(super) struct Receiver<C: Compression> {
    appends_num: u16,
    _compression: PhantomData<fn(C) -> C>,
}

impl<C: Compression> Receiver<C> {
    pub(super) fn new(appends_num: u16) -> Self {
        Self {
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
        let mut header_buf = [0; FRAME_HEADER_LEN];
        while let Ok(n) = rt.inbox_mut().read_exact(&mut header_buf).await {
            if n == 0 {
                break;
            }
            let header = Header::try_from(header_buf).map_err(|e| {
                log::error!("{}", e);
                ActorError::exit(e)
            })?;
            let mut body_buf = vec![0; header.body_len() as usize];
            if let Ok(n) = rt.inbox_mut().read_exact(&mut body_buf).await {
                if n == 0 {
                    break;
                }
                let frame_len = header.body_len() as usize + FRAME_HEADER_LEN;
                // get mut ref to payload for stream_id
                let mut payload = payloads[header.stream() as usize]
                    .as_mut()
                    .take()
                    .ok_or_else(|| anyhow!("No payload for stream {}!", header.stream()))?;
                payload.resize(frame_len, 0);
                payload[..FRAME_HEADER_LEN].copy_from_slice(&header_buf);
                payload[FRAME_HEADER_LEN..].copy_from_slice(&body_buf);
                // tell reporter that the payload is ready.
                let reporter_handle = reporters_handles
                    .get(&compute_reporter_num(header.stream(), self.appends_num))
                    .ok_or_else(|| anyhow!("No reporter handle for stream {}!", header.stream()))?;

                match ResponseFrame::decode::<C>(payload) {
                    Ok(frame) => {
                        reporter_handle
                            .send(ReporterEvent::Response { frame })
                            .unwrap_or_else(|e| log::error!("{}", e));
                    }
                    Err(e) => {
                        reporter_handle
                            .send(ReporterEvent::Err(anyhow::anyhow!(e), header.stream()))
                            .unwrap_or_else(|e| log::error!("{}", e));
                    }
                }
            }
        }
        Ok(())
    }
}
