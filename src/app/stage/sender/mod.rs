// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use std::borrow::Cow;

use super::{reporter::*, *};
use anyhow::anyhow;
use tokio::{io::AsyncWriteExt, net::tcp::OwnedWriteHalf};

mod init;
mod run;
mod shutdown;

#[build]
pub fn build_sender<ReporterEvent, ReportersHandles>(
    service: Service,
    socket: OwnedWriteHalf,
    payloads: Payloads,
    appends_num: i16,
) -> Sender {
    let (sender, inbox) = mpsc::unbounded_channel::<SenderEvent>();
    let handle = SenderHandle { sender };
    Sender {
        service,
        payloads,
        socket,
        appends_num,
        handle,
        inbox,
    }
}

/// SenderHandle to be passed to the supervisor (reporters)
#[derive(Clone)]
pub struct SenderHandle {
    sender: mpsc::UnboundedSender<SenderEvent>,
}

impl EventHandle<SenderEvent> for SenderHandle {
    fn send(&mut self, message: SenderEvent) -> anyhow::Result<()> {
        self.sender.send(message).ok();
        Ok(())
    }

    fn shutdown(mut self) -> Option<Self>
    where
        Self: Sized,
    {
        todo!()
    }

    fn update_status(&mut self, service: Service) -> anyhow::Result<()>
    where
        Self: Sized,
    {
        todo!()
    }
}

/// Sender event type.
type SenderEvent = i16;

/// Sender state
pub struct Sender {
    service: Service,
    socket: OwnedWriteHalf,
    handle: SenderHandle,
    inbox: mpsc::UnboundedReceiver<SenderEvent>,
    payloads: Payloads,
    appends_num: i16,
}

impl ActorTypes for Sender {
    const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);

    type Error = Cow<'static, str>;

    fn service(&mut self) -> &mut Service {
        &mut self.service
    }
}

impl EventActor<ReporterEvent, ReportersHandles> for Sender {
    type Event = SenderEvent;

    type Handle = SenderHandle;

    fn handle(&mut self) -> &mut Self::Handle {
        &mut self.handle
    }
}
