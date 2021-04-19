// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::{reporter::*, *};
use anyhow::anyhow;
use tokio::{io::AsyncWriteExt, net::tcp::OwnedWriteHalf};

mod event_loop;
mod init;
mod terminating;

// Sender builder
builder!(SenderBuilder {
    socket: OwnedWriteHalf,
    payloads: Payloads,
    appends_num: i16
});

/// SenderHandle to be passed to the supervisor (reporters)
#[derive(Clone)]
pub struct SenderHandle {
    tx: mpsc::UnboundedSender<SenderEvent>,
}
/// SenderInbox is used to recv events
pub struct SenderInbox {
    rx: mpsc::UnboundedReceiver<SenderEvent>,
}

impl Deref for SenderHandle {
    type Target = mpsc::UnboundedSender<SenderEvent>;

    fn deref(&self) -> &Self::Target {
        &self.tx
    }
}

impl DerefMut for SenderHandle {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.tx
    }
}

/// Sender event type.
type SenderEvent = i16;

/// Sender state
pub struct Sender {
    service: Service,
    socket: OwnedWriteHalf,
    handle: Option<SenderHandle>,
    inbox: SenderInbox,
    payloads: Payloads,
    appends_num: i16,
}

impl ActorBuilder<ReportersHandles> for SenderBuilder {}

/// implementation of builder
impl Builder for SenderBuilder {
    type State = Sender;
    fn build(self) -> Self::State {
        let (tx, rx) = mpsc::unbounded_channel::<SenderEvent>();
        let handle = Some(SenderHandle { tx });
        let inbox = SenderInbox { rx };

        Self::State {
            service: Service::new(),
            payloads: self.payloads.unwrap(),
            socket: self.socket.unwrap(),
            appends_num: self.appends_num.unwrap(),
            handle,
            inbox,
        }
        .set_name()
    }
}

/// impl name of the Sender
impl Name for Sender {
    fn set_name(mut self) -> Self {
        let name = String::from("Sender");
        self.service.update_name(name);
        self
    }
    fn get_name(&self) -> String {
        self.service.get_name()
    }
}

#[async_trait::async_trait]
impl AknShutdown<Sender> for ReportersHandles {
    async fn aknowledge_shutdown(self, mut _state: Sender, _status: Result<(), Need>) {
        _state.service.update_status(ServiceStatus::Stopped);
        for reporter_handle in self.values() {
            let event = ReporterEvent::Session(Session::Service(_state.service.clone()));
            let _ = reporter_handle.send(event);
        }
    }
}
