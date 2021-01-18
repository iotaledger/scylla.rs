// Copyright 2020 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::{reporter::*, *};
use tokio::io::AsyncReadExt;

use tokio::{io::ReadHalf, net::TcpStream};

mod event_loop;
mod init;
mod terminating;

const CQL_FRAME_HEADER_BYTES_LENGTH: usize = 9;

// Receiver builder
builder!(ReceiverBuilder {
    socket: ReadHalf<TcpStream>,
    session_id: usize,
    payloads: Payloads,
    buffer_size: usize,
    appends_num: i16
});

/// Receiver state
pub struct Receiver {
    service: Service,
    socket: ReadHalf<TcpStream>,
    stream_id: i16,
    total_length: usize,
    current_length: usize,
    header: bool,
    buffer: Vec<u8>,
    i: usize,
    session_id: usize,
    appends_num: i16,
    payloads: Payloads,
}

impl ActorBuilder<ReportersHandles> for ReceiverBuilder {}

/// implementation of builder
impl Builder for ReceiverBuilder {
    type State = Receiver;
    fn build(self) -> Self::State {
        Self::State {
            service: Service::new(),
            socket: self.socket.unwrap(),
            stream_id: 0,
            total_length: 0,
            current_length: 0,
            header: false,
            buffer: vec![0; self.buffer_size.unwrap()],
            i: 0,
            session_id: self.session_id.unwrap(),
            appends_num: self.appends_num.unwrap(),
            payloads: self.payloads.unwrap(),
        }
        .set_name()
    }
}

/// impl name of the Receiver
impl Name for Receiver {
    fn set_name(mut self) -> Self {
        let name = String::from("Receiver");
        self.service.update_name(name);
        self
    }
    fn get_name(&self) -> String {
        self.service.get_name()
    }
}

#[async_trait::async_trait]
impl AknShutdown<Receiver> for ReportersHandles {
    async fn aknowledge_shutdown(self, mut _state: Receiver, _status: Result<(), Need>) {
        _state.service.update_status(ServiceStatus::Stopped);
        for reporter_handle in self.values() {
            let event = ReporterEvent::Session(Session::Service(_state.service.clone()));
            let _ = reporter_handle.send(event);
        }
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
