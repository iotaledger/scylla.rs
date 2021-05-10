// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::{reporter::*, *};
use anyhow::anyhow;
use std::borrow::Cow;
use tokio::{io::AsyncReadExt, net::tcp::OwnedReadHalf};

mod init;
mod run;
mod shutdown;

const CQL_FRAME_HEADER_BYTES_LENGTH: usize = 9;

#[build]
pub fn build_receiver<ReporterEvent, ReportersHandles>(
    service: Service,
    socket: OwnedReadHalf,
    session_id: usize,
    payloads: Payloads,
    buffer_size: usize,
    appends_num: i16,
) -> Receiver {
    Receiver {
        service,
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

/// Receiver state
pub struct Receiver {
    service: Service,
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

impl ActorTypes for Receiver {
    const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);

    type Error = Cow<'static, str>;

    fn service(&mut self) -> &mut Service {
        &mut self.service
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
