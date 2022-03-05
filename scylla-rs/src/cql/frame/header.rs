// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

//! This module defines the header trait.

use super::{
    FromPayload,
    ToPayload,
};
use std::convert::{
    TryFrom,
    TryInto,
};

/// The ignore flag.
pub const IGNORE: u8 = 0x00;
/// The compression flag.
pub const COMPRESSION: u8 = 0x01;
/// The tracing flag.
pub const TRACING: u8 = 0x02;
/// The custoem payload flag.
pub const CUSTOM_PAYLOAD: u8 = 0x04;
/// The warning flag.
pub const WARNING: u8 = 0x08;

#[derive(Copy, Clone, Debug)]
#[repr(u8)]
pub enum Direction {
    Request = 0,
    Response = 1,
}

#[derive(Copy, Clone, Debug)]
pub struct Version(u8);

impl Default for Version {
    fn default() -> Self {
        Self(0x04)
    }
}

impl Version {
    pub fn direction(&self) -> Direction {
        match self.0 & 0x80 {
            0 => Direction::Request,
            _ => Direction::Response,
        }
    }

    pub fn version(&self) -> u8 {
        self.0 & 0x7f
    }
}

#[derive(Copy, Clone, Debug, Default)]
pub struct Flags(u8);

impl Flags {
    pub fn compression(&self) -> bool {
        self.0 & COMPRESSION != 0
    }

    pub fn set_compression(&mut self, value: bool) {
        if value {
            self.0 |= COMPRESSION;
        } else {
            self.0 &= !COMPRESSION;
        }
    }

    pub fn tracing(&self) -> bool {
        self.0 & TRACING != 0
    }

    pub fn set_tracing(&mut self, value: bool) {
        if value {
            self.0 |= TRACING;
        } else {
            self.0 &= !TRACING;
        }
    }

    pub fn custom_payload(&self) -> bool {
        self.0 & CUSTOM_PAYLOAD != 0
    }

    pub fn set_custom_payload(&mut self, value: bool) {
        if value {
            self.0 |= CUSTOM_PAYLOAD;
        } else {
            self.0 &= !CUSTOM_PAYLOAD;
        }
    }

    pub fn warning(&self) -> bool {
        self.0 & WARNING != 0
    }

    pub fn set_warning(&mut self, value: bool) {
        if value {
            self.0 |= WARNING;
        } else {
            self.0 &= !WARNING;
        }
    }
}

#[derive(Copy, Clone, Debug)]
pub struct Header {
    version: Version,
    flags: Flags,
    stream: u16,
    opcode: u8,
    body_len: u32,
}

impl Header {
    pub fn direction(&self) -> Direction {
        self.version.direction()
    }

    pub fn version(&self) -> u8 {
        self.version.version()
    }

    pub fn version_mut(&mut self) -> &mut Version {
        &mut self.version
    }

    pub fn flags(&self) -> &Flags {
        &self.flags
    }

    pub fn flags_mut(&mut self) -> &mut Flags {
        &mut self.flags
    }

    pub fn compression(&self) -> bool {
        self.flags.compression()
    }

    pub fn tracing(&self) -> bool {
        self.flags.tracing()
    }

    pub fn custom_payload(&self) -> bool {
        self.flags.custom_payload()
    }

    pub fn warning(&self) -> bool {
        self.flags.warning()
    }

    pub fn stream(&self) -> u16 {
        self.stream
    }

    pub fn set_stream(&mut self, stream: u16) {
        self.stream = stream;
    }

    pub fn opcode(&self) -> u8 {
        self.opcode
    }

    pub fn set_opcode(&mut self, opcode: u8) {
        self.opcode = opcode;
    }

    pub fn body_len(&self) -> u32 {
        self.body_len
    }

    pub fn set_body_len(&mut self, body_len: u32) {
        self.body_len = body_len;
    }

    pub fn from_opcode(opcode: u8) -> Self {
        Self {
            version: Version::default(),
            flags: Flags::default(),
            stream: 0,
            opcode,
            body_len: 0,
        }
    }
}

impl TryFrom<&[u8]> for Header {
    type Error = anyhow::Error;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        anyhow::ensure!(bytes.len() == 9, "Invalid header length");
        Ok(Header {
            version: Version(bytes[0]),
            flags: Flags(bytes[1]),
            stream: u16::from_be_bytes([bytes[2], bytes[3]]),
            opcode: bytes[4],
            body_len: u32::from_be_bytes([bytes[5], bytes[6], bytes[7], bytes[8]]),
        })
    }
}

impl FromPayload for Header {
    fn from_payload(start: &mut usize, payload: &[u8]) -> anyhow::Result<Self> {
        anyhow::ensure!(payload.len() >= *start + 9, "Payload is too small");
        let header = payload[*start..][..9].try_into()?;
        *start += 9;
        Ok(header)
    }
}

impl ToPayload for Header {
    fn to_payload(self, payload: &mut Vec<u8>) {
        if self.body_len() > 0 {
            payload.reserve(9 + self.body_len() as usize);
        }
        payload.extend(Into::<[u8; 9]>::into(self));
    }
}

impl Into<[u8; 9]> for Header {
    fn into(self) -> [u8; 9] {
        [
            self.version.0,
            self.flags.0,
            (self.stream >> 8) as u8,
            self.stream as u8,
            self.opcode,
            (self.body_len >> 24) as u8,
            (self.body_len >> 16) as u8,
            (self.body_len >> 8) as u8,
            self.body_len as u8,
        ]
    }
}
