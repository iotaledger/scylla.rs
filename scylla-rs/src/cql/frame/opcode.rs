// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

//! This module defines the opcode.

use std::convert::TryFrom;

#[allow(missing_docs)]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum OpCode {
    Error = 0x00,
    Startup = 0x01,
    Ready = 0x02,
    Authenticate = 0x03,
    Options = 0x05,
    Supported = 0x06,
    Query = 0x07,
    Result = 0x08,
    Prepare = 0x09,
    Execute = 0x0A,
    Register = 0x0B,
    Event = 0x0C,
    Batch = 0x0D,
    AuthChallenge = 0x0E,
    AuthResponse = 0x0F,
    AuthSuccess = 0x10,
}

impl TryFrom<u8> for OpCode {
    type Error = anyhow::Error;

    fn try_from(value: u8) -> Result<Self, anyhow::Error> {
        Ok(match value {
            0x00 => Self::Error,
            0x01 => Self::Startup,
            0x02 => Self::Ready,
            0x03 => Self::Authenticate,
            0x05 => Self::Options,
            0x06 => Self::Supported,
            0x07 => Self::Query,
            0x08 => Self::Result,
            0x09 => Self::Prepare,
            0x0A => Self::Execute,
            0x0B => Self::Register,
            0x0C => Self::Event,
            0x0D => Self::Batch,
            0x0E => Self::AuthChallenge,
            0x0F => Self::AuthResponse,
            0x10 => Self::AuthSuccess,
            _ => return Err(anyhow::anyhow!("Invalid opcode: {}", value)),
        })
    }
}
