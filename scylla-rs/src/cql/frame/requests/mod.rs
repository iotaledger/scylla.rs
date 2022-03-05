// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

pub mod auth_response;
pub mod batch;
pub mod batch_flags;
pub mod execute;
pub mod options;
pub mod prepare;
pub mod query;
pub mod query_flags;
pub mod register;
pub mod startup;

use super::*;
use derive_builder::Builder;
use thiserror::Error;

#[derive(Clone, Debug, From, TryInto)]
pub enum RequestBody {
    Startup(StartupFrame),
    Options(OptionsFrame),
    Query(QueryFrame),
    Prepare(PrepareFrame),
    Execute(ExecuteFrame),
    Register(RegisterFrame),
    Batch(BatchFrame),
    AuthResponse(AuthResponseFrame),
}

impl RequestBody {
    pub fn opcode(&self) -> u8 {
        match self {
            Self::Startup(_) => opcode::STARTUP,
            Self::Options(_) => opcode::OPTIONS,
            Self::Query(_) => opcode::QUERY,
            Self::Prepare(_) => opcode::PREPARE,
            Self::Execute(_) => opcode::EXECUTE,
            Self::Register(_) => opcode::REGISTER,
            Self::Batch(_) => opcode::BATCH,
            Self::AuthResponse(_) => opcode::AUTH_RESPONSE,
        }
    }
}

impl ToPayload for RequestBody {
    fn to_payload(self, payload: &mut Vec<u8>) {
        match self {
            Self::Startup(frame) => frame.to_payload(payload),
            Self::Query(frame) => frame.to_payload(payload),
            Self::Prepare(frame) => frame.to_payload(payload),
            Self::Execute(frame) => frame.to_payload(payload),
            Self::Batch(frame) => frame.to_payload(payload),
            Self::AuthResponse(frame) => frame.to_payload(payload),
            Self::Register(frame) => frame.to_payload(payload),
            Self::Options(_) => (),
        }
    }
}

#[derive(Clone, Debug)]
pub struct RequestFrame {
    pub(crate) header: Header,
    pub(crate) body: RequestBody,
}

impl<T: Into<RequestBody>> From<T> for RequestFrame {
    fn from(body: T) -> Self {
        let body = body.into();
        Self {
            header: Header::from_opcode(body.opcode()),
            body,
        }
    }
}

impl Deref for RequestFrame {
    type Target = Header;

    fn deref(&self) -> &Self::Target {
        &self.header
    }
}

impl TryFrom<&[u8]> for RequestFrame {
    type Error = FrameError;

    fn try_from(payload: &[u8]) -> Result<Self, Self::Error> {
        RequestFrame::from_payload(&mut 0, payload).map_err(FrameError::InvalidFrame)
    }
}

impl FromPayload for RequestFrame {
    fn from_payload(start: &mut usize, payload: &[u8]) -> anyhow::Result<Self> {
        let header = Header::from_payload(start, payload)?;
        let body = match header.opcode() {
            0x01 => RequestBody::Startup(StartupFrame::from_payload(start, payload).map_err(FrameError::InvalidBody)?),
            0x05 => RequestBody::Options(OptionsFrame),
            0x07 => RequestBody::Query(QueryFrame::from_payload(start, payload).map_err(FrameError::InvalidBody)?),
            0x09 => RequestBody::Prepare(PrepareFrame::from_payload(start, payload).map_err(FrameError::InvalidBody)?),
            0x0A => RequestBody::Execute(ExecuteFrame::from_payload(start, payload).map_err(FrameError::InvalidBody)?),
            0x0B => {
                RequestBody::Register(RegisterFrame::from_payload(start, payload).map_err(FrameError::InvalidBody)?)
            }
            0x0D => RequestBody::Batch(BatchFrame::from_payload(start, payload).map_err(FrameError::InvalidBody)?),
            0x0F => RequestBody::AuthResponse(
                AuthResponseFrame::from_payload(start, payload).map_err(FrameError::InvalidBody)?,
            ),
            c => anyhow::bail!("Unknown frame opcode: {}", c),
        };
        Ok(Self { header, body })
    }
}

impl ToPayload for RequestFrame {
    fn to_payload(mut self, payload: &mut Vec<u8>) {
        // First write the body to a separate buffer so that we can determine its length
        let mut body_buf = Vec::new();
        self.body.to_payload(&mut body_buf);

        // Set the length on the header
        self.header.set_body_len(body_buf.len() as u32);

        // Finally write the header and body
        self.header.to_payload(payload);
        payload.extend(body_buf);
    }
}

impl RequestFrame {
    pub fn body(&self) -> &RequestBody {
        &self.body
    }
    pub fn into_body(self) -> RequestBody {
        self.body
    }
    pub fn is_startup_frame(&self) -> bool {
        self.header.opcode() == opcode::STARTUP
    }
    pub fn is_options_frame(&self) -> bool {
        self.header.opcode() == opcode::OPTIONS
    }
    pub fn is_query_frame(&self) -> bool {
        self.header.opcode() == opcode::QUERY
    }
    pub fn is_prepare_frame(&self) -> bool {
        self.header.opcode() == opcode::PREPARE
    }
    pub fn is_execute_frame(&self) -> bool {
        self.header.opcode() == opcode::EXECUTE
    }
    pub fn is_register_frame(&self) -> bool {
        self.header.opcode() == opcode::REGISTER
    }
    pub fn is_batch_frame(&self) -> bool {
        self.header.opcode() == opcode::BATCH
    }
    pub fn is_auth_response_frame(&self) -> bool {
        self.header.opcode() == opcode::AUTH_RESPONSE
    }
    pub fn build_payload(self) -> Vec<u8> {
        let mut payload = Vec::new();
        self.to_payload(&mut payload);
        payload
    }
}

#[derive(Clone, Debug)]
pub enum Value {
    Set(Vec<u8>),
    Null,
    Unset,
    Invalid,
}

impl FromPayload for Value {
    fn from_payload(start: &mut usize, payload: &[u8]) -> anyhow::Result<Self> {
        anyhow::ensure!(payload.len() >= *start + 4, "Not enough bytes for value length");
        let length = read_int(start, payload)?;
        match length {
            -1 => Ok(Value::Null),
            -2 => Ok(Value::Unset),
            -3 => Ok(Value::Invalid),
            _ => {
                anyhow::ensure!(length > 0, "Invalid length");
                anyhow::ensure!(payload.len() >= *start + length as usize, "Not enough bytes for value");
                let res = Value::Set(payload[*start..][..length as usize].to_vec());
                *start += length as usize;
                Ok(res)
            }
        }
    }
}

impl ToPayload for Value {
    fn to_payload(self, payload: &mut Vec<u8>) {
        match self {
            Value::Set(value) => {
                payload.extend((value.len() as i32).to_be_bytes());
                payload.extend_from_slice(&value);
            }
            Value::Null => {
                payload.extend(i32::to_be_bytes(-1));
            }
            Value::Unset => {
                payload.extend(i32::to_be_bytes(-2));
            }
            Value::Invalid => {
                payload.extend(i32::to_be_bytes(-3));
            }
        }
    }
}
