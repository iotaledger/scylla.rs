// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

//! This module implements the version 4 scylla response frame protocol.
//! See `https://github.com/apache/cassandra/blob/3233c823116343cd95381790d736e239d800035a/doc/native_protocol_v4.spec#L492` for more details.

pub mod auth_challenge;
pub mod auth_success;
pub mod authenticate;
pub mod error;
pub mod event;
pub mod ready;
pub mod result;
pub mod supported;

use crate::prelude::Compression;

use super::*;
use thiserror::Error;

#[derive(Clone, Debug, From, TryInto)]
pub enum ResponseBody {
    Error(ErrorFrame),
    Ready(ReadyFrame),
    Authenticate(AuthenticateFrame),
    Supported(SupportedFrame),
    Result(ResultFrame),
    Event(EventFrame),
    AuthChallenge(AuthChallengeFrame),
    AuthSuccess(AuthSuccessFrame),
}

impl ResponseBody {
    pub fn opcode(&self) -> u8 {
        match self {
            Self::Error(_) => opcode::ERROR,
            Self::Ready(_) => opcode::READY,
            Self::Authenticate(_) => opcode::AUTHENTICATE,
            Self::Supported(_) => opcode::SUPPORTED,
            Self::Result(_) => opcode::RESULT,
            Self::Event(_) => opcode::EVENT,
            Self::AuthChallenge(_) => opcode::AUTH_CHALLENGE,
            Self::AuthSuccess(_) => opcode::AUTH_SUCCESS,
        }
    }
}

impl TryInto<RowsResult> for ResponseBody {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<RowsResult, anyhow::Error> {
        match self {
            Self::Result(frame) => frame.try_into(),
            _ => anyhow::bail!("Not a result frame"),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ResponseFrame {
    pub(crate) header: Header,
    pub(crate) body: ResponseBody,
}

impl<T: Into<ResponseBody>> From<T> for ResponseFrame {
    fn from(body: T) -> Self {
        let body = body.into();
        Self {
            header: Header::from_opcode(body.opcode()),
            body,
        }
    }
}

impl TryInto<RowsResult> for ResponseFrame {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<RowsResult, Self::Error> {
        match self.body {
            ResponseBody::Result(frame) => frame.try_into(),
            _ => anyhow::bail!("Expected Result Frame, got {:?}", self.body),
        }
    }
}

impl Deref for ResponseFrame {
    type Target = Header;

    fn deref(&self) -> &Self::Target {
        &self.header
    }
}

impl FromPayload for ResponseFrame {
    fn from_payload(start: &mut usize, payload: &[u8]) -> anyhow::Result<Self> {
        let header = Header::from_payload(start, payload)?;
        let body = match header.opcode() {
            0x00 => ResponseBody::Error(ErrorFrame::from_payload(start, payload).map_err(FrameError::InvalidBody)?),
            0x02 => ResponseBody::Ready(ReadyFrame),
            0x03 => ResponseBody::Authenticate(
                AuthenticateFrame::from_payload(start, payload).map_err(FrameError::InvalidBody)?,
            ),
            0x06 => {
                ResponseBody::Supported(SupportedFrame::from_payload(start, payload).map_err(FrameError::InvalidBody)?)
            }
            0x08 => ResponseBody::Result(ResultFrame::from_payload(start, payload).map_err(FrameError::InvalidBody)?),
            0x0C => ResponseBody::Event(EventFrame::from_payload(start, payload).map_err(FrameError::InvalidBody)?),
            0x0E => ResponseBody::AuthChallenge(
                AuthChallengeFrame::from_payload(start, payload).map_err(FrameError::InvalidBody)?,
            ),
            0x10 => ResponseBody::AuthSuccess(
                AuthSuccessFrame::from_payload(start, payload).map_err(FrameError::InvalidBody)?,
            ),
            c => anyhow::bail!("Unknown frame opcode: {}", c),
        };
        Ok(Self { header, body })
    }
}

impl ResponseFrame {
    pub fn body(&self) -> &ResponseBody {
        &self.body
    }
    pub fn into_body(self) -> ResponseBody {
        self.body
    }
    pub fn is_error_frame(&self) -> bool {
        self.header.opcode() == opcode::ERROR
    }
    pub fn is_ready_frame(&self) -> bool {
        self.header.opcode() == opcode::READY
    }
    pub fn is_authenticate_frame(&self) -> bool {
        self.header.opcode() == opcode::AUTHENTICATE
    }
    pub fn is_supported_frame(&self) -> bool {
        self.header.opcode() == opcode::SUPPORTED
    }
    pub fn is_result_frame(&self) -> bool {
        self.header.opcode() == opcode::RESULT
    }
    pub fn is_event_frame(&self) -> bool {
        self.header.opcode() == opcode::EVENT
    }
    pub fn is_auth_challenge_frame(&self) -> bool {
        self.header.opcode() == opcode::AUTH_CHALLENGE
    }
    pub fn is_auth_success_frame(&self) -> bool {
        self.header.opcode() == opcode::AUTH_SUCCESS
    }
    pub fn get_error(&self) -> anyhow::Result<&ErrorFrame> {
        match self.body() {
            ResponseBody::Error(e) => Ok(e),
            _ => Err(anyhow::anyhow!("Not an error")),
        }
    }
    pub fn decode<C: Compression>(payload: Vec<u8>) -> Result<Self, FrameError> {
        ResponseFrame::from_payload(
            &mut 0,
            C::decompress(payload).map_err(FrameError::CompressionError)?.as_slice(),
        )
        .map_err(FrameError::InvalidFrame)
    }
}
