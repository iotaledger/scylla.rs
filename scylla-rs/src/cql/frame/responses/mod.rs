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

use super::*;
use crate::prelude::Compression;
use thiserror::Error;

/// Possible response frame bodies.
#[derive(Clone, Debug, From, TryInto)]
#[allow(missing_docs)]
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
    /// Get the frame type's opcode.
    pub fn opcode(&self) -> OpCode {
        match self {
            Self::Error(_) => OpCode::Error,
            Self::Ready(_) => OpCode::Ready,
            Self::Authenticate(_) => OpCode::Authenticate,
            Self::Supported(_) => OpCode::Supported,
            Self::Result(_) => OpCode::Result,
            Self::Event(_) => OpCode::Event,
            Self::AuthChallenge(_) => OpCode::AuthChallenge,
            Self::AuthSuccess(_) => OpCode::AuthSuccess,
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

/// A response frame, which contains a [`Header`] and a [`ResponseBody`].
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
            OpCode::Error => {
                ResponseBody::Error(ErrorFrame::from_payload(start, payload).map_err(FrameError::InvalidBody)?)
            }
            OpCode::Ready => ResponseBody::Ready(ReadyFrame),
            OpCode::Authenticate => ResponseBody::Authenticate(
                AuthenticateFrame::from_payload(start, payload).map_err(FrameError::InvalidBody)?,
            ),
            OpCode::Supported => {
                ResponseBody::Supported(SupportedFrame::from_payload(start, payload).map_err(FrameError::InvalidBody)?)
            }
            OpCode::Result => {
                ResponseBody::Result(ResultFrame::from_payload(start, payload).map_err(FrameError::InvalidBody)?)
            }
            OpCode::Event => {
                ResponseBody::Event(EventFrame::from_payload(start, payload).map_err(FrameError::InvalidBody)?)
            }
            OpCode::AuthChallenge => ResponseBody::AuthChallenge(
                AuthChallengeFrame::from_payload(start, payload).map_err(FrameError::InvalidBody)?,
            ),
            OpCode::AuthSuccess => ResponseBody::AuthSuccess(
                AuthSuccessFrame::from_payload(start, payload).map_err(FrameError::InvalidBody)?,
            ),
            c => anyhow::bail!("Invalid response frame opcode: {:x}", c as u8),
        };
        Ok(Self { header, body })
    }
}

impl ResponseFrame {
    /// Get the frame body.
    pub fn body(&self) -> &ResponseBody {
        &self.body
    }
    /// Consume the frame and get the body.
    pub fn into_body(self) -> ResponseBody {
        self.body
    }
    /// Check if the frame is an [`ErrorFrame`].
    pub fn is_error_frame(&self) -> bool {
        self.header.opcode() == OpCode::Error
    }
    /// Check if the frame is a [`ReadyFrame`].
    pub fn is_ready_frame(&self) -> bool {
        self.header.opcode() == OpCode::Ready
    }
    /// Check if the frame is an [`AuthenticateFrame`].
    pub fn is_authenticate_frame(&self) -> bool {
        self.header.opcode() == OpCode::Authenticate
    }
    /// Check if the frame is a [`SupportedFrame`].
    pub fn is_supported_frame(&self) -> bool {
        self.header.opcode() == OpCode::Supported
    }
    /// Check if the frame is a [`ResultFrame`].
    pub fn is_result_frame(&self) -> bool {
        self.header.opcode() == OpCode::Result
    }
    /// Check if the frame is an [`EventFrame`].
    pub fn is_event_frame(&self) -> bool {
        self.header.opcode() == OpCode::Event
    }
    /// Check if the frame is an [`AuthChallengeFrame`].
    pub fn is_auth_challenge_frame(&self) -> bool {
        self.header.opcode() == OpCode::AuthChallenge
    }
    /// Check if the frame is an [`AuthSuccessFrame`].
    pub fn is_auth_success_frame(&self) -> bool {
        self.header.opcode() == OpCode::AuthSuccess
    }
    /// Get the frame error if it is an [`ErrorFrame`].
    pub fn get_error(&self) -> anyhow::Result<&ErrorFrame> {
        match self.body() {
            ResponseBody::Error(e) => Ok(e),
            _ => Err(anyhow::anyhow!("Not an error")),
        }
    }
    /// Decode the frame using a given compression.
    pub fn decode<C: Compression>(payload: Vec<u8>) -> Result<Self, FrameError> {
        ResponseFrame::from_payload(
            &mut 0,
            C::decompress(payload).map_err(FrameError::CompressionError)?.as_slice(),
        )
        .map_err(FrameError::InvalidFrame)
    }
}
