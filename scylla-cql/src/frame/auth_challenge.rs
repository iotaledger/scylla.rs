// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

//! This module implements the challenge part of the challenge–response authentication.

use std::convert::TryFrom;

use super::decoder::{bytes, Decoder, Frame};

/// The Autentication Challenge structure with the token field.
pub(crate) struct AuthChallenge {
    #[allow(unused)]
    token: Option<Vec<u8>>,
}

impl AuthChallenge {
    /// Create a new `AuthChallenge ` from the body of frame.
    pub(crate) fn new(decoder: &Decoder) -> anyhow::Result<Self> {
        Self::try_from(decoder.body()?)
    }
}

impl TryFrom<&[u8]> for AuthChallenge {
    type Error = anyhow::Error;

    fn try_from(slice: &[u8]) -> Result<Self, Self::Error> {
        Ok(Self { token: bytes(slice)? })
    }
}
