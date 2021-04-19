// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

//! This module implements the needed structure and traits used for successful autentication.

use super::decoder::{bytes, Decoder, Frame};
use std::convert::TryFrom;

/// The structure for successful autentication.
pub struct AuthSuccess {
    token: Option<Vec<u8>>,
}

impl AuthSuccess {
    /// Create a new `AuthSuccess` structure from frame decoder.
    pub fn new(decoder: &Decoder) -> anyhow::Result<Self> {
        Self::try_from(decoder.body()?)
    }
    /// Get the autentication token.
    pub fn token(&self) -> Option<&Vec<u8>> {
        self.token.as_ref()
    }
}

impl TryFrom<&[u8]> for AuthSuccess {
    type Error = anyhow::Error;

    fn try_from(slice: &[u8]) -> Result<Self, Self::Error> {
        Ok(Self { token: bytes(slice)? })
    }
}
