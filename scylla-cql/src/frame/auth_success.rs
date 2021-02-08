// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

//! This module implements the needed structure and traits used for successful autentication.

use super::decoder::{bytes, Decoder, Frame};

/// The structure for successful autentication.
pub struct AuthSuccess {
    token: Option<Vec<u8>>,
}

impl AuthSuccess {
    /// Create a new `AuthSuccess` structure from frame decoder.
    pub fn new(decoder: &Decoder) -> Self {
        Self::from(decoder.body())
    }
    /// Get the autentication token.
    pub fn token(&self) -> Option<&Vec<u8>> {
        self.token.as_ref()
    }
}

impl From<&[u8]> for AuthSuccess {
    fn from(slice: &[u8]) -> Self {
        let token = bytes(slice);
        Self { token }
    }
}
