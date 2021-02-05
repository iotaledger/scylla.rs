// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

//! This module implements the challenge part of the challenge–response authentication.

use super::decoder::{bytes, Decoder, Frame};

/// The Autentication Challenge structure with the token field.
pub(crate) struct AuthChallenge {
    token: Option<Vec<u8>>,
}

impl AuthChallenge {
    /// Create a new `AuthChallenge ` from the body of frame.
    pub(crate) fn new(decoder: &Decoder) -> Self {
        Self::from(decoder.body())
    }
}

impl From<&[u8]> for AuthChallenge {
    fn from(slice: &[u8]) -> Self {
        let token = bytes(slice);
        Self { token }
    }
}
