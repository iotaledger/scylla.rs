// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

//! This module implements the challenge part of the challenge–response authentication.

use super::*;

/// The authentication Challenge structure with the token field.
#[derive(Clone, Debug)]
pub struct AuthChallengeFrame {
    pub(crate) token: Vec<u8>,
}

impl AuthChallengeFrame {
    /// Get the authentication token.
    pub fn token(&self) -> &[u8] {
        self.token.as_ref()
    }
}

impl FromPayload for AuthChallengeFrame {
    fn from_payload(start: &mut usize, payload: &[u8]) -> anyhow::Result<Self> {
        Ok(Self {
            token: read_bytes(start, payload)?.to_vec(),
        })
    }
}
