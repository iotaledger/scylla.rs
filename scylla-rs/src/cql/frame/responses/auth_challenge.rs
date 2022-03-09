// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

//! This module implements the AUTH_CHALLENGE frame.

use super::*;

/**
   A server authentication challenge (see [`AuthResponseFrame`] for more
   details).

   The body of this message is a single `[bytes]` token. The details of what this
   token contains (and when it can be null/empty, if ever) depends on the actual
   authenticator used.

   Clients are expected to answer the server challenge with an [`AuthResponseFrame`].
*/
#[derive(Clone, Debug)]
pub struct AuthChallengeFrame {
    /// The authentication token.
    pub token: Vec<u8>,
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
