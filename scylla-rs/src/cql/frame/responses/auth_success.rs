// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

//! This module implements AUTH_SUCCESS frame.

use super::*;

/**
   Indicates the success of the authentication phase. See [`AuthenticateFrame`] for more
   details.

   The body of this message is a single `[bytes]` token holding final information
   from the server that the client may require to finish the authentication
   process. What that token contains and whether it can be null depends on the
   actual authenticator used.
*/
#[derive(Clone, Debug)]
pub struct AuthSuccessFrame {
    /// The authentication token.
    pub token: Vec<u8>,
}

impl AuthSuccessFrame {
    /// Get the authentication token.
    pub fn token(&self) -> &[u8] {
        self.token.as_ref()
    }
}

impl FromPayload for AuthSuccessFrame {
    fn from_payload(start: &mut usize, payload: &[u8]) -> anyhow::Result<Self> {
        Ok(Self {
            token: read_bytes(start, payload)?.to_vec(),
        })
    }
}
