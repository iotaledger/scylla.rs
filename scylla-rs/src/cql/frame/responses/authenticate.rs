// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

//! This module implements the AUTHENTICATE frame.

use super::*;

/**
   Indicates that the server requires authentication, and which authentication
   mechanism to use.

   The authentication is SASL based and thus consists of a number of server
   challenges ([`AuthChallengeFrame`]) followed by client responses
   ([`AuthResponseFrame`]). The initial exchange is however boostrapped
   by an initial client response. The details of that exchange (including how
   many challenge-response pairs are required) are specific to the authenticator
   in use. The exchange ends when the server sends an [`AuthSuccessFrame`] or
   an [`ErrorFrame`].

   This message will be sent following a [`StartupFrame`] if authentication is
   required and must be answered by a [`AuthResponseFrame`] from the client.

   The body consists of a single `[string]` indicating the full class name of the
   `IAuthenticator` in use.
*/
#[derive(Clone, Debug)]
pub struct AuthenticateFrame {
    /// The autenticator name.
    pub authenticator: String,
}

impl AuthenticateFrame {
    /// Get the autenticator name.
    pub fn authenticator(&self) -> &String {
        &self.authenticator
    }
}

impl FromPayload for AuthenticateFrame {
    fn from_payload(start: &mut usize, payload: &[u8]) -> anyhow::Result<Self> {
        Ok(Self {
            authenticator: read_string(start, payload)?,
        })
    }
}
