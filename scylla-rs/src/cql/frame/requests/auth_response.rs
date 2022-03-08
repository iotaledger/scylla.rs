// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

//! This module implements the AUTH_RESPONSE frame.

use super::*;

/**
    Answers a server authentication challenge.

    Authentication in the protocol is SASL based. The server sends authentication
    challenges (a bytes token) to which the client answers with this message. Those
    exchanges continue until the server accepts the authentication by sending a
    AUTH_SUCCESS message after a client AUTH_RESPONSE. Note that the exchange
    begins with the client sending an initial AUTH_RESPONSE in response to a
    server AUTHENTICATE request.

    The body of this message is a single `[bytes]` token. The details of what this
    token contains (and when it can be null/empty, if ever) depends on the actual
    authenticator used.

    The response to a AUTH_RESPONSE is either a follow-up AUTH_CHALLENGE message,
    an AUTH_SUCCESS message or an ERROR message.
*/
#[derive(Debug, Clone, Builder)]
#[builder(derive(Clone, Debug))]
#[builder(pattern = "owned", setter(strip_option))]
pub struct AuthResponseFrame {
    #[allow(missing_docs)]
    pub(crate) token: Vec<u8>,
}

impl AuthResponseFrame {
    /// Get the authentication token.
    pub fn token(&self) -> &[u8] {
        &self.token
    }
}

impl FromPayload for AuthResponseFrame {
    fn from_payload(start: &mut usize, payload: &[u8]) -> anyhow::Result<Self> {
        Ok(Self {
            token: read_bytes(start, payload)?.to_vec(),
        })
    }
}

impl ToPayload for AuthResponseFrame {
    fn to_payload(self, payload: &mut Vec<u8>) {
        write_bytes(&self.token, payload);
    }
}

impl AuthResponseFrameBuilder {
    /// Set the authentication token using an [`Authenticator`]
    pub fn auth_token(mut self, authenticator: &impl Authenticator) -> Self {
        let token = authenticator.token();
        self.token.replace(token);
        self
    }
}

/// The Authenticator structure with the token field.
pub trait Authenticator: Clone + Default {
    /// Get the token in the Authenticator.
    fn token(&self) -> Vec<u8>;
}
#[derive(Debug, Clone, Default)]
/// The unit structure used for letting all users be autenticated.
pub struct AllowAllAuth;

impl Authenticator for AllowAllAuth {
    // Return token as [bytes]
    fn token(&self) -> Vec<u8> {
        // [int] n, followed by n-bytes
        vec![0, 0, 0, 1, 0]
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq, Clone)]
/// The password authentication structure with the user and password fields.
pub struct PasswordAuth {
    user: String,
    pass: String,
}

impl Default for PasswordAuth {
    fn default() -> Self {
        PasswordAuth::new("cassandra".to_owned(), "cassandra".to_owned())
    }
}
impl PasswordAuth {
    /// Create a new user with account and the corresponding password.
    pub fn new(user: String, pass: String) -> Self {
        Self { user, pass }
    }
}

impl Authenticator for PasswordAuth {
    fn token(&self) -> Vec<u8> {
        // compute length in advance
        let length = self.user.len() + self.pass.len() + 2;
        let mut token = Vec::new();
        token.extend_from_slice(&i32::to_be_bytes(length.try_into().unwrap()));
        token.push(0);
        token.extend_from_slice(self.user.as_bytes());
        token.push(0);
        token.extend_from_slice(self.pass.as_bytes());
        token
    }
}
