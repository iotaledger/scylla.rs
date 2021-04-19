// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

//! This module implements the response part of the challenge–response authentication.

use super::opcode::AUTH_RESPONSE;
use crate::compression::{Compression, MyCompression};
use std::convert::TryInto;

/// Blanket cql frame header for AUTH_RESPONSE frame.
const AUTH_RESPONSE_HEADER: &'static [u8] = &[4, 0, 0, 0, AUTH_RESPONSE, 0, 0, 0, 0];

/// The Authenticator structure with the token field.
pub trait Authenticator: Clone + Default {
    /// Get the token in the Authenticator.
    fn token(&self) -> Vec<u8>;
}
#[derive(Clone, Default)]
/// The unit structure used for letting all users be autenticated.
pub struct AllowAllAuth;

impl Authenticator for AllowAllAuth {
    // Return token as [bytes]
    fn token(&self) -> Vec<u8> {
        // [int] n, followed by n-bytes
        vec![0, 0, 0, 1, 0]
    }
}

#[derive(Clone)]
/// The password autentication structure with the user and password fields.
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

/// The autentication response frame.
pub(crate) struct AuthResponse(pub Vec<u8>);

impl AuthResponse {
    pub(crate) fn new() -> Self {
        let mut buffer = Vec::new();
        buffer.extend_from_slice(&AUTH_RESPONSE_HEADER);
        AuthResponse(buffer)
    }
    /// Update the response token to be the token from autenticator.
    pub(crate) fn token(mut self, authenticator: &impl Authenticator) -> Self {
        let token = authenticator.token();
        self.0.extend(token);
        self
    }
    /// Build a response frame with a assigned compression type.
    pub(crate) fn build(mut self, compression: impl Compression) -> anyhow::Result<Self> {
        // apply compression flag(if any to the header)
        self.0[1] |= MyCompression::flag();
        self.0 = compression.compress(self.0)?;
        Ok(self)
    }
}
