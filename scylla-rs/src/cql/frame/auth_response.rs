// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

//! This module implements the response part of the challenge–response authentication.

use super::{
    opcode::AUTH_RESPONSE,
    FrameBuilder,
};
use std::convert::TryInto;
use thiserror::Error;

/// Blanket cql frame header for AUTH_RESPONSE frame.
const AUTH_RESPONSE_HEADER: [u8; 5] = [4, 0, 0, 0, AUTH_RESPONSE];

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
pub struct AuthResponse(pub Vec<u8>);

#[derive(Debug, Clone, Default)]
pub struct AuthResponseBuilder {
    token: Option<Vec<u8>>,
}

#[derive(Debug, Error)]
pub enum AuthResponseBuildError {
    #[error("No token provided")]
    NoToken,
}

impl AuthResponseBuilder {
    /// Update the response token to be the token from autenticator.
    pub fn token(mut self, authenticator: &impl Authenticator) -> Self {
        let token = authenticator.token();
        self.token.replace(token);
        self
    }

    /// Build the prepare frame with an assigned compression type.
    pub fn build(mut self) -> Result<AuthResponse, AuthResponseBuildError> {
        Ok(AuthResponse(FrameBuilder::build(
            AUTH_RESPONSE_HEADER,
            self.token.take().ok_or_else(|| AuthResponseBuildError::NoToken)?,
        )))
    }
}
