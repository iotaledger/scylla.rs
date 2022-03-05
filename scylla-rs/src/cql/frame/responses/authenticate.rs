// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

//! This module implements the structure used in authentication process.

use super::*;

/// The `Authenticate` sturcutre with the autenticator name.
#[derive(Clone, Debug)]
pub struct AuthenticateFrame {
    /// The autenticator name.
    pub(crate) authenticator: String,
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
