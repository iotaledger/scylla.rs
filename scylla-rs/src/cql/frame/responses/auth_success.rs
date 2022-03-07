// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

//! This module implements the needed structure and traits used for successful authentication.

use super::*;

/// The structure for successful authentication.
#[derive(Clone, Debug)]
pub struct AuthSuccessFrame {
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
