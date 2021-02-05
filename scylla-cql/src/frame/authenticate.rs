// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

//! This module implements the structure used in autentication process.

use super::decoder::{string, Decoder, Frame};

/// The `Authenticate` sturcutre with the autenticator name.
pub struct Authenticate {
    /// The autenticator name.
    pub authenticator: String,
}

impl Authenticate {
    /// Create a new autenticator from the frame decoder.
    pub fn new(decoder: &Decoder) -> Self {
        Self::from(decoder.body())
    }
    /// Get the autenticator name.
    pub fn authenticator(&self) -> &str {
        &self.authenticator[..]
    }
}

impl From<&[u8]> for Authenticate {
    fn from(slice: &[u8]) -> Self {
        let authenticator = string(slice);
        Self { authenticator }
    }
}
