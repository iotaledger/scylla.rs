// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

//! This module implements the structure used in autentication process.

use super::decoder::{string, Decoder, Frame};
use std::convert::TryFrom;

/// The `Authenticate` sturcutre with the autenticator name.
pub struct Authenticate {
    /// The autenticator name.
    pub authenticator: String,
}

impl Authenticate {
    /// Create a new autenticator from the frame decoder.
    pub fn new(decoder: &Decoder) -> anyhow::Result<Self> {
        Self::try_from(decoder.body()?)
    }
    /// Get the autenticator name.
    #[allow(unused)]
    pub fn authenticator(&self) -> &str {
        &self.authenticator[..]
    }
}

impl TryFrom<&[u8]> for Authenticate {
    type Error = anyhow::Error;

    fn try_from(slice: &[u8]) -> Result<Self, Self::Error> {
        Ok(Self {
            authenticator: string(slice)?,
        })
    }
}
