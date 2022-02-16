// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

//! This module defines the consistency enum.

use anyhow::anyhow;
use num_derive::FromPrimitive;
use num_traits::FromPrimitive;
use std::convert::{
    TryFrom,
    TryInto,
};
#[derive(Copy, Clone, Debug, FromPrimitive)]
#[repr(u16)]
/// The consistency level enum.
pub enum Consistency {
    /// The any consistency level.
    Any = 0x0,
    /// The one consistency level.
    One = 0x1,
    /// The two consistency level.
    Two = 0x2,
    /// The three consistency level.
    Three = 0x3,
    /// The quorum consistency level.
    Quorum = 0x4,
    /// The all consistency level.
    All = 0x5,
    /// The local quorum consistency level.
    LocalQuorum = 0x6,
    /// The each quorum consistency level.
    EachQuorum = 0x7,
    /// The serial consistency level.
    Serial = 0x8,
    /// The local serial consistency level.
    LocalSerial = 0x9,
    /// The local one consistency level.
    LocalOne = 0xA,
}

impl TryFrom<&[u8]> for Consistency {
    type Error = anyhow::Error;

    fn try_from(slice: &[u8]) -> Result<Self, Self::Error> {
        Consistency::from_u16(u16::from_be_bytes(slice[0..2].try_into()?))
            .ok_or(anyhow!("No consistency representation for provided bytes!"))
    }
}
