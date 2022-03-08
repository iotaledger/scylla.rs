// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

//! This module defines the consistency enum.

use anyhow::anyhow;
use num_derive::FromPrimitive;
use std::{
    convert::{
        TryFrom,
        TryInto,
    },
    fmt::Display,
};

use super::{
    read_short,
    FromPayload,
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

impl Display for Consistency {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Consistency::Any => write!(f, "Any"),
            Consistency::One => write!(f, "One"),
            Consistency::Two => write!(f, "Two"),
            Consistency::Three => write!(f, "Three"),
            Consistency::Quorum => write!(f, "Quorum"),
            Consistency::All => write!(f, "All"),
            Consistency::LocalQuorum => write!(f, "Local Quorum"),
            Consistency::EachQuorum => write!(f, "Each Quorum"),
            Consistency::Serial => write!(f, "Serial"),
            Consistency::LocalSerial => write!(f, "Local Serial"),
            Consistency::LocalOne => write!(f, "Local One"),
        }
    }
}

impl TryFrom<u16> for Consistency {
    type Error = anyhow::Error;

    fn try_from(value: u16) -> Result<Self, Self::Error> {
        match value {
            0x0 => Ok(Consistency::Any),
            0x1 => Ok(Consistency::One),
            0x2 => Ok(Consistency::Two),
            0x3 => Ok(Consistency::Three),
            0x4 => Ok(Consistency::Quorum),
            0x5 => Ok(Consistency::All),
            0x6 => Ok(Consistency::LocalQuorum),
            0x7 => Ok(Consistency::EachQuorum),
            0x8 => Ok(Consistency::Serial),
            0x9 => Ok(Consistency::LocalSerial),
            0xA => Ok(Consistency::LocalOne),
            _ => Err(anyhow!("Invalid consistency value: {}", value)),
        }
    }
}

impl FromPayload for Consistency {
    fn from_payload(start: &mut usize, payload: &[u8]) -> anyhow::Result<Self> {
        Ok(read_short(start, payload)?.try_into()?)
    }
}
