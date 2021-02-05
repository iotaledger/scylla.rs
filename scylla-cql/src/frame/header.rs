// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

//! This module defines the header trait.

/// The ignore flag.
pub const IGNORE: u8 = 0x00;
/// The compression flag.
pub(crate) const COMPRESSION: u8 = 0x01;
/// The tracing flag.
pub const TRACING: u8 = 0x02;
/// The custoem payload flag.
pub const CUSTOM_PAYLOAD: u8 = 0x04;
/// The warning flag.
pub const WARNING: u8 = 0x08;
