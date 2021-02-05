// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

//! This module defines the batch flag.

/// The batch flag indicates that there is no flags.
pub const NOFLAGS: u8 = 0x00;
/// The batch flag indicates whether to use serial consistency.
pub const SERIAL_CONSISTENCY: u8 = 0x10;
/// The batch flag indicates whether to use the default timestamp.
pub const TIMESTAMP: u8 = 0x20;
