// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

//! This module defines the query flags.

/// The query flag indicating that values are provided.
pub const VALUES: u8 = 0x01;
/// The query flag indicating that there is no metadata
pub const SKIP_METADATA: u8 = 0x02;
/// The query flag indicating whether to set a page size.
pub const PAGE_SIZE: u8 = 0x04;
/// The query flag indicating the paging state is present or not.
pub const PAGING_STATE: u8 = 0x08;
/// The query flag indicating whether the serial consistency is present or not.
pub const SERIAL_CONSISTENCY: u8 = 0x10;
/// The query flag indicating whether to use the default timestamp or not.
pub const TIMESTAMP: u8 = 0x20;
/// The query flag indicating whether bound values are named
pub const NAMED_VALUES: u8 = 0x40;
