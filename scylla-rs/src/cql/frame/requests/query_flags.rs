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
pub const DEFAULT_TIMESTAMP: u8 = 0x20;
/// The query flag indicating whether bound values are named
pub const NAMED_VALUES: u8 = 0x40;

#[derive(Copy, Clone, Debug, Default)]
pub struct QueryFlags(pub u8);

impl QueryFlags {
    pub fn values(&self) -> bool {
        self.0 & VALUES != 0
    }

    pub fn set_values(&mut self, value: bool) {
        if value {
            self.0 |= VALUES;
        } else {
            self.0 &= !VALUES;
        }
    }

    pub fn skip_metadata(&self) -> bool {
        self.0 & SKIP_METADATA != 0
    }

    pub fn set_skip_metadata(&mut self, value: bool) {
        if value {
            self.0 |= SKIP_METADATA;
        } else {
            self.0 &= !SKIP_METADATA;
        }
    }

    pub fn page_size(&self) -> bool {
        self.0 & PAGE_SIZE != 0
    }

    pub fn set_page_size(&mut self, value: bool) {
        if value {
            self.0 |= PAGE_SIZE;
        } else {
            self.0 &= !PAGE_SIZE;
        }
    }

    pub fn paging_state(&self) -> bool {
        self.0 & PAGING_STATE != 0
    }

    pub fn set_paging_state(&mut self, value: bool) {
        if value {
            self.0 |= PAGING_STATE;
        } else {
            self.0 &= !PAGING_STATE;
        }
    }

    pub fn serial_consistency(&self) -> bool {
        self.0 & SERIAL_CONSISTENCY != 0
    }

    pub fn set_serial_consistency(&mut self, value: bool) {
        if value {
            self.0 |= SERIAL_CONSISTENCY;
        } else {
            self.0 &= !SERIAL_CONSISTENCY;
        }
    }

    pub fn default_timestamp(&self) -> bool {
        self.0 & DEFAULT_TIMESTAMP != 0
    }

    pub fn set_default_timestamp(&mut self, value: bool) {
        if value {
            self.0 |= DEFAULT_TIMESTAMP;
        } else {
            self.0 &= !DEFAULT_TIMESTAMP;
        }
    }

    pub fn named_values(&self) -> bool {
        self.0 & NAMED_VALUES != 0
    }

    pub fn set_named_values(&mut self, value: bool) {
        if value {
            self.0 |= NAMED_VALUES;
        } else {
            self.0 &= !NAMED_VALUES;
        }
    }
}
