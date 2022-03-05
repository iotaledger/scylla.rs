// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

//! This module defines the batch flag.

/// The batch flag indicates that there is no flags.
pub const NOFLAGS: u8 = 0x00;
/// The batch flag indicates whether to use serial consistency.
pub const SERIAL_CONSISTENCY: u8 = 0x10;
/// The batch flag indicates whether to use the default timestamp.
pub const DEFAULT_TIMESTAMP: u8 = 0x20;
/// The batch flag indicating whether bound values are named
pub const NAMED_VALUES: u8 = 0x40;

#[derive(Copy, Clone, Debug, Default)]
pub struct BatchFlags(pub u8);

impl BatchFlags {
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
