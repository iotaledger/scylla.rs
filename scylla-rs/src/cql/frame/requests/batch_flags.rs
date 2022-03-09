// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

//! This module defines the BATCH frame flags.

/**
    [`BatchFrame`](super::BatchFrame) flags. Similar to [`QueryFlags`](super::QueryFlags),
    except that the 4 rightmost bits must always be 0 as their corresponding options do not make
    sense for Batch. A flag is set if the bit corresponding to its `mask` is set. Supported
    flags are, given their mask:
    - `0x10`: **With serial consistency.** If set, `<serial_consistency>` should be
           present. `<serial_consistency>` is the `[consistency]` level for the
           serial phase of conditional updates. That consistency can only be
           either SERIAL or LOCAL_SERIAL and if not present, it defaults to
           SERIAL. This option will be ignored for anything else other than a
           conditional update/insert.
    - `0x20`: **With default timestamp.** If set, `<timestamp>` should be present.
           `<timestamp>` is a `[long]` representing the default timestamp for the query
           in microseconds. This will replace the server side assigned
           timestamp as default timestamp. Note that a timestamp in the query itself
           will still override this timestamp.
    - `0x40`: **With names for values.** If set, then all values for all <query_i> must be
           preceded by a [string] <name_i> that have the same meaning as in QUERY
           requests [IMPORTANT NOTE: this feature does not work and should not be
           used. It is specified in a way that makes it impossible for the server
           to implement.
*/
#[derive(Copy, Clone, Debug, Default)]
#[repr(transparent)]
pub struct BatchFlags(pub u8);

impl BatchFlags {
    /// The batch flag indicates whether to use serial consistency.
    pub const SERIAL_CONSISTENCY: u8 = 0x10;
    /// The batch flag indicates whether to use the default timestamp.
    pub const DEFAULT_TIMESTAMP: u8 = 0x20;
    /// The batch flag indicating whether bound values are named
    pub const NAMED_VALUES: u8 = 0x40;

    /// Indicates that a serial consistency is provided in this frame.
    ///
    /// Serial consistency is the [`Consistency`](super::Consistency) level for the
    /// serial phase of conditional updates.
    pub fn serial_consistency(&self) -> bool {
        self.0 & Self::SERIAL_CONSISTENCY != 0
    }

    /// Set the serial consistency flag. This consistency can only be
    /// either SERIAL or LOCAL_SERIAL and if not present, it defaults to
    /// SERIAL. This option will be ignored for anything else other than a
    /// conditional update/insert.
    pub fn set_serial_consistency(&mut self, value: bool) {
        if value {
            self.0 |= Self::SERIAL_CONSISTENCY;
        } else {
            self.0 &= !Self::SERIAL_CONSISTENCY;
        }
    }

    /// Indicates that a default timestamp is provided in this frame.
    pub fn default_timestamp(&self) -> bool {
        self.0 & Self::DEFAULT_TIMESTAMP != 0
    }

    /// Set the default timestamp flag. This will replace the server side assigned timestamp as default timestamp.
    /// Note that a timestamp in the query itself will still override this timestamp.
    pub fn set_default_timestamp(&mut self, value: bool) {
        if value {
            self.0 |= Self::DEFAULT_TIMESTAMP;
        } else {
            self.0 &= !Self::DEFAULT_TIMESTAMP;
        }
    }

    #[allow(dead_code)]
    /// Indicates that the values provided in this frame are named.
    ///
    /// NOTE: This is not currently supported!
    pub(crate) fn named_values(&self) -> bool {
        self.0 & Self::NAMED_VALUES != 0
    }

    #[allow(dead_code)]
    /// Set the named values flag.
    ///
    /// NOTE: This is not currently supported!
    pub(crate) fn set_named_values(&mut self, value: bool) {
        if value {
            self.0 |= Self::NAMED_VALUES;
        } else {
            self.0 &= !Self::NAMED_VALUES;
        }
    }
}
