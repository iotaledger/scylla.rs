// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

//! This module defines the flags for QUERY and EXECUTE frames.

/**
   Flags for a [`QueryFrame`](super::QueryFrame) or [`ExecuteFrame`](super::ExecuteFrame).
   A flag is set if the bit corresponding to its `mask` is set.
   Supported flags are, given their mask:
   - `0x01`: **Values.** If set, a `[short]` `<n>` followed by `<n>` `[value]`
           values are provided. Those values are used for bound variables in
           the query. Optionally, if the `0x40` flag is present, each value
           will be preceded by a `[string]` name, representing the name of
           the marker the value must be bound to.
   - `0x02`: **Skip metadata.** If set, the Result Set returned as a response
           to the query (if any) will have the NO_METADATA flag.
   - `0x04`: **Page size.** If set, `<result_page_size>` is an `[int]`
           controlling the desired page size of the result (in CQL3 rows).
   - `0x08`: **With paging state.** If set, `<paging_state>` should be present.
           `<paging_state>` is a `[bytes]` value that should have been returned
           in a result set. The query will be
           executed but starting from a given paging state. This is also to
           continue paging on a different node than the one where it
           started.
   - `0x10`: **With serial consistency.** If set, <serial_consistency> should be
           present. `<serial_consistency>` is the [consistency] level for the
           serial phase of conditional updates. That consitency can only be
           either SERIAL or LOCAL_SERIAL and if not present, it defaults to
           SERIAL. This option will be ignored for anything else other than a
           conditional update/insert.
   - `0x20`: **With default timestamp.** If set, `<timestamp>` should be present.
           `<timestamp>` is a `[long]` representing the default timestamp for the query
           in microseconds (negative values are forbidden). This will
           replace the server side assigned timestamp as default timestamp.
           Note that a timestamp in the query itself will still override
           this timestamp. This is entirely optional.
   - `0x40`: **With names for values.** This only makes sense if the `0x01` flag is set and
           is ignored otherwise. If present, the values from the `0x01` flag will
           be preceded by a name (see above). Note that this is only useful for
           QUERY requests where named bind markers are used; for EXECUTE statements,
           since the names for the expected values was returned during preparation,
           a client can always provide values in the right order without any names
           and using this flag, while supported, is almost surely inefficient.
*/
#[derive(Copy, Clone, Debug, Default)]
#[repr(transparent)]
pub struct QueryFlags(pub u8);

impl QueryFlags {
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

    /// Indicates that bound values are provided in this frame.
    pub fn values(&self) -> bool {
        self.0 & Self::VALUES != 0
    }

    /// Set the values flag.
    pub fn set_values(&mut self, value: bool) {
        if value {
            self.0 |= Self::VALUES;
        } else {
            self.0 &= !Self::VALUES;
        }
    }

    /// Indicates that there should be no metadata in the response frame resulting from this request.
    pub fn skip_metadata(&self) -> bool {
        self.0 & Self::SKIP_METADATA != 0
    }

    /// Set the skip metadata flag.
    pub fn set_skip_metadata(&mut self, value: bool) {
        if value {
            self.0 |= Self::SKIP_METADATA;
        } else {
            self.0 &= !Self::SKIP_METADATA;
        }
    }

    /// Indicates that a page size is provided in this frame.
    pub fn page_size(&self) -> bool {
        self.0 & Self::PAGE_SIZE != 0
    }

    /// Set the page size flag.
    pub fn set_page_size(&mut self, value: bool) {
        if value {
            self.0 |= Self::PAGE_SIZE;
        } else {
            self.0 &= !Self::PAGE_SIZE;
        }
    }

    /// Indicates that a paging state is provided in this frame.
    pub fn paging_state(&self) -> bool {
        self.0 & Self::PAGING_STATE != 0
    }

    /// Set the paging state flag.
    pub fn set_paging_state(&mut self, value: bool) {
        if value {
            self.0 |= Self::PAGING_STATE;
        } else {
            self.0 &= !Self::PAGING_STATE;
        }
    }

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

    /// Indicates that the values provided in this frame are named.
    ///
    /// NOTE: This is not currently supported!
    pub(crate) fn named_values(&self) -> bool {
        self.0 & Self::NAMED_VALUES != 0
    }

    /// Set the named values flag.
    ///
    /// NOTE: This is not currently supported!
    #[allow(dead_code)]
    pub(crate) fn set_named_values(&mut self, value: bool) {
        if value {
            self.0 |= Self::NAMED_VALUES;
        } else {
            self.0 &= !Self::NAMED_VALUES;
        }
    }
}
