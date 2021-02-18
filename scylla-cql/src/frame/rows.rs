// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

//! This module defines the row/column decoder/encoder for the frame structure.

/// The column count type.
pub type ColumnsCount = i32;
#[derive(Debug)]
/// The flags for row decoder.
pub struct Flags {
    global_table_spec: bool,
    has_more_pages: bool,
    no_metadata: bool,
}

impl Flags {
    /// Decode i32 to flags of row decoder.
    pub fn from_i32(flags: i32) -> Self {
        Flags {
            global_table_spec: (flags & 1) == 1,
            has_more_pages: (flags & 2) == 2,
            no_metadata: (flags & 4) == 4,
        }
    }
    /// Check if are there more pages to decode.
    pub fn has_more_pages(&self) -> bool {
        self.has_more_pages
    }
}
#[derive(Debug)]
/// The pageing state of the response.
pub struct PagingState {
    paging_state: Option<Vec<u8>>,
    end: usize,
}
impl PagingState {
    /// Create a new paing state.
    pub fn new(paging_state: Option<Vec<u8>>, end: usize) -> Self {
        PagingState { paging_state, end }
    }
}
#[derive(Debug)]
/// The meta structure of the row.
pub struct Metadata {
    flags: Flags,
    columns_count: ColumnsCount,
    paging_state: PagingState,
}

impl Metadata {
    /// Create a new meta data.
    pub fn new(flags: Flags, columns_count: ColumnsCount, paging_state: PagingState) -> Self {
        Metadata {
            flags,
            columns_count,
            paging_state,
        }
    }
    /// Get the starting rows.
    pub fn rows_start(&self) -> usize {
        self.paging_state.end
    }
    /// Take the paging state of the metadata.
    pub fn take_paging_state(&mut self) -> Option<Vec<u8>> {
        self.paging_state.paging_state.take()
    }
}

/// Rows trait to decode the final result from scylla
pub trait Rows: Iterator {
    /// create new rows decoder struct
    fn new(decoder: super::decoder::Decoder) -> Self;
}

#[macro_export]
/// The rows macro implements the row decoder.
macro_rules! rows {
    (rows: $rows:ident, row: $row:ident {$( $col_field:ident: $col_type:ty,)*}, row_into: $row_into:tt ) => {
        #[allow(dead_code)]
        /// The `rows` struct for processing each received row in ScyllaDB.
        pub struct $rows {
            decoder: Decoder,
            rows_count: usize,
            remaining_rows_count: usize,
            metadata: Metadata,
            column_start: usize,
        }
        /// It's the `row` struct
        pub struct $row {
            $(
                pub $col_field: $col_type,
            )*
        }

        impl Iterator for $rows {
            type Item = $row_into;
            /// Note the row decoder is implemented in this `next` method.
            fn next(&mut self) -> Option<<Self as Iterator>::Item> {
                if self.remaining_rows_count > 0 {
                    self.remaining_rows_count -= 1;
                    let row_struct = $row {
                        $(
                            $col_field: {
                                let length = i32::from_be_bytes(
                                    self.decoder.buffer_as_ref()[self.column_start..][..4].try_into().unwrap()
                                );
                                self.column_start += 4; // now it become the column_value start, or next column_start if length < 0
                                if length > 0 {
                                    let col_slice = self.decoder.buffer_as_ref()[self.column_start..][..(length as usize)].into();
                                    // update the next column_start to start from next column
                                    self.column_start += (length as usize);
                                    <$col_type>::decode(col_slice)
                                } else {
                                    <$col_type>::decode(&[])
                                }
                            },
                        )*
                    };
                    Some(row_struct.into())
                } else {
                    None
                }
            }
        }

        impl Rows for $rows {
            /// Create a new rows structure.
            fn new(decoder: Decoder) -> Self {
                let metadata = decoder.metadata();
                let rows_start = metadata.rows_start();
                let column_start = rows_start+4;
                let rows_count = i32::from_be_bytes(decoder.buffer_as_ref()[rows_start..column_start].try_into().unwrap());
                Self{
                    decoder,
                    metadata,
                    rows_count: rows_count as usize,
                    remaining_rows_count: rows_count as usize,
                    column_start,
                }
            }
        }
    };
}
