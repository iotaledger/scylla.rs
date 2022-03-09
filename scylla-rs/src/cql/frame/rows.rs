// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

//! This module defines the row/column decoder/encoder for the frame structure.

use super::{
    ColumnDecoder,
    RowsResult,
};
use anyhow::ensure;
use std::{
    convert::TryInto,
    marker::PhantomData,
};

/// A result row which can be used to decode column values in order.
pub struct ResultRow<'a> {
    idx: &'a mut usize,
    remaining_cols: usize,
    buffer: &'a [u8],
}

impl<'a> ResultRow<'a> {
    pub(crate) fn new(idx: &'a mut usize, rows: &'a RowsResult) -> Self {
        Self {
            buffer: &rows.rows()[*idx..],
            remaining_cols: rows.metadata().columns_count() as usize,
            idx,
        }
    }

    /// Decode a column value from the row. This will fail if there are no more columns to decode
    /// (specified by the result frame), or if the buffer is malformatted.
    pub fn decode_column<C: ColumnDecoder>(&mut self) -> anyhow::Result<C> {
        ensure!(self.remaining_cols > 0, "No more columns to decode");
        ensure!(
            self.buffer.len() >= *self.idx + 4,
            "Buffer is too small for value length bytes!"
        );
        let length = i32::from_be_bytes(self.buffer[*self.idx..][..4].try_into()?) as usize;
        *self.idx += 4;
        let res = if length > 0 {
            ensure!(
                self.buffer.len() >= *self.idx + length,
                "Buffer is too small for value bytes!"
            );
            let col_slice = &self.buffer[*self.idx..][..length];
            *self.idx += length;
            C::try_decode_column(col_slice)
        } else {
            C::try_decode_column(&[])
        }?;
        self.remaining_cols -= 1;
        Ok(res)
    }
}

/// Defines a result-set row
pub trait RowDecoder: Sized {
    /// Define how to decode the row
    fn try_decode_row(row: ResultRow) -> anyhow::Result<Self>;
}

impl<T> RowDecoder for T
where
    T: ColumnDecoder,
{
    fn try_decode_row(mut row: ResultRow) -> anyhow::Result<Self> {
        Ok(row.decode_column()?)
    }
}

/// A reference iterator over the rows of a result set
#[derive(Clone)]
pub struct Iter<'a, T: RowDecoder> {
    rows: &'a RowsResult,
    idx: usize,
    remaining_rows: usize,
    _marker: PhantomData<T>,
}
impl<'a, T: RowDecoder> std::fmt::Debug for Iter<'a, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Iter")
            .field("rows", &self.rows)
            .field("idx", &self.idx)
            .field("remaining_rows", &self.remaining_rows)
            .finish()
    }
}
impl<'a, T: RowDecoder> Iter<'a, T> {
    /// Create a new iterator over the rows of a result set
    pub fn new(result: &'a RowsResult) -> Self {
        Self {
            remaining_rows: result.rows_count() as usize,
            idx: 0,
            rows: result,
            _marker: PhantomData,
        }
    }

    /// Check if the iterator doesn't have any row
    pub fn is_empty(&self) -> bool {
        self.rows.rows_count() == 0
    }
    /// Get the iterator rows count
    pub fn rows_count(&self) -> usize {
        self.rows.rows_count() as usize
    }
    /// Get the iterator remaining rows count
    pub fn remaining_rows(&self) -> usize {
        self.remaining_rows
    }
    /// Check if it has more pages to request
    pub fn has_more_pages(&self) -> bool {
        self.rows.metadata().flags().has_more_pages()
    }

    /// Get the result set column count
    pub fn columns_count(&self) -> usize {
        self.rows.metadata().columns_count() as usize
    }

    /// Get the result set paging state
    pub fn paging_state(&self) -> &Option<Vec<u8>> {
        self.rows.metadata().paging_state()
    }
}

impl<'a, T: RowDecoder> Iterator for Iter<'a, T> {
    type Item = T;
    /// Note the row decoder is implemented in this `next` method of HardCodedSpecs.
    fn next(&mut self) -> Option<<Self as Iterator>::Item> {
        if self.remaining_rows > 0 {
            let res = match T::try_decode_row(ResultRow::new(&mut self.idx, self.rows)) {
                Ok(res) => Some(res),
                Err(e) => {
                    log::error!("Error decoding row: {}", e);
                    None
                }
            };
            self.remaining_rows -= 1;
            res
        } else {
            None
        }
    }
}

/// An owning iterator over the rows of a result set
#[derive(Clone)]
pub struct IntoIter<T: RowDecoder> {
    rows: RowsResult,
    idx: usize,
    remaining_rows: usize,
    _marker: PhantomData<T>,
}

impl<T: RowDecoder> std::fmt::Debug for IntoIter<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Iter")
            .field("rows", &self.rows)
            .field("idx", &self.idx)
            .field("remaining_rows", &self.remaining_rows)
            .finish()
    }
}

impl<T: RowDecoder> IntoIter<T> {
    /// Create a new owning iterator over the rows of a result set
    pub fn new(result: RowsResult) -> Self {
        Self {
            remaining_rows: result.rows_count() as usize,
            idx: 0,
            rows: result,
            _marker: PhantomData,
        }
    }

    /// Check if the iterator doesn't have any row
    pub fn is_empty(&self) -> bool {
        self.rows.rows_count() == 0
    }
    /// Get the iterator rows count
    pub fn rows_count(&self) -> usize {
        self.rows.rows_count() as usize
    }
    /// Get the iterator remaining rows count
    pub fn remaining_rows(&self) -> usize {
        self.remaining_rows
    }
    /// Check if it has more pages to request
    pub fn has_more_pages(&self) -> bool {
        self.rows.metadata().flags().has_more_pages()
    }

    /// Get the result set column count
    pub fn columns_count(&self) -> usize {
        self.rows.metadata().columns_count() as usize
    }

    /// Get the result set paging state
    pub fn paging_state(&self) -> &Option<Vec<u8>> {
        self.rows.metadata().paging_state()
    }
}

impl<T: RowDecoder> Iterator for IntoIter<T> {
    type Item = T;
    /// Note the row decoder is implemented in this `next` method of HardCodedSpecs.
    fn next(&mut self) -> Option<<Self as Iterator>::Item> {
        if self.remaining_rows > 0 {
            let res = match T::try_decode_row(ResultRow::new(&mut self.idx, &self.rows)) {
                Ok(res) => Some(res),
                Err(e) => {
                    log::error!("Error decoding row: {}", e);
                    None
                }
            };
            self.remaining_rows -= 1;
            res
        } else {
            None
        }
    }
}

macro_rules! row {
    ($($t:tt),*) => {
        impl<$($t: ColumnDecoder),*> RowDecoder for ($($t,)*) {
            fn try_decode_row(mut row: ResultRow) -> anyhow::Result<Self> {
                Ok((
                    $(
                        row.decode_column::<$t>()?,
                    )*
                ))
            }
        }
    };
}

// make a pretty staircase
row!(T1);
row!(T1, T2);
row!(T1, T2, T3);
row!(T1, T2, T3, T4);
row!(T1, T2, T3, T4, T5);
row!(T1, T2, T3, T4, T5, T6);
row!(T1, T2, T3, T4, T5, T6, T7);
row!(T1, T2, T3, T4, T5, T6, T7, T8);
row!(T1, T2, T3, T4, T5, T6, T7, T8, T9);
row!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10);
row!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11);
row!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12);
row!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13);
row!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14);
row!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15);
row!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16);
row!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17);
row!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18);
row!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19);
row!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20);
row!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21);
row!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22);
row!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, T23);
row!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, T23, T24);
row!(
    T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, T23, T24, T25
);
