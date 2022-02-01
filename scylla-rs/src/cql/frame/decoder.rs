// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

//! This module implements the frame decoder.

use super::{
    error,
    header,
    opcode,
    result,
    rows::{
        ColumnsCount,
        Flags,
        Metadata,
        PagingState,
    },
};
use chrono::{
    Date,
    DateTime,
    NaiveDate,
    NaiveDateTime,
    NaiveTime,
    Utc,
};

use crate::{
    cql::compression::{
        Compression,
        MyCompression,
    },
    prelude::Row,
};
use anyhow::{
    anyhow,
    ensure,
};
use std::{
    collections::HashMap,
    convert::{
        TryFrom,
        TryInto,
    },
    hash::Hash,
    io::Cursor,
    net::{
        IpAddr,
        Ipv4Addr,
        Ipv6Addr,
    },
    str,
};
/// RowsDecoder trait to decode the rows result from scylla
pub trait RowsDecoder: Sized {
    /// The Row to decode. Must implement [`super::Row`].
    type Row: Row;
    /// Try to decode the provided Decoder with an expected Rows result
    fn try_decode_rows(decoder: Decoder) -> anyhow::Result<Option<Self>>;
    /// Decode the provided Decoder with deterministic Rows result
    fn decode_rows(decoder: Decoder) -> Option<Self> {
        Self::try_decode_rows(decoder).unwrap()
    }
}

impl<T> RowsDecoder for T
where
    T: Row,
{
    type Row = T;

    fn try_decode_rows(decoder: Decoder) -> anyhow::Result<Option<Self>> {
        Ok(Self::Row::rows_iter(decoder)?.next())
    }
}

impl<T> RowsDecoder for crate::prelude::Iter<T>
where
    T: Row,
{
    type Row = T;

    fn try_decode_rows(decoder: Decoder) -> anyhow::Result<Option<Self>> {
        ensure!(decoder.is_rows()?, "Decoded response is not rows!");
        let rows_iter = Self::Row::rows_iter(decoder)?;
        if rows_iter.is_empty() && !rows_iter.has_more_pages() {
            Ok(None)
        } else {
            Ok(Some(rows_iter))
        }
    }
}

// impl<T> RowsDecoder for Vec<T>
// where
//    T: ColumnDecoder + Row,
//{
//    type Row = T;
//
//    fn try_decode_rows(decoder: Decoder) -> anyhow::Result<Option<Self>> {
//        ensure!(decoder.is_rows()?, "Decoded response is not rows!");
//        Ok(Some(Self::Row::rows_iter(decoder)?.collect()))
//    }
//}

/// VoidDecoder trait to decode the VOID result from scylla
pub struct VoidDecoder;

impl VoidDecoder {
    /// Try to decode the provided Decoder with an expected Void result
    pub fn try_decode_void(decoder: Decoder) -> anyhow::Result<()> {
        if decoder.is_error()? {
            Err(anyhow!(decoder.get_error()?))
        } else {
            Ok(())
        }
    }
    /// Decode the provided Decoder with an deterministic Void result
    pub fn decode_void(decoder: Decoder) {
        Self::try_decode_void(decoder).unwrap()
    }
}

impl TryFrom<Vec<u8>> for Decoder {
    type Error = anyhow::Error;

    fn try_from(buffer: Vec<u8>) -> Result<Self, Self::Error> {
        Decoder::new(buffer, MyCompression::get())
    }
}

/// The CQL frame trait.
pub trait Frame {
    /// Get the frame version.
    fn version(&self) -> anyhow::Result<u8>;
    /// G the frame header flags.
    fn flags(&self) -> &HeaderFlags;
    /// Get the stream in the frame header.
    fn stream(&self) -> anyhow::Result<i16>;
    /// Get the opcode in the frame header.
    fn opcode(&self) -> anyhow::Result<u8>;
    /// Get the length of the frame body.
    fn length(&self) -> anyhow::Result<usize>;
    /// Get the frame body.
    fn body(&self) -> anyhow::Result<&[u8]>;
    /// Get the body start of the frame.
    fn body_start(&self, padding: usize) -> usize;
    /// Get the body kind.
    fn body_kind(&self) -> anyhow::Result<i32>;
    /// Check whether the opcode is `AUTHENTICATE`.
    fn is_authenticate(&self) -> anyhow::Result<bool>;
    /// Check whether the opcode is `AUTH_CHALLENGE`.
    fn is_auth_challenge(&self) -> anyhow::Result<bool>;
    /// Check whether the opcode is `AUTH_SUCCESS`.
    fn is_auth_success(&self) -> anyhow::Result<bool>;
    /// Check whether the opcode is `SUPPORTED`.
    fn is_supported(&self) -> anyhow::Result<bool>;
    /// Check whether the opcode is `READY`.
    fn is_ready(&self) -> anyhow::Result<bool>;
    /// Check whether the body kind is `VOID`.
    fn is_void(&self) -> anyhow::Result<bool>;
    /// Check whether the body kind is `ROWS`.
    fn is_rows(&self) -> anyhow::Result<bool>;
    /// Check whether the opcode is `ERROR`.
    fn is_error(&self) -> anyhow::Result<bool>;
    /// Get the `CqlError`.
    fn get_error(&self) -> anyhow::Result<error::CqlError>;
    /// Get Void `()`
    fn get_void(&self) -> anyhow::Result<()>;
    /// Check whether the error is `UNPREPARED`.
    fn is_unprepared(&self) -> anyhow::Result<bool>;
    /// Check whether the error is `ALREADY_EXISTS.
    fn is_already_exists(&self) -> anyhow::Result<bool>;
    /// Check whether the error is `CONFIGURE_ERROR.
    fn is_configure_error(&self) -> anyhow::Result<bool>;
    /// Check whether the error is `INVALID.
    fn is_invalid(&self) -> anyhow::Result<bool>;
    /// Check whether the error is `UNAUTHORIZED.
    fn is_unauthorized(&self) -> anyhow::Result<bool>;
    /// Check whether the error is `SYNTAX_ERROR.
    fn is_syntax_error(&self) -> anyhow::Result<bool>;
    /// Check whether the error is `WRITE_FAILURE.
    fn is_write_failure(&self) -> anyhow::Result<bool>;
    /// Check whether the error is `FUNCTION_FAILURE.
    fn is_function_failure(&self) -> anyhow::Result<bool>;
    /// Check whether the error is `READ_FAILURE.
    fn is_read_failure(&self) -> anyhow::Result<bool>;
    /// Check whether the error is `READ_TIMEOUT.
    fn is_read_timeout(&self) -> anyhow::Result<bool>;
    /// Check whether the error is `WRITE_TIMEOUT.
    fn is_write_timeout(&self) -> anyhow::Result<bool>;
    /// Check whether the error is `TRUNCATE_ERROR.
    fn is_truncate_error(&self) -> anyhow::Result<bool>;
    /// Check whether the error is `IS_BOOSTRAPPING.
    fn is_boostrapping(&self) -> anyhow::Result<bool>;
    /// Check whether the error is `OVERLOADED.
    fn is_overloaded(&self) -> anyhow::Result<bool>;
    /// Check whether the error is `UNAVAILABLE_EXCEPTION.
    fn is_unavailable_exception(&self) -> anyhow::Result<bool>;
    /// Check whether the error is `AUTHENTICATION_ERROR.
    fn is_authentication_error(&self) -> anyhow::Result<bool>;
    /// Check whether the error is `PROTOCOL_ERROR.
    fn is_protocol_error(&self) -> anyhow::Result<bool>;
    /// Check whether the error is `SERVER_ERROR.
    fn is_server_error(&self) -> anyhow::Result<bool>;
    /// The the row flags.
    fn rows_flags(&self) -> anyhow::Result<Flags>;
    /// The the column counts.
    fn columns_count(&self) -> anyhow::Result<ColumnsCount>;
    /// The the paging state.
    fn paging_state(&self, has_more_pages: bool) -> anyhow::Result<PagingState>;
    /// The the metadata.
    fn metadata(&self) -> anyhow::Result<Metadata>;
}
/// The frame decoder structure.
#[derive(Debug, Clone)]
pub struct Decoder {
    buffer: Vec<u8>,
    header_flags: HeaderFlags,
}
impl Decoder {
    /// Create a new decoder with an assigned compression type.
    pub fn new(mut buffer: Vec<u8>, decompressor: impl Compression) -> anyhow::Result<Self> {
        buffer = decompressor.decompress(buffer)?;
        let header_flags = HeaderFlags::new(&buffer)?;
        Ok(Decoder { buffer, header_flags })
    }
    /// Get the decoder buffer referennce.
    pub fn buffer_as_ref(&self) -> &Vec<u8> {
        &self.buffer
    }
    /// Get the mutable decoder buffer referennce.
    pub fn buffer_as_mut(&mut self) -> &mut Vec<u8> {
        &mut self.buffer
    }
    /// Get the decoder buffer.
    pub fn into_buffer(self) -> Vec<u8> {
        self.buffer
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
/// The header flags structure in the CQL frame.
pub struct HeaderFlags {
    compression: bool,
    tracing: Option<[u8; 16]>,
    custom_payload: bool,
    warnings: Option<Vec<String>>,
    // this not a flag, but it indicates the body start in the buffer.
    body_start: usize,
}

#[allow(dead_code)]
impl HeaderFlags {
    /// Create a new header flags.
    pub fn new(buffer: &Vec<u8>) -> anyhow::Result<Self> {
        let mut body_start = 9;
        let flags = buffer[1];
        let compression = flags & header::COMPRESSION == header::COMPRESSION;
        let tracing;
        if flags & header::TRACING == header::TRACING {
            let mut tracing_id = [0; 16];
            tracing_id.copy_from_slice(&buffer[9..25]);
            tracing = Some(tracing_id);
            // add tracing_id length = 16
            body_start += 16;
        } else {
            tracing = None;
        }
        let warnings = if flags & header::WARNING == header::WARNING {
            let string_list = string_list(&buffer[body_start..])?;
            // add all [short] length to the body_start
            body_start += 2 * (string_list.len() + 1);
            // add the warning length
            for warning in &string_list {
                // add the warning.len to the body_start
                body_start += warning.len();
            }
            Some(string_list)
        } else {
            None
        };
        let custom_payload = flags & header::CUSTOM_PAYLOAD == header::CUSTOM_PAYLOAD;
        Ok(Self {
            compression,
            tracing,
            warnings,
            custom_payload,
            body_start,
        })
    }
    /// Get whether the frame is compressed.
    pub fn compression(&self) -> bool {
        self.compression
    }
    /// Take the tracing id of the frame.
    pub fn take_tracing_id(&mut self) -> Option<[u8; 16]> {
        self.tracing.take()
    }
    /// Take the warnings of the frame.
    fn take_warnings(&mut self) -> Option<Vec<String>> {
        self.warnings.take()
    }
}

impl Frame for Decoder {
    fn version(&self) -> anyhow::Result<u8> {
        let buffer = self.buffer_as_ref();
        ensure!(buffer.len() > 0, "Buffer is too small!");
        Ok(buffer[0])
    }
    fn flags(&self) -> &HeaderFlags {
        &self.header_flags
    }
    fn stream(&self) -> anyhow::Result<i16> {
        todo!()
    }
    fn opcode(&self) -> anyhow::Result<u8> {
        let buffer = self.buffer_as_ref();
        ensure!(buffer.len() > 4, "Buffer is too small!");
        Ok(buffer[4])
    }
    fn length(&self) -> anyhow::Result<usize> {
        let buffer = self.buffer_as_ref();
        ensure!(buffer.len() >= 9, "Buffer is too small!");
        Ok(i32::from_be_bytes(buffer[5..9].try_into()?) as usize)
    }
    fn body(&self) -> anyhow::Result<&[u8]> {
        let buffer = self.buffer_as_ref();
        let body_start = self.header_flags.body_start;
        ensure!(buffer.len() >= body_start, "Buffer is too small!");
        Ok(&buffer[body_start..])
    }
    fn body_start(&self, padding: usize) -> usize {
        self.header_flags.body_start + padding
    }
    fn body_kind(&self) -> anyhow::Result<i32> {
        Ok(i32::from_be_bytes(self.body()?[0..4].try_into()?))
    }
    fn is_authenticate(&self) -> anyhow::Result<bool> {
        Ok(self.opcode()? == opcode::AUTHENTICATE)
    }
    fn is_auth_challenge(&self) -> anyhow::Result<bool> {
        Ok(self.opcode()? == opcode::AUTH_CHALLENGE)
    }
    fn is_auth_success(&self) -> anyhow::Result<bool> {
        Ok(self.opcode()? == opcode::AUTH_SUCCESS)
    }
    fn is_supported(&self) -> anyhow::Result<bool> {
        Ok(self.opcode()? == opcode::SUPPORTED)
    }
    fn is_ready(&self) -> anyhow::Result<bool> {
        Ok(self.opcode()? == opcode::READY)
    }
    fn is_void(&self) -> anyhow::Result<bool> {
        Ok((self.opcode()? == opcode::RESULT) && (self.body_kind()? == result::VOID))
    }
    fn is_rows(&self) -> anyhow::Result<bool> {
        Ok((self.opcode()? == opcode::RESULT) && (self.body_kind()? == result::ROWS))
    }
    fn is_error(&self) -> anyhow::Result<bool> {
        Ok(self.opcode()? == opcode::ERROR)
    }
    fn get_error(&self) -> anyhow::Result<error::CqlError> {
        if self.is_error()? {
            error::CqlError::new(self)
        } else {
            Err(anyhow!("Not error"))
        }
    }
    fn get_void(&self) -> anyhow::Result<()> {
        if self.is_void()? {
            Ok(())
        } else {
            Err(anyhow!("Not void"))
        }
    }
    fn is_unprepared(&self) -> anyhow::Result<bool> {
        Ok(self.opcode()? == opcode::ERROR && self.body_kind()? == error::UNPREPARED)
    }
    fn is_already_exists(&self) -> anyhow::Result<bool> {
        Ok(self.opcode()? == opcode::ERROR && self.body_kind()? == error::ALREADY_EXISTS)
    }
    fn is_configure_error(&self) -> anyhow::Result<bool> {
        Ok(self.opcode()? == opcode::ERROR && self.body_kind()? == error::CONFIGURE_ERROR)
    }
    fn is_invalid(&self) -> anyhow::Result<bool> {
        Ok(self.opcode()? == opcode::ERROR && self.body_kind()? == error::INVALID)
    }
    fn is_unauthorized(&self) -> anyhow::Result<bool> {
        Ok(self.opcode()? == opcode::ERROR && self.body_kind()? == error::UNAUTHORIZED)
    }
    fn is_syntax_error(&self) -> anyhow::Result<bool> {
        Ok(self.opcode()? == opcode::ERROR && self.body_kind()? == error::SYNTAX_ERROR)
    }
    fn is_write_failure(&self) -> anyhow::Result<bool> {
        Ok(self.opcode()? == opcode::ERROR && self.body_kind()? == error::WRITE_FAILURE)
    }
    fn is_function_failure(&self) -> anyhow::Result<bool> {
        Ok(self.opcode()? == opcode::ERROR && self.body_kind()? == error::FUNCTION_FAILURE)
    }
    fn is_read_failure(&self) -> anyhow::Result<bool> {
        Ok(self.opcode()? == opcode::ERROR && self.body_kind()? == error::READ_FAILURE)
    }
    fn is_read_timeout(&self) -> anyhow::Result<bool> {
        Ok(self.opcode()? == opcode::ERROR && self.body_kind()? == error::READ_TIMEOUT)
    }
    fn is_write_timeout(&self) -> anyhow::Result<bool> {
        Ok(self.opcode()? == opcode::ERROR && self.body_kind()? == error::WRITE_TIMEOUT)
    }
    fn is_truncate_error(&self) -> anyhow::Result<bool> {
        Ok(self.opcode()? == opcode::ERROR && self.body_kind()? == error::TRUNCATE_ERROR)
    }
    fn is_boostrapping(&self) -> anyhow::Result<bool> {
        Ok(self.opcode()? == opcode::ERROR && self.body_kind()? == error::IS_BOOSTRAPPING)
    }
    fn is_overloaded(&self) -> anyhow::Result<bool> {
        Ok(self.opcode()? == opcode::ERROR && self.body_kind()? == error::OVERLOADED)
    }
    fn is_unavailable_exception(&self) -> anyhow::Result<bool> {
        Ok(self.opcode()? == opcode::ERROR && self.body_kind()? == error::UNAVAILABLE_EXCEPTION)
    }
    fn is_authentication_error(&self) -> anyhow::Result<bool> {
        Ok(self.opcode()? == opcode::ERROR && self.body_kind()? == error::AUTHENTICATION_ERROR)
    }
    fn is_protocol_error(&self) -> anyhow::Result<bool> {
        Ok(self.opcode()? == opcode::ERROR && self.body_kind()? == error::PROTOCOL_ERROR)
    }
    fn is_server_error(&self) -> anyhow::Result<bool> {
        Ok(self.opcode()? == opcode::ERROR && self.body_kind()? == error::SERVER_ERROR)
    }
    fn rows_flags(&self) -> anyhow::Result<Flags> {
        // cql rows specs, flags is [int] and protocol is big-endian
        let buffer = self.buffer_as_ref();
        ensure!(buffer.len() >= self.body_start(8), "Buffer is too small!");
        let flags = i32::from_be_bytes(buffer[self.body_start(4)..self.body_start(8)].try_into()?);
        Ok(Flags::from_i32(flags))
    }
    fn columns_count(&self) -> anyhow::Result<ColumnsCount> {
        let buffer = self.buffer_as_ref();
        ensure!(buffer.len() >= self.body_start(12), "Buffer is too small!");
        // column count located right after flags, therefore
        Ok(i32::from_be_bytes(
            buffer[self.body_start(8)..self.body_start(12)].try_into()?,
        ))
    }
    fn paging_state(&self, has_more_pages: bool) -> anyhow::Result<PagingState> {
        let paging_state_bytes_start = self.body_start(12);
        let buffer = self.buffer_as_ref();
        Ok(if has_more_pages {
            // decode PagingState
            let paging_state_value_start = paging_state_bytes_start + 4;
            ensure!(buffer.len() >= paging_state_value_start, "Buffer is too small!");
            let paging_state_len =
                i32::from_be_bytes(buffer[paging_state_bytes_start..paging_state_value_start].try_into()?);
            if paging_state_len == -1 {
                PagingState::new(None, paging_state_value_start)
            } else {
                let paging_state_end: usize = paging_state_value_start + (paging_state_len as usize);
                ensure!(buffer.len() >= paging_state_end, "Buffer is too small!");
                PagingState::new(
                    Some(buffer[paging_state_value_start..paging_state_end].to_vec()),
                    paging_state_end,
                )
            }
        } else {
            PagingState::new(None, paging_state_bytes_start)
        })
    }
    fn metadata(&self) -> anyhow::Result<Metadata> {
        let flags = self.rows_flags()?;
        let columns_count = self.columns_count()?;
        let paging_state = self.paging_state(flags.has_more_pages())?;
        Ok(Metadata::new(flags, columns_count, paging_state))
    }
}

// TODO remove length, and make sure slice.len() is more than enough.

/// The column decoder trait to decode the frame.
pub trait ColumnDecoder {
    /// Decode the column.
    fn try_decode_column(slice: &[u8]) -> anyhow::Result<Self>
    where
        Self: Sized;
}

impl<T: ColumnDecoder> ColumnDecoder for Option<T> {
    fn try_decode_column(slice: &[u8]) -> anyhow::Result<Self> {
        if slice.is_empty() {
            Ok(None)
        } else {
            T::try_decode_column(slice).map(Into::into)
        }
    }
}

impl ColumnDecoder for i64 {
    fn try_decode_column(slice: &[u8]) -> anyhow::Result<Self> {
        Ok(i64::from_be_bytes(slice.try_into()?))
    }
}

impl ColumnDecoder for u64 {
    fn try_decode_column(slice: &[u8]) -> anyhow::Result<Self> {
        Ok(u64::from_be_bytes(slice.try_into()?))
    }
}

impl ColumnDecoder for f64 {
    fn try_decode_column(slice: &[u8]) -> anyhow::Result<Self> {
        Ok(f64::from_be_bytes(slice.try_into()?))
    }
}

impl ColumnDecoder for i32 {
    fn try_decode_column(slice: &[u8]) -> anyhow::Result<Self> {
        Ok(i32::from_be_bytes(slice.try_into()?))
    }
}

impl ColumnDecoder for u32 {
    fn try_decode_column(slice: &[u8]) -> anyhow::Result<Self> {
        Ok(u32::from_be_bytes(slice.try_into()?))
    }
}

impl ColumnDecoder for f32 {
    fn try_decode_column(slice: &[u8]) -> anyhow::Result<Self> {
        Ok(f32::from_be_bytes(slice.try_into()?))
    }
}

impl ColumnDecoder for i16 {
    fn try_decode_column(slice: &[u8]) -> anyhow::Result<Self> {
        Ok(i16::from_be_bytes(slice.try_into()?))
    }
}

impl ColumnDecoder for u16 {
    fn try_decode_column(slice: &[u8]) -> anyhow::Result<Self> {
        Ok(u16::from_be_bytes(slice.try_into()?))
    }
}

impl ColumnDecoder for i8 {
    fn try_decode_column(slice: &[u8]) -> anyhow::Result<Self> {
        Ok(i8::from_be_bytes(slice.try_into()?))
    }
}

impl ColumnDecoder for u8 {
    fn try_decode_column(slice: &[u8]) -> anyhow::Result<Self> {
        Ok(slice[0])
    }
}

impl ColumnDecoder for String {
    fn try_decode_column(slice: &[u8]) -> anyhow::Result<Self> {
        Ok(String::from_utf8(slice.to_vec())?)
    }
}

impl ColumnDecoder for IpAddr {
    fn try_decode_column(slice: &[u8]) -> anyhow::Result<Self> {
        Ok(if slice.len() == 4 {
            IpAddr::V4(Ipv4Addr::try_decode_column(slice)?)
        } else {
            IpAddr::V6(Ipv6Addr::try_decode_column(slice)?)
        })
    }
}

impl ColumnDecoder for Ipv4Addr {
    fn try_decode_column(slice: &[u8]) -> anyhow::Result<Self> {
        Ok(Ipv4Addr::new(slice[0], slice[1], slice[2], slice[3]))
    }
}

impl ColumnDecoder for Ipv6Addr {
    fn try_decode_column(slice: &[u8]) -> anyhow::Result<Self> {
        Ok(Ipv6Addr::new(
            ((slice[0] as u16) << 8) | slice[1] as u16,
            ((slice[2] as u16) << 8) | slice[3] as u16,
            ((slice[4] as u16) << 8) | slice[5] as u16,
            ((slice[6] as u16) << 8) | slice[7] as u16,
            ((slice[8] as u16) << 8) | slice[9] as u16,
            ((slice[10] as u16) << 8) | slice[11] as u16,
            ((slice[12] as u16) << 8) | slice[13] as u16,
            ((slice[14] as u16) << 8) | slice[15] as u16,
        ))
    }
}

impl ColumnDecoder for Cursor<Vec<u8>> {
    fn try_decode_column(slice: &[u8]) -> anyhow::Result<Self> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(slice);
        Ok(Cursor::new(bytes))
    }
}

impl<E> ColumnDecoder for Vec<E>
where
    E: ColumnDecoder,
{
    fn try_decode_column(slice: &[u8]) -> anyhow::Result<Self> {
        let list_len = i32::from_be_bytes(slice[0..4].try_into()?) as usize;
        let mut list: Vec<E> = Vec::new();
        let mut element_start = 4;
        for _ in 0..list_len {
            // decode element byte_size
            let element_value_start = element_start + 4;
            let length = i32::from_be_bytes(slice[element_start..element_value_start].try_into()?) as usize;
            if length > 0 {
                let e = E::try_decode_column(&slice[element_value_start..(element_value_start + length)])?;
                list.push(e);
                // next element start
                element_start = element_value_start + length;
            } else {
                let e = E::try_decode_column(&[])?;
                list.push(e);
                // next element start
                element_start = element_value_start;
            }
        }
        Ok(list)
    }
}

impl<K, V, S> ColumnDecoder for HashMap<K, V, S>
where
    K: Eq + Hash + ColumnDecoder,
    V: ColumnDecoder,
    S: ::std::hash::BuildHasher + Default,
{
    fn try_decode_column(slice: &[u8]) -> anyhow::Result<Self> {
        let map_len = i32::from_be_bytes(slice[0..4].try_into()?) as usize;
        let mut map: HashMap<K, V, S> = HashMap::default();
        let mut pair_start = 4;
        for _ in 0..map_len {
            let k;
            let v;
            // decode key_byte_size
            let length = i32::from_be_bytes(slice[pair_start..][..4].try_into()?) as usize;
            pair_start += 4;
            if length > 0 {
                k = K::try_decode_column(&slice[pair_start..][..length])?;
                // modify pair_start to be the vtype_start
                pair_start += length;
            } else {
                k = K::try_decode_column(&[])?;
            }
            let length = i32::from_be_bytes(slice[pair_start..][..4].try_into()?) as usize;
            pair_start += 4;
            if length > 0 {
                v = V::try_decode_column(&slice[pair_start..][..length])?;
                pair_start += length;
            } else {
                v = V::try_decode_column(&[])?;
            }
            // insert key,value
            map.insert(k, v);
        }
        Ok(map)
    }
}

impl ColumnDecoder for Date<Utc> {
    fn try_decode_column(slice: &[u8]) -> anyhow::Result<Self> {
        let num_days = u32::from_be_bytes(slice.try_into()?);
        let epoch = NaiveDate::from_ymd(1970, 1, 1);
        let dt = Date::<Utc>::from_utc(epoch, Utc);
        Ok(dt
            .checked_add_signed(chrono::Duration::days(num_days as i64))
            .ok_or(anyhow!("Overflowed epoch + duration::days"))?)
    }
}

impl ColumnDecoder for NaiveDate {
    fn try_decode_column(slice: &[u8]) -> anyhow::Result<Self> {
        let num_days = u32::from_be_bytes(slice.try_into()?);
        let epoch = NaiveDate::from_ymd(1970, 1, 1);
        Ok(epoch
            .checked_add_signed(chrono::Duration::days(num_days as i64))
            .ok_or(anyhow!("Overflowed epoch + duration::days"))?)
    }
}

impl ColumnDecoder for DateTime<Utc> {
    fn try_decode_column(slice: &[u8]) -> anyhow::Result<Self> {
        Ok(DateTime::<Utc>::from_utc(NaiveDateTime::try_decode_column(slice)?, Utc))
    }
}

impl ColumnDecoder for NaiveDateTime {
    fn try_decode_column(slice: &[u8]) -> anyhow::Result<Self> {
        let num_of_milliseconds = u64::from_be_bytes(slice.try_into()?);
        let millis_reminder = (num_of_milliseconds % 1000) as u32;
        Ok(NaiveDateTime::from_timestamp(
            (num_of_milliseconds / 1000) as i64,
            millis_reminder * 1000_000,
        ))
    }
}

impl ColumnDecoder for NaiveTime {
    fn try_decode_column(slice: &[u8]) -> anyhow::Result<Self> {
        let num_of_nanos = u64::from_be_bytes(slice.try_into()?);
        let nanos_reminder = num_of_nanos % 1000_000_000;
        Ok(NaiveTime::from_num_seconds_from_midnight(
            (num_of_nanos / 1000_000_000) as u32,
            nanos_reminder as u32,
        ))
    }
}

// helper types decoder functions
/// Get the string list from a u8 slice.
pub fn string_list(slice: &[u8]) -> anyhow::Result<Vec<String>> {
    let list_len = u16::from_be_bytes(slice[0..2].try_into()?) as usize;
    let mut list: Vec<String> = Vec::with_capacity(list_len);
    // current_string_start
    let mut s = 2;
    for _ in 0..list_len {
        // ie first string length is buffer[2..4]
        let string_len = u16::from_be_bytes(slice[s..(s + 2)].try_into()?) as usize;
        s += 2;
        let e = s + string_len;
        let string = String::from_utf8_lossy(&slice[s..e]);
        list.push(string.to_string());
        s = e;
    }
    Ok(list)
}

/// Get the `String` from a u8 slice.
pub fn string(slice: &[u8]) -> anyhow::Result<String> {
    let length = u16::from_be_bytes(slice[0..2].try_into()?) as usize;
    String::try_decode_column(&slice[2..][..length])
}

/// Get the `&str` from a u8 slice.
pub fn str(slice: &[u8]) -> anyhow::Result<&str> {
    let length = u16::from_be_bytes(slice[0..2].try_into()?) as usize;
    str::from_utf8(&slice[2..(2 + length)]).map_err(|e| anyhow!(e))
}

/// Get the vector from byte slice.
pub fn bytes(slice: &[u8]) -> anyhow::Result<Option<Vec<u8>>> {
    let length = i32::from_be_bytes(slice[0..4].try_into()?);
    Ok(if length >= 0 {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&slice[4..(4 + (length as usize))]);
        Some(bytes)
    } else {
        None
    })
}

/// Get the `short_bytes` from a u8 slice.
#[allow(unused)]
pub fn short_bytes(slice: &[u8]) -> anyhow::Result<Vec<u8>> {
    let length = u16::from_be_bytes(slice[0..2].try_into()?) as usize;
    Ok(slice[2..][..length].into())
}

/// Get the `prepared_id` from a u8 slice.
pub fn prepared_id(slice: &[u8]) -> anyhow::Result<[u8; 16]> {
    let length = u16::from_be_bytes(slice[0..2].try_into()?) as usize;
    let res: Result<[u8; 16], _> = slice[2..][..length].try_into();
    res.map_err(|e| anyhow!(e))
}

/// Get hashmap of string to string vector from slice.
pub fn string_multimap(slice: &[u8]) -> anyhow::Result<HashMap<String, Vec<String>>> {
    let length = u16::from_be_bytes(slice[0..2].try_into()?) as usize;
    let mut multimap = HashMap::with_capacity(length);
    let mut i = 2;
    for _ in 0..length {
        let key = string(&slice[i..])?;
        // add [short] + string.len()
        i += 2 + key.len();
        let (list, len) = string_list_with_returned_bytes_length(&slice[i..])?;
        i += len;
        multimap.insert(key, list);
    }
    Ok(multimap)
}

// Usefull for multimap.
/// Get the string list and the byte length from slice.
pub fn string_list_with_returned_bytes_length(slice: &[u8]) -> anyhow::Result<(Vec<String>, usize)> {
    let list_len = u16::from_be_bytes(slice[0..2].try_into()?) as usize;
    let mut list: Vec<String> = Vec::with_capacity(list_len);
    // current_string_start
    let mut s = 2;
    for _ in 0..list_len {
        // ie first string length is buffer[2..4]
        let string_len = u16::from_be_bytes(slice[s..(s + 2)].try_into()?) as usize;
        s += 2;
        let e = s + string_len;
        let string = String::from_utf8_lossy(&slice[s..e]);
        list.push(string.to_string());
        s = e;
    }
    Ok((list, s))
}
// todo inet fn (with port).
// todo inet fn (with port).
