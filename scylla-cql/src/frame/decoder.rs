// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

//! This module implements the frame decoder.

use super::{
    error, header, opcode, result,
    rows::{ColumnsCount, Flags, Metadata, PagingState},
};
use crate::{
    compression::{Compression, MyCompression},
    frame::rows::{Row, Rows},
};
use std::{
    collections::HashMap,
    convert::TryInto,
    hash::Hash,
    io::Cursor,
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
    str,
};
/// RowsDecoder trait to decode the rows result from scylla
pub trait RowsDecoder<K, V> {
    type Row: super::Row;
    /// Try to decode the provided Decoder with an expected Rows result
    fn try_decode(decoder: Decoder) -> Result<Option<V>, error::CqlError>;
    /// Decode the provided Decoder with deterministic Rows result
    fn decode(decoder: Decoder) -> Option<V> {
        Self::try_decode(decoder).unwrap()
    }
}

/// VoidDecoder trait to decode the VOID result from scylla
pub trait VoidDecoder {
    /// Try to decode the provided Decoder with an expected Void result
    fn try_decode(decoder: Decoder) -> Result<(), error::CqlError> {
        if decoder.is_error() {
            Err(decoder.get_error())
        } else {
            Ok(())
        }
    }
    /// Decode the provided Decoder with an deterministic Void result
    fn decode(decoder: Decoder) {
        Self::try_decode(decoder).unwrap()
    }
}

impl From<Vec<u8>> for Decoder {
    fn from(buffer: Vec<u8>) -> Self {
        Decoder::new(buffer, MyCompression::get())
    }
}

/// The CQL frame trait.
pub trait Frame {
    /// Get the frame version.
    fn version(&self) -> u8;
    /// G the frame header flags.
    fn flags(&self) -> &HeaderFlags;
    /// Get the stream in the frame header.
    fn stream(&self) -> i16;
    /// Get the opcode in the frame header.
    fn opcode(&self) -> u8;
    /// Get the length of the frame body.
    fn length(&self) -> usize;
    /// Get the frame body.
    fn body(&self) -> &[u8];
    /// Get the body start of the frame.
    fn body_start(&self, padding: usize) -> usize;
    /// Get the body kind.
    fn body_kind(&self) -> i32;
    /// Check whether the opcode is `AUTHENTICATE`.
    fn is_authenticate(&self) -> bool;
    /// Check whether the opcode is `AUTH_CHALLENGE`.
    fn is_auth_challenge(&self) -> bool;
    /// Check whether the opcode is `AUTH_SUCCESS`.
    fn is_auth_success(&self) -> bool;
    /// Check whether the opcode is `SUPPORTED`.
    fn is_supported(&self) -> bool;
    /// Check whether the opcode is `READY`.
    fn is_ready(&self) -> bool;
    /// Check whether the body kind is `VOID`.
    fn is_void(&self) -> bool;
    /// Check whether the body kind is `ROWS`.
    fn is_rows(&self) -> bool;
    /// Check whether the opcode is `ERROR`.
    fn is_error(&self) -> bool;
    /// Get the `CqlError`.
    fn get_error(&self) -> error::CqlError;
    /// Get Void `()`
    fn get_void(&self) -> Result<(), error::CqlError>;
    /// Check whether the error is `UNPREPARED`.
    fn is_unprepared(&self) -> bool;
    /// Check whether the error is `ALREADY_EXISTS.
    fn is_already_exists(&self) -> bool;
    /// Check whether the error is `CONFIGURE_ERROR.
    fn is_configure_error(&self) -> bool;
    /// Check whether the error is `INVALID.
    fn is_invalid(&self) -> bool;
    /// Check whether the error is `UNAUTHORIZED.
    fn is_unauthorized(&self) -> bool;
    /// Check whether the error is `SYNTAX_ERROR.
    fn is_syntax_error(&self) -> bool;
    /// Check whether the error is `WRITE_FAILURE.
    fn is_write_failure(&self) -> bool;
    /// Check whether the error is `FUNCTION_FAILURE.
    fn is_function_failure(&self) -> bool;
    /// Check whether the error is `READ_FAILURE.
    fn is_read_failure(&self) -> bool;
    /// Check whether the error is `READ_TIMEOUT.
    fn is_read_timeout(&self) -> bool;
    /// Check whether the error is `WRITE_TIMEOUT.
    fn is_write_timeout(&self) -> bool;
    /// Check whether the error is `TRUNCATE_ERROR.
    fn is_truncate_error(&self) -> bool;
    /// Check whether the error is `IS_BOOSTRAPPING.
    fn is_boostrapping(&self) -> bool;
    /// Check whether the error is `OVERLOADED.
    fn is_overloaded(&self) -> bool;
    /// Check whether the error is `UNAVAILABLE_EXCEPTION.
    fn is_unavailable_exception(&self) -> bool;
    /// Check whether the error is `AUTHENTICATION_ERROR.
    fn is_authentication_error(&self) -> bool;
    /// Check whether the error is `PROTOCOL_ERROR.
    fn is_protocol_error(&self) -> bool;
    /// Check whether the error is `SERVER_ERROR.
    fn is_server_error(&self) -> bool;
    /// The the row flags.
    fn rows_flags(&self) -> Flags;
    /// The the column counts.
    fn columns_count(&self) -> ColumnsCount;
    /// The the paging state.
    fn paging_state(&self, has_more_pages: bool) -> PagingState;
    /// The the metadata.
    fn metadata(&self) -> Metadata;
}
/// The frame decoder structure.
#[derive(Clone)]
pub struct Decoder {
    buffer: Vec<u8>,
    header_flags: HeaderFlags,
}
impl Decoder {
    /// Create a new decoder with an assigned compression type.
    pub fn new(mut buffer: Vec<u8>, decompressor: impl Compression) -> Self {
        buffer = decompressor.decompress(buffer);
        let header_flags = HeaderFlags::new(&mut buffer);
        Decoder { buffer, header_flags }
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
#[derive(Clone)]
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
    pub fn new(buffer: &mut Vec<u8>) -> Self {
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
            let string_list = string_list(&buffer[body_start..]);
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
        Self {
            compression,
            tracing,
            warnings,
            custom_payload,
            body_start,
        }
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
    fn version(&self) -> u8 {
        self.buffer_as_ref()[0]
    }
    fn flags(&self) -> &HeaderFlags {
        &self.header_flags
    }
    fn stream(&self) -> i16 {
        todo!()
    }
    fn opcode(&self) -> u8 {
        self.buffer_as_ref()[4]
    }
    fn length(&self) -> usize {
        i32::from_be_bytes(self.buffer_as_ref()[5..9].try_into().unwrap()) as usize
    }
    fn body(&self) -> &[u8] {
        let body_start = self.header_flags.body_start;
        &self.buffer_as_ref()[body_start..]
    }
    fn body_start(&self, padding: usize) -> usize {
        self.header_flags.body_start + padding
    }
    fn body_kind(&self) -> i32 {
        i32::from_be_bytes(self.body()[0..4].try_into().unwrap())
    }
    fn is_authenticate(&self) -> bool {
        self.opcode() == opcode::AUTHENTICATE
    }
    fn is_auth_challenge(&self) -> bool {
        self.opcode() == opcode::AUTH_CHALLENGE
    }
    fn is_auth_success(&self) -> bool {
        self.opcode() == opcode::AUTH_SUCCESS
    }
    fn is_supported(&self) -> bool {
        self.opcode() == opcode::SUPPORTED
    }
    fn is_ready(&self) -> bool {
        self.opcode() == opcode::READY
    }
    fn is_void(&self) -> bool {
        (self.opcode() == opcode::RESULT) && (self.body_kind() == result::VOID)
    }
    fn is_rows(&self) -> bool {
        (self.opcode() == opcode::RESULT) && (self.body_kind() == result::ROWS)
    }
    fn is_error(&self) -> bool {
        self.opcode() == opcode::ERROR
    }
    fn get_error(&self) -> error::CqlError {
        error::CqlError::new(self)
    }
    fn get_void(&self) -> Result<(), error::CqlError> {
        if self.is_void() {
            Ok(())
        } else {
            Err(self.get_error())
        }
    }
    fn is_unprepared(&self) -> bool {
        self.opcode() == opcode::ERROR && self.body_kind() == error::UNPREPARED
    }
    fn is_already_exists(&self) -> bool {
        self.opcode() == opcode::ERROR && self.body_kind() == error::ALREADY_EXISTS
    }
    fn is_configure_error(&self) -> bool {
        self.opcode() == opcode::ERROR && self.body_kind() == error::CONFIGURE_ERROR
    }
    fn is_invalid(&self) -> bool {
        self.opcode() == opcode::ERROR && self.body_kind() == error::INVALID
    }
    fn is_unauthorized(&self) -> bool {
        self.opcode() == opcode::ERROR && self.body_kind() == error::UNAUTHORIZED
    }
    fn is_syntax_error(&self) -> bool {
        self.opcode() == opcode::ERROR && self.body_kind() == error::SYNTAX_ERROR
    }
    fn is_write_failure(&self) -> bool {
        self.opcode() == opcode::ERROR && self.body_kind() == error::WRITE_FAILURE
    }
    fn is_function_failure(&self) -> bool {
        self.opcode() == opcode::ERROR && self.body_kind() == error::FUNCTION_FAILURE
    }
    fn is_read_failure(&self) -> bool {
        self.opcode() == opcode::ERROR && self.body_kind() == error::READ_FAILURE
    }
    fn is_read_timeout(&self) -> bool {
        self.opcode() == opcode::ERROR && self.body_kind() == error::READ_TIMEOUT
    }
    fn is_write_timeout(&self) -> bool {
        self.opcode() == opcode::ERROR && self.body_kind() == error::WRITE_TIMEOUT
    }
    fn is_truncate_error(&self) -> bool {
        self.opcode() == opcode::ERROR && self.body_kind() == error::TRUNCATE_ERROR
    }
    fn is_boostrapping(&self) -> bool {
        self.opcode() == opcode::ERROR && self.body_kind() == error::IS_BOOSTRAPPING
    }
    fn is_overloaded(&self) -> bool {
        self.opcode() == opcode::ERROR && self.body_kind() == error::OVERLOADED
    }
    fn is_unavailable_exception(&self) -> bool {
        self.opcode() == opcode::ERROR && self.body_kind() == error::UNAVAILABLE_EXCEPTION
    }
    fn is_authentication_error(&self) -> bool {
        self.opcode() == opcode::ERROR && self.body_kind() == error::AUTHENTICATION_ERROR
    }
    fn is_protocol_error(&self) -> bool {
        self.opcode() == opcode::ERROR && self.body_kind() == error::PROTOCOL_ERROR
    }
    fn is_server_error(&self) -> bool {
        self.opcode() == opcode::ERROR && self.body_kind() == error::SERVER_ERROR
    }
    fn rows_flags(&self) -> Flags {
        // cql rows specs, flags is [int] and protocol is big-endian
        let flags = i32::from_be_bytes(
            self.buffer_as_ref()[self.body_start(4)..self.body_start(8)]
                .try_into()
                .unwrap(),
        );
        Flags::from_i32(flags)
    }
    fn columns_count(&self) -> ColumnsCount {
        // column count located right after flags, therefore
        i32::from_be_bytes(
            self.buffer_as_ref()[self.body_start(8)..self.body_start(12)]
                .try_into()
                .unwrap(),
        )
    }
    fn paging_state(&self, has_more_pages: bool) -> PagingState {
        let paging_state_bytes_start = self.body_start(12);
        if has_more_pages {
            // decode PagingState
            let paging_state_value_start = paging_state_bytes_start + 4;
            let paging_state_len = i32::from_be_bytes(
                self.buffer_as_ref()[paging_state_bytes_start..paging_state_value_start]
                    .try_into()
                    .unwrap(),
            );
            if paging_state_len == -1 {
                PagingState::new(None, paging_state_value_start)
            } else {
                let paging_state_end: usize = paging_state_value_start + (paging_state_len as usize);
                PagingState::new(
                    Some(self.buffer_as_ref()[paging_state_value_start..paging_state_end].to_vec()),
                    paging_state_end,
                )
            }
        } else {
            PagingState::new(None, paging_state_bytes_start)
        }
    }
    fn metadata(&self) -> Metadata {
        let flags = self.rows_flags();
        let columns_count = self.columns_count();
        let paging_state = self.paging_state(flags.has_more_pages());
        Metadata::new(flags, columns_count, paging_state)
    }
}

// TODO remove length, and make sure slice.len() is more than enough.

/// The column decoder trait to decode the frame.
pub trait ColumnDecoder {
    /// Decode the column.
    fn decode(slice: &[u8]) -> Self;
}

impl<T: ColumnDecoder> ColumnDecoder for Option<T> {
    fn decode(slice: &[u8]) -> Self {
        if slice.is_empty() {
            None
        } else {
            Some(T::decode(slice))
        }
    }
}

impl ColumnDecoder for i64 {
    fn decode(slice: &[u8]) -> i64 {
        i64::from_be_bytes(slice.try_into().unwrap())
    }
}

impl ColumnDecoder for u64 {
    fn decode(slice: &[u8]) -> u64 {
        u64::from_be_bytes(slice.try_into().unwrap())
    }
}

impl ColumnDecoder for f64 {
    fn decode(slice: &[u8]) -> f64 {
        f64::from_be_bytes(slice.try_into().unwrap())
    }
}

impl ColumnDecoder for i32 {
    fn decode(slice: &[u8]) -> i32 {
        i32::from_be_bytes(slice.try_into().unwrap())
    }
}

impl ColumnDecoder for u32 {
    fn decode(slice: &[u8]) -> u32 {
        u32::from_be_bytes(slice.try_into().unwrap())
    }
}

impl ColumnDecoder for f32 {
    fn decode(slice: &[u8]) -> f32 {
        f32::from_be_bytes(slice.try_into().unwrap())
    }
}

impl ColumnDecoder for i16 {
    fn decode(slice: &[u8]) -> i16 {
        i16::from_be_bytes(slice.try_into().unwrap())
    }
}

impl ColumnDecoder for u16 {
    fn decode(slice: &[u8]) -> u16 {
        u16::from_be_bytes(slice.try_into().unwrap())
    }
}

impl ColumnDecoder for i8 {
    fn decode(slice: &[u8]) -> i8 {
        i8::from_be_bytes(slice.try_into().unwrap())
    }
}

impl ColumnDecoder for u8 {
    fn decode(slice: &[u8]) -> u8 {
        slice[0]
    }
}

impl ColumnDecoder for String {
    fn decode(slice: &[u8]) -> String {
        String::from_utf8(slice.to_vec()).unwrap()
    }
}

impl ColumnDecoder for IpAddr {
    fn decode(slice: &[u8]) -> Self {
        if slice.len() == 4 {
            IpAddr::V4(Ipv4Addr::decode(slice))
        } else {
            IpAddr::V6(Ipv6Addr::decode(slice))
        }
    }
}

impl ColumnDecoder for Ipv4Addr {
    fn decode(slice: &[u8]) -> Self {
        Ipv4Addr::new(slice[0], slice[1], slice[2], slice[3])
    }
}

impl ColumnDecoder for Ipv6Addr {
    fn decode(slice: &[u8]) -> Self {
        Ipv6Addr::new(
            ((slice[0] as u16) << 8) | slice[1] as u16,
            ((slice[2] as u16) << 8) | slice[3] as u16,
            ((slice[4] as u16) << 8) | slice[5] as u16,
            ((slice[6] as u16) << 8) | slice[7] as u16,
            ((slice[8] as u16) << 8) | slice[9] as u16,
            ((slice[10] as u16) << 8) | slice[11] as u16,
            ((slice[12] as u16) << 8) | slice[13] as u16,
            ((slice[14] as u16) << 8) | slice[15] as u16,
        )
    }
}

impl ColumnDecoder for Cursor<Vec<u8>> {
    fn decode(slice: &[u8]) -> Self {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(slice);
        Cursor::new(bytes)
    }
}

impl<E> ColumnDecoder for Vec<E>
where
    E: ColumnDecoder,
{
    fn decode(slice: &[u8]) -> Vec<E> {
        let list_len = i32::from_be_bytes(slice[0..4].try_into().unwrap()) as usize;
        let mut list: Vec<E> = Vec::new();
        let mut element_start = 4;
        for _ in 0..list_len {
            // decode element byte_size
            let element_value_start = element_start + 4;
            let length = i32::from_be_bytes(slice[element_start..element_value_start].try_into().unwrap()) as usize;
            if length > 0 {
                let e = E::decode(&slice[element_value_start..(element_value_start + length)]);
                list.push(e);
                // next element start
                element_start = element_value_start + length;
            } else {
                let e = E::decode(&[]);
                list.push(e);
                // next element start
                element_start = element_value_start;
            }
        }
        list
    }
}

impl<K, V, S> ColumnDecoder for HashMap<K, V, S>
where
    K: Eq + Hash + ColumnDecoder,
    V: ColumnDecoder,
    S: ::std::hash::BuildHasher + Default,
{
    fn decode(slice: &[u8]) -> HashMap<K, V, S> {
        let map_len = i32::from_be_bytes(slice[0..4].try_into().unwrap()) as usize;
        let mut map: HashMap<K, V, S> = HashMap::default();
        let mut pair_start = 4;
        for _ in 0..map_len {
            let k;
            let v;
            // decode key_byte_size
            let length = i32::from_be_bytes(slice[pair_start..][..4].try_into().unwrap()) as usize;
            pair_start += 4;
            if length > 0 {
                k = K::decode(&slice[pair_start..][..length]);
                // modify pair_start to be the vtype_start
                pair_start += length;
            } else {
                k = K::decode(&[]);
            }
            let length = i32::from_be_bytes(slice[pair_start..][..4].try_into().unwrap()) as usize;
            pair_start += 4;
            if length > 0 {
                v = V::decode(&slice[pair_start..][..length]);
                pair_start += length;
            } else {
                v = V::decode(&[]);
            }
            // insert key,value
            map.insert(k, v);
        }
        map
    }
}

// helper types decoder functions
/// Get the string list from a u8 slice.
pub fn string_list(slice: &[u8]) -> Vec<String> {
    let list_len = u16::from_be_bytes(slice[0..2].try_into().unwrap()) as usize;
    let mut list: Vec<String> = Vec::with_capacity(list_len);
    // current_string_start
    let mut s = 2;
    for _ in 0..list_len {
        // ie first string length is buffer[2..4]
        let string_len = u16::from_be_bytes(slice[s..(s + 2)].try_into().unwrap()) as usize;
        s += 2;
        let e = s + string_len;
        let string = String::from_utf8_lossy(&slice[s..e]);
        list.push(string.to_string());
        s = e;
    }
    list
}

/// Get the `String` from a u8 slice.
pub fn string(slice: &[u8]) -> String {
    let length = u16::from_be_bytes(slice[0..2].try_into().unwrap()) as usize;
    String::decode(&slice[2..][..length])
}

/// Get the `&str` from a u8 slice.
pub fn str(slice: &[u8]) -> &str {
    let length = u16::from_be_bytes(slice[0..2].try_into().unwrap()) as usize;
    str::from_utf8(&slice[2..(2 + length)]).unwrap()
}

/// Get the vector from byte slice.
pub fn bytes(slice: &[u8]) -> Option<Vec<u8>> {
    let length = i32::from_be_bytes(slice[0..4].try_into().unwrap());
    if length >= 0 {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&slice[4..(4 + (length as usize))]);
        Some(bytes)
    } else {
        None
    }
}

/// Get the `short_bytes` from a u8 slice.
#[allow(unused)]
pub fn short_bytes(slice: &[u8]) -> Vec<u8> {
    let length = u16::from_be_bytes(slice[0..2].try_into().unwrap()) as usize;
    slice[2..][..length].into()
}

/// Get the `prepared_id` from a u8 slice.
pub fn prepared_id(slice: &[u8]) -> [u8; 16] {
    let length = u16::from_be_bytes(slice[0..2].try_into().unwrap()) as usize;
    slice[2..][..length].try_into().unwrap()
}

/// Get hashmap of string to string vector from slice.
pub fn string_multimap(slice: &[u8]) -> HashMap<String, Vec<String>> {
    let length = u16::from_be_bytes(slice[0..2].try_into().unwrap()) as usize;
    let mut multimap = HashMap::with_capacity(length);
    let mut i = 2;
    for _ in 0..length {
        let key = string(&slice[i..]);
        // add [short] + string.len()
        i += 2 + key.len();
        let (list, len) = string_list_with_returned_bytes_length(&slice[i..]);
        i += len;
        multimap.insert(key, list);
    }
    multimap
}

// Usefull for multimap.
/// Get the string list and the byte length from slice.
pub fn string_list_with_returned_bytes_length(slice: &[u8]) -> (Vec<String>, usize) {
    let list_len = u16::from_be_bytes(slice[0..2].try_into().unwrap()) as usize;
    let mut list: Vec<String> = Vec::with_capacity(list_len);
    // current_string_start
    let mut s = 2;
    for _ in 0..list_len {
        // ie first string length is buffer[2..4]
        let string_len = u16::from_be_bytes(slice[s..(s + 2)].try_into().unwrap()) as usize;
        s += 2;
        let e = s + string_len;
        let string = String::from_utf8_lossy(&slice[s..e]);
        list.push(string.to_string());
        s = e;
    }
    (list, s)
}
// todo inet fn (with port).
