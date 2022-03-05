// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

//! This module implements the frame frame.

use super::{
    Blob,
    IntoIter,
    RowDecoder,
    RowsResult,
};
use anyhow::anyhow;
use chrono::{
    NaiveDate,
    NaiveDateTime,
    NaiveTime,
};
use std::{
    collections::HashMap,
    convert::TryInto,
    hash::Hash,
    net::{
        IpAddr,
        Ipv4Addr,
        Ipv6Addr,
    },
};
/// RowsDecoder trait to decode the rows result from scylla
pub trait RowsDecoder: Sized {
    /// The Row to decode. Must implement [`super::Row`].
    type Row: RowDecoder;
    /// Try to decode the provided Decoder with an expected Rows result
    fn try_decode_rows(result: RowsResult) -> anyhow::Result<Option<Self>>;
    /// Decode the provided Decoder with deterministic Rows result
    fn decode_rows(result: RowsResult) -> Option<Self> {
        Self::try_decode_rows(result).unwrap()
    }
}

impl<T> RowsDecoder for T
where
    T: RowDecoder,
{
    type Row = T;

    fn try_decode_rows(result: RowsResult) -> anyhow::Result<Option<Self>> {
        Ok(result.into_iter().next())
    }
}

impl<T> RowsDecoder for IntoIter<T>
where
    T: RowDecoder,
{
    type Row = T;

    fn try_decode_rows(result: RowsResult) -> anyhow::Result<Option<Self>> {
        let rows_iter = result.into_iter();
        if rows_iter.is_empty() && !rows_iter.has_more_pages() {
            Ok(None)
        } else {
            Ok(Some(rows_iter))
        }
    }
}

/// The column frame trait to decode the frame.
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

impl ColumnDecoder for Blob {
    fn try_decode_column(slice: &[u8]) -> anyhow::Result<Self> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(slice);
        Ok(Blob(bytes))
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

impl ColumnDecoder for NaiveDate {
    fn try_decode_column(slice: &[u8]) -> anyhow::Result<Self> {
        let num_days = u32::from_be_bytes(slice.try_into()?) - (1u32 << 31);
        let epoch = NaiveDate::from_ymd(1970, 1, 1);
        Ok(epoch
            .checked_add_signed(chrono::Duration::days(num_days as i64))
            .ok_or(anyhow!("Overflowed epoch + duration::days"))?)
    }
}

impl ColumnDecoder for NaiveTime {
    fn try_decode_column(slice: &[u8]) -> anyhow::Result<Self> {
        let nanos = u64::from_be_bytes(slice.try_into()?);
        let (secs, nanos) = (nanos / 1_000_000_000, nanos % 1_000_000_000);
        Ok(NaiveTime::from_num_seconds_from_midnight(secs as u32, nanos as u32))
    }
}

impl ColumnDecoder for NaiveDateTime {
    fn try_decode_column(slice: &[u8]) -> anyhow::Result<Self> {
        let millis = u64::from_be_bytes(slice.try_into()?);
        let (secs, nanos) = (millis / 1_000, millis % 1_000 * 1_000_000);
        Ok(NaiveDateTime::from_timestamp(secs as i64, nanos as u32))
    }
}
