// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

pub mod delete;
pub mod insert;
pub mod keyspace;
pub mod select;
pub mod update;

use super::Worker;
use keyspace::Keyspace;
use scylla_cql::{CqlError, Decoder, Frame, Query, RowsDecoder, VoidDecoder};
use std::{
    marker::PhantomData,
    ops::{Deref, DerefMut},
};

#[repr(u8)]
#[derive(Clone)]
enum RequestType {
    Insert = 0,
    Update = 1,
    Delete = 2,
    Select = 3,
}

#[derive(Clone, Copy, Default)]
pub struct DecodeRows<S, K, V> {
    _marker: PhantomData<(S, K, V)>
}

#[derive(Copy, Clone, Default)]
pub struct DecodeVoid<S> {
    _marker: PhantomData<S>
}

#[derive(Clone)]
pub struct DecodeResult<T> {
    inner: T,
    request_type: RequestType,
}
