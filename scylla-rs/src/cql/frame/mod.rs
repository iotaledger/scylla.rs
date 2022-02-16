// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

//! This crate implements decoder/encoder for a Cassandra frame and the associated protocol.
//! See `https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec` for more details.

pub(crate) mod auth_challenge;
pub(crate) mod auth_response;
pub(crate) mod auth_success;
pub(crate) mod authenticate;
pub(crate) mod batch;
pub(crate) mod batchflags;
pub(crate) mod consistency;
pub(crate) mod decoder;
pub(crate) mod encoder;
pub(crate) mod error;
pub(crate) mod header;
pub(crate) mod opcode;
pub(crate) mod options;
pub(crate) mod prepare;
pub(crate) mod query;
pub(crate) mod queryflags;
pub(crate) mod result;
pub(crate) mod rows;
pub(crate) mod startup;
pub(crate) mod supported;

pub use auth_response::{
    AllowAllAuth,
    PasswordAuth,
};
pub use auth_success::AuthSuccess;
pub use batch::*;
pub use consistency::Consistency;
use core::fmt::Debug;
pub use decoder::{
    ColumnDecoder,
    Decoder,
    Frame,
    RowsDecoder,
    VoidDecoder,
};
pub use encoder::{
    ColumnEncoder,
    TokenEncodeChain,
    TokenEncoder,
};
pub use error::{
    CqlError,
    ErrorCodes,
};
pub use prepare::Prepare;
pub use query::{
    Query,
    QueryBuilder,
};
pub use rows::*;
pub use std::convert::TryInto;
use std::ops::{
    Deref,
    DerefMut,
};

#[derive(Debug, Clone)]
pub struct Blob(pub Vec<u8>);

impl Blob {
    pub fn new(data: Vec<u8>) -> Self {
        Blob(data)
    }

    pub fn into_inner(self) -> Vec<u8> {
        self.0
    }
}

impl Deref for Blob {
    type Target = Vec<u8>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Blob {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<Vec<u8>> for Blob {
    fn from(v: Vec<u8>) -> Self {
        Blob(v)
    }
}

/// Big Endian 16-length, used for MD5 ID
const MD5_BE_LENGTH: [u8; 2] = [0, 16];

/// Defines how values are bound to the frame
pub trait Binder {
    type Error: Debug;
    /// Add a single value
    fn value<V: ColumnEncoder>(&mut self, value: &V) -> Result<&mut Self, Self::Error>
    where
        Self: Sized;
    /// Add a single named value
    fn named_value<V: ColumnEncoder>(&mut self, name: &str, value: &V) -> Result<&mut Self, Self::Error>
    where
        Self: Sized;
    /// Add a slice of values
    fn bind<V: Bindable>(&mut self, values: &V) -> Result<&mut Self, Self::Error>
    where
        Self: Sized,
    {
        values.bind(self)?;
        Ok(self)
    }
    /// Unset value
    fn unset_value(&mut self) -> Result<&mut Self, Self::Error>
    where
        Self: Sized;
    /// Set Null value, note: for write queries this will create tombstone for V;
    fn null_value(&mut self) -> Result<&mut Self, Self::Error>
    where
        Self: Sized;
}

/// Defines a query bindable value
pub trait Bindable {
    /// Bind the value using the provided binder
    fn bind<B: Binder>(&self, binder: &mut B) -> Result<(), B::Error>;
}

impl<T: ColumnEncoder> Bindable for T {
    fn bind<B: Binder>(&self, binder: &mut B) -> Result<(), B::Error> {
        binder.value(self);
        Ok(())
    }
}

impl Bindable for () {
    fn bind<B: Binder>(&self, binder: &mut B) -> Result<(), B::Error> {
        Ok(())
    }
}

impl<T: Bindable> Bindable for [T] {
    fn bind<B: Binder>(&self, binder: &mut B) -> Result<(), B::Error> {
        for v in self.iter() {
            v.bind(binder)?;
        }
        Ok(())
    }
}
