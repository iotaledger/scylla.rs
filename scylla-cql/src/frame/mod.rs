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
pub(crate) mod execute;
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

pub use auth_response::{AllowAllAuth, PasswordAuth};
pub use auth_success::AuthSuccess;
pub use batch::*;
pub use consistency::Consistency;
pub use decoder::{ColumnDecoder, Decoder, Frame, RowsDecoder, VoidDecoder};
pub use encoder::ColumnEncoder;
pub use error::{CqlError, ErrorCodes};
pub use execute::Execute;
pub use prepare::Prepare;
pub use query::{
    PreparedStatement, Query, QueryBuild, QueryBuilder, QueryConsistency, QueryFlags, QueryStatement, QueryValues,
};
pub use rows::*;
pub use std::convert::TryInto;

/// Big Endian 16-length, used for MD5 ID
const MD5_BE_LENGTH: [u8; 2] = [0, 16];

/// Statement or ID
pub trait QueryOrPrepared: Sized {
    fn encode_statement<R, T: Statements<R>>(query_or_batch: T, statement: &[u8]) -> R;
    fn is_prepared() -> bool;
}

pub trait Statements<T> {
    fn statement(self, statement: &str) -> T;
    fn id(self, id: &[u8; 16]) -> T;
}

pub trait Values: Sized {
    type Return: Values<Return = Self::Return>;
    /// Value of type V.
    fn value<V: ColumnEncoder>(self, value: &V) -> Self::Return;
    /// Unset value.
    fn unset_value(self) -> Self::Return;
    /// Set Null value, note: for write queries this will create tombstone for V;
    fn null_value(self) -> Self::Return;
}
