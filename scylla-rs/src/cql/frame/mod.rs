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
pub use decoder::{
    ColumnDecoder,
    Decoder,
    Frame,
    RowsDecoder,
    VoidDecoder,
};
pub use encoder::{
    ColumnEncodeChain,
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
    PreparedStatement,
    Query,
    QueryBuild,
    QueryBuilder,
    QueryConsistency,
    QueryFlags,
    QueryPagingState,
    QuerySerialConsistency,
    QueryStatement,
    QueryValues,
};
pub use rows::*;
pub use std::convert::TryInto;
use std::{
    collections::HashMap,
    io::Cursor,
};

use self::encoder::{
    Null,
    Unset,
};

/// Big Endian 16-length, used for MD5 ID
const MD5_BE_LENGTH: [u8; 2] = [0, 16];

/// Statement or ID
pub trait QueryOrPrepared: Sized {
    /// Encode the statement as either a query string or an md5 hash prepared id
    fn encode_statement<T: Statements>(query_or_batch: T, statement: &str) -> T::Return;
    /// Returns whether this is a prepared statement
    fn is_prepared() -> bool;
}

/// Defines shared functionality for frames that can receive statements
pub trait Statements {
    /// The return type after applying a statement
    type Return;
    /// Add a statement to the frame
    fn statement(self, statement: &str) -> Self::Return;
    /// Add a prepared statement id to the frame
    fn id(self, id: &[u8; 16]) -> Self::Return;
}

/// Defines how values are bound to the frame
pub trait Binder {
    /// Add a single value
    fn value<V: ColumnEncoder + Sync>(self, value: V) -> Self
    where
        Self: Sized;
    /// Add a slice of values
    fn bind<V: Bindable<Self> + Sync>(self, values: V) -> Self
    where
        Self: Sized,
    {
        values.bind(self)
    }
    /// Unset value
    fn unset_value(self) -> Self
    where
        Self: Sized;
    /// Set Null value, note: for write queries this will create tombstone for V;
    fn null_value(self) -> Self
    where
        Self: Sized;

    /// Skip binding a value
    fn skip_value(self) -> Self
    where
        Self: Sized,
    {
        self
    }
}

/// Defines a query bindable value
pub trait Bindable<B: Binder> {
    /// Bind the value using the provided binder
    fn bind(&self, binder: B) -> B;
}

macro_rules! impl_token_col_encoder {
    ($($t:ty),*) => {
        $(
            impl<B: Binder> Bindable<B> for $t {
                fn bind(&self, binder: B) -> B {
                    binder.value(self)
                }
            }
        )*
    };
}

impl_token_col_encoder!(
    i8,
    i16,
    i32,
    i64,
    u8,
    u16,
    u32,
    u64,
    f32,
    f64,
    bool,
    String,
    str,
    Cursor<Vec<u8>>,
    Unset,
    Null
);

impl<B: Binder, T: ColumnEncoder + Sync> Bindable<B> for Vec<T> {
    fn bind(&self, binder: B) -> B {
        binder.value(self)
    }
}

impl<B: Binder, K: ColumnEncoder + Sync, V: ColumnEncoder + Sync> Bindable<B> for HashMap<K, V> {
    fn bind(&self, binder: B) -> B {
        binder.value(self)
    }
}

impl<B: Binder, T: ColumnEncoder + Sync> Bindable<B> for Option<T> {
    fn bind(&self, binder: B) -> B {
        binder.value(self)
    }
}

impl<B: Binder, T: Bindable<B> + ?Sized> Bindable<B> for &T {
    fn bind(&self, binder: B) -> B {
        (*self).bind(binder)
    }
}

impl<B: Binder> Bindable<B> for () {
    fn bind(&self, binder: B) -> B {
        binder.skip_value()
    }
}

impl<B: Binder, T: Bindable<B> + Sync> Bindable<B> for [T] {
    fn bind(&self, mut binder: B) -> B {
        for v in self.iter() {
            binder = binder.bind(v);
        }
        binder
    }
}

macro_rules! impl_tuple_bind {
    ($(($t:tt, $n:tt)),*) => {
        impl<B: Binder, $($t: Bindable<B> + Sync),*> Bindable<B> for ($(&$t),*,) {
            fn bind(&self, binder: B) -> B {
                binder$(.bind(self.$n))*
            }
        }
    };
}

impl_tuple_bind!((T0, 0));
impl_tuple_bind!((T0, 0), (T1, 1));
impl_tuple_bind!((T0, 0), (T1, 1), (T2, 2));
impl_tuple_bind!((T0, 0), (T1, 1), (T2, 2), (T3, 3));
impl_tuple_bind!((T0, 0), (T1, 1), (T2, 2), (T3, 3), (T4, 4));
impl_tuple_bind!((T0, 0), (T1, 1), (T2, 2), (T3, 3), (T4, 4), (T5, 5));
impl_tuple_bind!((T0, 0), (T1, 1), (T2, 2), (T3, 3), (T4, 4), (T5, 5), (T6, 6));
impl_tuple_bind!((T0, 0), (T1, 1), (T2, 2), (T3, 3), (T4, 4), (T5, 5), (T6, 6), (T7, 7));
impl_tuple_bind!(
    (T0, 0),
    (T1, 1),
    (T2, 2),
    (T3, 3),
    (T4, 4),
    (T5, 5),
    (T6, 6),
    (T7, 7),
    (T8, 8)
);
impl_tuple_bind!(
    (T0, 0),
    (T1, 1),
    (T2, 2),
    (T3, 3),
    (T4, 4),
    (T5, 5),
    (T6, 6),
    (T7, 7),
    (T8, 8),
    (T9, 9)
);
impl_tuple_bind!(
    (T0, 0),
    (T1, 1),
    (T2, 2),
    (T3, 3),
    (T4, 4),
    (T5, 5),
    (T6, 6),
    (T7, 7),
    (T8, 8),
    (T9, 9),
    (T10, 10)
);
