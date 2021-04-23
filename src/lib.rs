// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0
pub mod cql;
#[cfg(not(feature = "app"))]
pub use cql::*;
#[cfg(feature = "app")]
pub mod app;

pub mod prelude {
    #[cfg(feature = "app")]
    pub use super::app::{access::*, worker::*, *};
    #[cfg(feature = "app")]
    pub use backstage::*;

    pub use super::cql::{
        Batch, ColumnDecoder, ColumnEncoder, ColumnValue, Consistency, Decoder, Frame, Iter, Prepare,
        PreparedStatement, Query, QueryStatement, Row, Rows, RowsDecoder, Statements, TokenEncoder, Values,
        VoidDecoder,
    };
}
