// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0
pub mod cql;
#[cfg(not(feature = "app"))]
pub use cql::*;
#[cfg(feature = "app")]
pub mod app;

#[cfg(feature = "app")]
pub mod prelude {
    pub use super::{
        app::{
            access::*,
            cluster::ClusterHandleExt,
            worker::*,
            ScyllaHandleExt,
            *,
        },
        cql::{
            Batch,
            Binder,
            ColumnDecoder,
            ColumnEncoder,
            ColumnValue,
            Consistency,
            Decoder,
            Frame,
            Iter,
            Prepare,
            PreparedStatement,
            Query,
            QueryStatement,
            Row,
            Rows,
            RowsDecoder,
            Statements,
            TokenEncoder,
            VoidDecoder,
        },
    };
    pub use backstage::core::*;
}
