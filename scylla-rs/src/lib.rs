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
        cql::*,
    };
    pub use backstage::core::*;
    pub use maplit::{
        self,
        *,
    };
    pub use scylla_parse;
    pub use scylla_rs_macros::*;
}
