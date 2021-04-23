// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

#[cfg(feature = "app")]
pub mod app {
    pub use scylla_app::{application::*, *};
}

#[cfg(feature = "app")]
pub mod cql {
    pub use scylla_cql::*;
}

#[cfg(not(feature = "app"))]
pub use scylla_cql::*;
