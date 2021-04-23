// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0
pub mod cql;
#[cfg(not(feature = "app"))]
pub use cql::*;
#[cfg(feature = "app")]
pub mod app;

#[cfg(feature = "app")]
pub mod prelude {
    pub use super::app::{access::*, worker::*, *};
    pub use backstage::*;
}
