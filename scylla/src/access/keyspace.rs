// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

/// Keyspace trait, almost marker
pub trait Keyspace: Send + Sized + Sync {
    type Error;
    const NAME: &'static str;
    fn name() -> &'static str {
        Self::NAME
    }

    // TODO replication_refactor, strategy, options,etc.
}
