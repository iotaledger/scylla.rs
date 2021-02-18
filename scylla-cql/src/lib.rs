// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

//! This crate implements the binary Cql protocol V4.
//! See `https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec` for more details.

#![warn(missing_docs)]
mod compression;
mod connection;
mod frame;
mod murmur3;

pub use connection::*;
/// This is the public API of this crate
pub use frame::*;

pub use murmur3::murmur3_cassandra_x64_128;

/// expose MyCompression
pub use compression::MyCompression;
