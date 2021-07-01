// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

#![warn(missing_docs)]
//! Scylla
/// Scylla application module
mod application;

/// Access traits and helpers for constructing and executing queries
pub mod access;
/// Cluster application
pub mod cluster;
/// Node application which manages scylla nodes
pub mod node;
/// The ring, which manages scylla access
pub mod ring;
/// The stage application, which handles sending and receiving scylla requests
pub mod stage;
/// Websocket listener which processes commands
pub mod websocket;
/// Workers which can be used when sending requests to handle the responses
pub mod worker;

pub use application::*;
use async_trait::async_trait;
use backstage::prelude::*;
use log::*;
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;
use std::time::Duration;
use thiserror::Error;
pub use websocket::add_nodes::add_nodes;
pub use worker::{Worker, WorkerError};
