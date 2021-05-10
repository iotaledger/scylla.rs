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
/// Listener application which monitors for incoming connections
pub mod listener;
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

use anyhow::{anyhow, bail};
pub use application::*;
use backstage::*;
use log::*;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::mpsc;
pub use websocket::client::add_nodes::add_nodes;
pub use worker::{Worker, WorkerError};

pub(crate) struct ChildHandle<T> {
    pub event_handle: tokio::sync::mpsc::UnboundedSender<T>,
    pub join_handle: tokio::task::JoinHandle<Result<ActorRequest, ActorError>>,
}
