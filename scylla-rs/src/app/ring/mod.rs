// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use crate::app::{
    cluster::{
        NodeInfo,
        Nodes,
    },
    stage::reporter::ReporterEvent,
};
use std::net::SocketAddr;

use backstage::core::UnboundedHandle;
use rand::{
    distributions::Uniform,
    prelude::ThreadRng,
    Rng,
};
use std::{
    collections::HashMap,
    i64::{
        MAX,
        MIN,
    },
};
/// The token of Ring.
pub type Token = i64;
/// The tokens of shards.
pub type Tokens = Vec<(Token, SocketAddr, DC, Msb, ShardCount)>;
/// The most significant bit in virtual node.
pub type Msb = u8;
/// The number of shards.
pub type ShardCount = u16;
/// The tuple of recording a virtual node.
pub type VnodeTuple = (Token, Token, SocketAddr, DC, Msb, ShardCount);
/// The data center string.
pub type DC = String;
type Replicas = HashMap<DC, Vec<Replica>>;
type Replica = (SocketAddr, Msb, ShardCount);
type Vcell = Box<dyn Vnode>;
/// The registry of `SocketAddr` to its reporters.
pub type Registry = HashMap<SocketAddr, HashMap<u8, UnboundedHandle<ReporterEvent>>>;

trait SmartId {
    fn send_reporter(
        &self,
        token: Token,
        registry: &Registry,
        rng: &mut ThreadRng,
        uniform: Uniform<u8>,
        request: ReporterEvent,
    ) -> anyhow::Result<(), RingSendError>;
}

impl SmartId for Replica {
    #[inline]
    fn send_reporter(
        &self,
        token: Token,
        registry: &Registry,
        rng: &mut ThreadRng,
        uniform: Uniform<u8>,
        request: ReporterEvent,
    ) -> anyhow::Result<(), RingSendError> {
        // shard awareness algo,
        let mut key = self.0;
        key.set_port((((((token as i128 + MIN as i128) as u64) << self.1) as u128 * self.2 as u128) >> 64) as u16);
        registry
            .get(&key)
            .unwrap()
            .get(&rng.sample(uniform))
            .unwrap()
            .send(request)
            .map_err(|e| RingSendError::SendError(e))
    }
}

#[derive(Debug)]
/// Ring send error
pub enum RingSendError {
    /// No replicas
    NoReplica(ReporterEvent),
    /// No ring exist error
    NoRing(ReporterEvent),
    /// Reporter's send error
    SendError(tokio::sync::mpsc::error::SendError<ReporterEvent>),
}

impl std::fmt::Display for RingSendError {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoRing(_) => {
                write!(fmt, "no ring")
            }
            Self::SendError(tokio_send_error) => tokio_send_error.fmt(fmt),
            Self::NoReplica(_) => {
                write!(fmt, "no replica")
            }
        }
    }
}

impl std::error::Error for RingSendError {}
impl Into<ReporterEvent> for RingSendError {
    fn into(self) -> ReporterEvent {
        match self {
            Self::NoRing(event) => event,
            Self::SendError(tokio_send_error) => tokio_send_error.0,
            Self::NoReplica(event) => event,
        }
    }
}

/// Endpoints trait which should be implemented by `Replicas`.
pub trait Endpoints: EndpointsClone + Send + Sync + std::fmt::Debug {
    /// Send the request through the endpoints.
    fn send(
        &self,
        data_center: &str,
        replica_index: usize,
        token: Token,
        request: ReporterEvent,
        registry: &Registry,
        rng: &mut ThreadRng,
        uniform: Uniform<u8>,
    ) -> anyhow::Result<(), RingSendError>;
}

/// Clone the endpoints.
pub trait EndpointsClone {
    /// Clone the box of endpoints.
    fn clone_box(&self) -> Box<dyn Endpoints>;
}

impl<T> EndpointsClone for T
where
    T: 'static + Endpoints + Clone,
{
    fn clone_box(&self) -> Box<dyn Endpoints> {
        Box::new(self.clone())
    }
}

impl Clone for Box<dyn Endpoints> {
    fn clone(&self) -> Box<dyn Endpoints> {
        self.clone_box()
    }
}

impl Endpoints for Replicas {
    #[inline]
    fn send(
        &self,
        data_center: &str,
        replica_index: usize,
        token: Token,
        request: ReporterEvent,
        registry: &Registry,
        rng: &mut ThreadRng,
        uniform: Uniform<u8>,
    ) -> anyhow::Result<(), RingSendError> {
        if let Some(replicas) = self.get(data_center) {
            if let Some(replica) = replicas.get(replica_index) {
                replica.send_reporter(token, registry, rng, uniform, request)
            } else {
                if replicas.len() != 0 {
                    // send to a random node
                    let rf = Uniform::new(0, replicas.len());
                    let replica = replicas[rng.sample(rf)];
                    replica.send_reporter(token, registry, rng, uniform, request)
                } else {
                    Err(RingSendError::NoReplica(request))
                }
            }
        } else {
            Err(RingSendError::NoReplica(request))
        }
    }
}

/// Search the endpoint of the virtual node.
pub trait Vnode: VnodeClone + Sync + Send + std::fmt::Debug {
    /// Search the endpoints by the given token.
    fn search(&self, token: Token) -> &Box<dyn Endpoints>;
}

/// Clone the virtual node.
pub trait VnodeClone {
    /// Clone the box of virtual node.
    fn clone_box(&self) -> Box<dyn Vnode>;
}

impl<T> VnodeClone for T
where
    T: 'static + Vnode + Clone,
{
    fn clone_box(&self) -> Box<dyn Vnode> {
        Box::new(self.clone())
    }
}

impl Clone for Box<dyn Vnode> {
    fn clone(&self) -> Box<dyn Vnode> {
        self.clone_box()
    }
}

impl Vnode for Mild {
    #[inline]
    fn search(&self, token: Token) -> &Box<dyn Endpoints> {
        if token > self.left && token <= self.right {
            &self.replicas
        } else if token <= self.left {
            // proceed binary search; shift left.
            self.left_child.search(token)
        } else {
            // proceed binary search; shift right
            self.right_child.search(token)
        }
    }
}

impl Vnode for LeftMild {
    #[inline]
    fn search(&self, token: Token) -> &Box<dyn Endpoints> {
        if token > self.left && token <= self.right {
            &self.replicas
        } else {
            // proceed binary search; shift left
            self.left_child.search(token)
        }
    }
}

impl Vnode for DeadEnd {
    #[inline]
    fn search(&self, _token: Token) -> &Box<dyn Endpoints> {
        &self.replicas
    }
}

// this struct represent a vnode without left or right child,
// we don't need to set conditions because it's a deadend during search(),
// and condition must be true.
#[derive(Clone, Debug)]
struct DeadEnd {
    replicas: Box<dyn Endpoints>,
}

// this struct represent the mild possible vnode(..)
// condition: token > left, and token <= right
#[derive(Clone, Debug)]
struct Mild {
    left: Token,
    right: Token,
    left_child: Vcell,
    right_child: Vcell,
    replicas: Box<dyn Endpoints>,
}

// as mild but with left child.
#[derive(Clone, Debug)]
struct LeftMild {
    left: Token,
    right: Token,
    left_child: Vcell,
    replicas: Box<dyn Endpoints>,
}

fn compute_vnode(chain: &[(Token, Token, Replicas)]) -> Vcell {
    let index = chain.len() / 2;
    let (left, right) = chain.split_at(index);
    let (vnode, right) = right.split_first().unwrap();
    if right.is_empty() && left.is_empty() {
        // then the parent_vnode without any child so consider it deadend
        Box::new(DeadEnd {
            replicas: Box::new(vnode.2.to_owned()),
        })
    } else if !right.is_empty() && !left.is_empty() {
        // parent_vnode is mild with left /right childern
        // compute both left and right
        let left_child = compute_vnode(left);
        let right_child = compute_vnode(right);
        Box::new(Mild {
            left: vnode.0,
            right: vnode.1,
            left_child,
            right_child,
            replicas: Box::new(vnode.2.to_owned()),
        })
    } else {
        // if !left.is_empty() && right.is_empty()
        // parent_vnode is leftmild
        Box::new(LeftMild {
            left: vnode.0,
            right: vnode.1,
            left_child: compute_vnode(left),
            replicas: Box::new(vnode.2.to_owned()),
        })
    }
}

/// Mod impl the shared ring
pub mod shared;

pub use shared::{
    ReplicationFactor,
    ReplicationInfo,
    SharedRing,
};
