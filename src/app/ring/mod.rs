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
    thread_rng,
    Rng,
};
use std::{
    cell::RefCell,
    collections::HashMap,
    i64::{
        MAX,
        MIN,
    },
    sync::{
        atomic::{
            AtomicPtr,
            Ordering,
        },
        Arc,
        Weak,
    },
};
// types
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
/// The global ring  of ScyllaDB.
pub type GlobalRing = (
    Vec<DC>,
    Uniform<usize>,
    Uniform<usize>,
    Uniform<u8>,
    u8,
    Registry,
    Vcell,
);
/// The atomic `GlobalRing`.
pub type AtomicRing = AtomicPtr<Weak<GlobalRing>>;
/// The reference counting pointer for `GlobalRing`.
pub type ArcRing = Arc<GlobalRing>;
/// The weak pointer for `GlobalRing`.
pub type WeakRing = Weak<GlobalRing>;

/// The Ring structure used to handle the access to ScyllaDB ring.
pub struct Ring {
    /// Version of the Ring
    pub version: u8,
    /// Most recent weak global ring
    pub weak: Option<Weak<GlobalRing>>,
    /// Registry which holds all scylla reporters
    pub registry: Registry,
    /// Root of ring (binary tree)
    pub root: Vcell,
    /// Uniform to sample reporter_id up to reporter_count == 255
    pub uniform: Uniform<u8>,
    /// Rng used by uniform to sample random number
    pub rng: ThreadRng,
    /// DataCenter (NOTE: the very first is the local datacenter)
    pub dcs: Vec<DC>,
    /// Uniform to pick random datacenter (used by send_global strategy)
    pub uniform_dcs: Uniform<usize>,
    /// Uniform to pick random replication refactor
    pub uniform_rf: Uniform<usize>,
}

static mut VERSION: u8 = 0;
static mut GLOBAL_RING: Option<AtomicRing> = None;

thread_local! {
    static RING: RefCell<Ring> = {
        let rng = thread_rng();
        let uniform: Uniform<u8> = Uniform::new(0,1);
        let registry: Registry = HashMap::new();
        let root: Vcell = DeadEnd::initial_vnode();
        let version = 0;
        let weak = None;
        let dcs = vec!["".to_string()];
        let uniform_dcs: Uniform<usize> = Uniform::new(0,dcs.len());
        let uniform_rf: Uniform<usize> = Uniform::new(0,1);
        RefCell::new(Ring{
            version,
            weak,
            registry,
            root,
            uniform,
            rng,
            uniform_dcs,
            uniform_rf,
            dcs
        })
    };
}

#[allow(unused)]
impl Ring {
    /// Send request to a given data_center with the given replica_index and token.
    pub fn send(
        data_center: &str,
        replica_index: usize,
        token: Token,
        request: ReporterEvent,
    ) -> Result<(), RingSendError> {
        RING.with(|local| {
            local
                .borrow_mut()
                .sending()
                .global(data_center, replica_index, token, request)
        })
    }
    /// Send request to the first local datacenter with the given replica_index and token.
    pub fn send_local(replica_index: usize, token: Token, request: ReporterEvent) -> Result<(), RingSendError> {
        RING.with(|local| local.borrow_mut().sending().local(replica_index, token, request))
    }
    /// Send request to the first local datacenter with the given token and a random replica.
    pub fn send_local_random_replica(token: Token, request: ReporterEvent) -> Result<(), RingSendError> {
        RING.with(|local| local.borrow_mut().sending().local_random_replica(token, request))
    }
    /// Send request to the global datacenter with the given token and a random replica.
    pub fn send_global_random_replica(token: Token, request: ReporterEvent) -> Result<(), RingSendError> {
        RING.with(|local| local.borrow_mut().sending().global_random_replica(token, request))
    }
    /// Rebuild the Ring the most up to date version
    pub fn rebuild() {
        RING.with(|local| {
            let mut ring = local.borrow_mut();
            unsafe {
                if VERSION != ring.version {
                    // load weak and upgrade to arc if strong_count > 0;
                    if let Some(mut arc) =
                        Weak::upgrade(GLOBAL_RING.as_ref().unwrap().load(Ordering::Relaxed).as_ref().unwrap())
                    {
                        let new_weak = Arc::downgrade(&arc);
                        let (dcs, uniform_dcs, uniform_rf, uniform, version, registry, root) = Arc::make_mut(&mut arc);
                        // update the local ring
                        ring.dcs = dcs.clone();
                        ring.uniform_dcs = *uniform_dcs;
                        ring.uniform_rf = *uniform_rf;
                        ring.uniform = *uniform;
                        ring.version = *version;
                        ring.registry = registry.clone();
                        ring.root = root.clone();
                        ring.weak.replace(new_weak);
                    };
                }
            }
        });
    }
    fn sending(&mut self) -> &mut Self {
        unsafe {
            if VERSION != self.version {
                // load weak and upgrade to arc if strong_count > 0;
                if let Some(mut arc) =
                    Weak::upgrade(GLOBAL_RING.as_ref().unwrap().load(Ordering::Relaxed).as_ref().unwrap())
                {
                    let new_weak = Arc::downgrade(&arc);
                    let (dcs, uniform_dcs, uniform_rf, uniform, version, registry, root) = Arc::make_mut(&mut arc);
                    // update the local ring
                    self.dcs = dcs.clone();
                    self.uniform_dcs = *uniform_dcs;
                    self.uniform_rf = *uniform_rf;
                    self.uniform = *uniform;
                    self.version = *version;
                    self.registry = registry.clone();
                    self.root = root.clone();
                    self.weak.replace(new_weak);
                };
            }
        }
        self
    }
    /// Return the current ring version
    pub(crate) fn version() -> u8 {
        unsafe { super::ring::VERSION }
    }
    fn global(
        &mut self,
        data_center: &str,
        replica_index: usize,
        token: Token,
        request: ReporterEvent,
    ) -> Result<(), RingSendError> {
        // send request.
        self.root.as_mut().search(token).send(
            data_center,
            replica_index,
            token,
            request,
            &mut self.registry,
            &mut self.rng,
            self.uniform,
        )
    }
    fn local(&mut self, replica_index: usize, token: Token, request: ReporterEvent) -> Result<(), RingSendError> {
        // send request.
        self.root.as_mut().search(token).send(
            &self.dcs[0],
            replica_index,
            token,
            request,
            &mut self.registry,
            &mut self.rng,
            self.uniform,
        )
    }
    fn local_random_replica(&mut self, token: Token, request: ReporterEvent) -> Result<(), RingSendError> {
        // send request.
        self.root.as_mut().search(token).send(
            &self.dcs[0],
            self.rng.sample(self.uniform_rf),
            token,
            request,
            &mut self.registry,
            &mut self.rng,
            self.uniform,
        )
    }
    fn global_random_replica(&mut self, token: Token, request: ReporterEvent) -> Result<(), RingSendError> {
        // send request.
        self.root.as_mut().search(token).send(
            &self.dcs[self.rng.sample(self.uniform_dcs)],
            self.rng.sample(self.uniform_rf),
            token,
            request,
            &mut self.registry,
            &mut self.rng,
            self.uniform,
        )
    }
    fn initialize_ring(version: u8, rebuild: bool) -> (ArcRing, Option<Box<Weak<GlobalRing>>>) {
        // create empty Registry
        let registry: Registry = HashMap::new();
        // create initial vnode
        let root = DeadEnd::initial_vnode();
        // pack Into globlal ring tuple
        let global_ring: GlobalRing = (
            vec!["".to_string()], // dcs
            Uniform::new(0, 1),
            Uniform::new(0, 1),
            Uniform::new(0, 1),
            version,
            registry,
            root,
        );
        // create Arc ring
        let arc_ring = Arc::new(global_ring);
        // downgrade to weak
        let weak_ring = Arc::downgrade(&arc_ring);
        // create atomicptr
        let boxed = Box::new(weak_ring);
        let raw_box = Box::into_raw(boxed);
        let atomic_ptr = AtomicPtr::new(raw_box);
        if rebuild {
            let old_weak = unsafe {
                let old_weak = GLOBAL_RING.as_mut().unwrap().swap(raw_box, Ordering::Relaxed);
                VERSION = version;
                old_weak
            };
            (arc_ring, Some(unsafe { Box::from_raw(old_weak) }))
        } else {
            unsafe {
                GLOBAL_RING = Some(atomic_ptr);
                VERSION = version;
            }
            (arc_ring, None)
        }
    }
}
trait SmartId {
    fn send_reporter(
        &mut self,
        token: Token,
        registry: &mut Registry,
        rng: &mut ThreadRng,
        uniform: Uniform<u8>,
        request: ReporterEvent,
    ) -> anyhow::Result<(), RingSendError>;
}

impl SmartId for Replica {
    fn send_reporter(
        &mut self,
        token: Token,
        registry: &mut Registry,
        rng: &mut ThreadRng,
        uniform: Uniform<u8>,
        request: ReporterEvent,
    ) -> anyhow::Result<(), RingSendError> {
        // shard awareness algo,
        let mut key = self.0;
        key.set_port((((((token as i128 + MIN as i128) as u64) << self.1) as u128 * self.2 as u128) >> 64) as u16);
        registry
            .get_mut(&self.0)
            .unwrap()
            .get_mut(&rng.sample(uniform))
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
pub trait Endpoints: EndpointsClone + Send + Sync {
    /// Send the request through the endpoints.
    fn send(
        &mut self,
        data_center: &str,
        replica_index: usize,
        token: Token,
        request: ReporterEvent,
        registry: &mut Registry,
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
    fn send(
        &mut self,
        data_center: &str,
        replica_index: usize,
        token: Token,
        request: ReporterEvent,
        mut registry: &mut Registry,
        mut rng: &mut ThreadRng,
        uniform: Uniform<u8>,
    ) -> anyhow::Result<(), RingSendError> {
        let replicas = self.get_mut(data_center).expect("Expected Replicas");
        if let Some(replica) = replicas.get_mut(replica_index) {
            replica.send_reporter(token, &mut registry, &mut rng, uniform, request)
        } else {
            if replicas.len() != 0 {
                // send to a random node
                let rf = Uniform::new(0, replicas.len());
                let mut replica = replicas[rng.sample(rf)];
                replica.send_reporter(token, &mut registry, &mut rng, uniform, request)
            } else {
                Err(RingSendError::NoReplica(request))
            }
        }
    }
}
impl Endpoints for Option<Replicas> {
    // this method will be invoked when we store Replicas as None.
    // used for initial ring to simulate the reporter and respond to worker(self) with NoRing error
    fn send(
        &mut self,
        _: &str,
        _: usize,
        _: Token,
        request: ReporterEvent,
        _: &mut Registry,
        _: &mut ThreadRng,
        _uniform: Uniform<u8>,
    ) -> anyhow::Result<(), RingSendError> {
        Err(RingSendError::NoRing(request))
    }
}

/// Search the endpoint of the virtual node.
pub trait Vnode: VnodeClone + Sync + Send {
    /// Search the endpoints by the given token.
    fn search(&mut self, token: Token) -> &mut Box<dyn Endpoints>;
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
    fn search(&mut self, token: Token) -> &mut Box<dyn Endpoints> {
        if token > self.left && token <= self.right {
            &mut self.replicas
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
    fn search(&mut self, token: Token) -> &mut Box<dyn Endpoints> {
        if token > self.left && token <= self.right {
            &mut self.replicas
        } else {
            // proceed binary search; shift left
            self.left_child.search(token)
        }
    }
}

impl Vnode for DeadEnd {
    fn search(&mut self, _token: Token) -> &mut Box<dyn Endpoints> {
        &mut self.replicas
    }
}

// this struct represent a vnode without left or right child,
// we don't need to set conditions because it's a deadend during search(),
// and condition must be true.
#[derive(Clone)]
struct DeadEnd {
    replicas: Box<dyn Endpoints>,
}

impl DeadEnd {
    fn initial_vnode() -> Vcell {
        Box::new(DeadEnd {
            replicas: Box::new(None),
        })
    }
}
// this struct represent the mild possible vnode(..)
// condition: token > left, and token <= right
#[derive(Clone)]
struct Mild {
    left: Token,
    right: Token,
    left_child: Vcell,
    right_child: Vcell,
    replicas: Box<dyn Endpoints>,
}

// as mild but with left child.
#[derive(Clone)]
struct LeftMild {
    left: Token,
    right: Token,
    left_child: Vcell,
    replicas: Box<dyn Endpoints>,
}

// Ring Builder Work in progress
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

fn walk_clockwise(starting_index: usize, end_index: usize, vnodes: &[VnodeTuple], replicas: &mut Replicas) {
    for vnode in vnodes.iter().take(end_index).skip(starting_index) {
        // fetch replica
        let (_, _, node_id, dc, msb, shard_count) = &vnode;
        let replica: Replica = (*node_id, *msb, *shard_count);
        // now push it to Replicas
        match replicas.get_mut(dc) {
            Some(vec_replicas_in_dc) => {
                if !vec_replicas_in_dc.contains(&replica) {
                    vec_replicas_in_dc.push(replica)
                }
            }
            None => {
                let vec_replicas_in_dc = vec![replica];
                replicas.insert(dc.clone(), vec_replicas_in_dc);
            }
        }
    }
}

/// Build the ScyllaDB ring
pub fn build_ring(
    dcs: &mut Vec<DC>,
    nodes: &Nodes,
    registry: Registry,
    reporter_count: u8,
    uniform_rf: usize,
    version: u8,
) -> (Arc<GlobalRing>, Box<Weak<GlobalRing>>) {
    // complete tokens-range
    let mut tokens: Tokens = Vec::new();
    // iter nodes
    for NodeInfo {
        tokens: node_tokens,
        address,
        data_center,
        msb,
        shard_count,
        ..
    } in nodes.values()
    {
        // we generate the tokens li
        for token in node_tokens {
            let node_token = (*token, address.clone(), data_center.clone(), *msb, *shard_count);
            tokens.push(node_token)
        }
    }
    // sort_unstable_by token
    tokens.sort_unstable_by(|a, b| a.0.cmp(&b.0));
    // create vnodes tuple from tokens
    let mut vnodes = Vec::new();
    let mut recent_left = MIN;
    for (right, node_id, dc, msb, shard_count) in &tokens {
        // create vnode tuple (starting from min)
        let vnode = (recent_left, *right, *node_id, dc.clone(), *msb, *shard_count);
        // push to vnodes
        vnodes.push(vnode);
        // update recent_left to right
        recent_left = *right;
    }
    // the check bellow is only to make sure if scylla-node didn't already
    // randmoly didn't gen the MIN token by luck.
    // confirm if the vnode_min is not already exist in our token range
    if vnodes.first().unwrap().1 == MIN {
        // remove it, otherwise the first vnode will be(MIN, MIN, ..) and invalidate vnode conditions
        vnodes.remove(0);
    };
    // we don't forget to add max vnode to our token range only if not already presented,
    // the check bellow is only to make sure if scylla-node didn't already
    // randmoly gen the MAX token by luck.
    // the MAX to our last vnode(the largest token )
    let last_vnode = vnodes.last().unwrap();
    // confirm if the vnode max is not present in our token-range
    if last_vnode.1 != MAX {
        let max_vnode = (
            recent_left,
            MAX,
            last_vnode.2,
            last_vnode.3.clone(),
            last_vnode.4,
            last_vnode.5,
        );
        // now push it
        vnodes.push(max_vnode);
    }
    // compute_ring
    let root_vnode = compute_ring(&vnodes, dcs);
    // create arc_ring
    let arc_ring = Arc::new((
        dcs.clone(),
        Uniform::new(0, dcs.len()),
        Uniform::new(0, uniform_rf),
        Uniform::new(0, reporter_count),
        version,
        registry,
        root_vnode,
    ));
    // downgrade to weak_ring
    let weak_ring = Arc::downgrade(&arc_ring);
    let boxed = Box::new(weak_ring);
    let raw_box = Box::into_raw(boxed);
    // update the global ring
    let old_weak = unsafe {
        // swap
        let old_weak = GLOBAL_RING.as_mut().unwrap().swap(raw_box, Ordering::Relaxed);
        // update version with new one.// this must be atomic and safe because it's u8.
        VERSION = version;
        old_weak
    };

    // return new arc_ring, weak_ring
    (arc_ring, unsafe { Box::from_raw(old_weak) })
}

fn compute_ring(vnodes: &[VnodeTuple], dcs: &mut Vec<DC>) -> Vcell {
    // compute chain (vnodes with replicas)
    let chain = compute_chain(vnodes);
    // clear dcs except the local_dc which is located at the header
    dcs.truncate(1);
    // collect data centers
    for dc in chain.first().as_ref().unwrap().2.keys() {
        // push rest dcs
        if dc != &dcs[0] {
            dcs.push(dc.to_string())
        }
    }
    // compute balanced binary tree
    compute_vnode(&chain)
}

fn compute_chain(vnodes: &[VnodeTuple]) -> Vec<(Token, Token, Replicas)> {
    // compute all possible replicas in advance for each vnode in vnodes
    // prepare ring chain
    let mut chain = Vec::new();
    for (starting_index, (left, right, _, _, _, _)) in vnodes.iter().enumerate() {
        let mut replicas: Replicas = HashMap::new();
        // first walk clockwise phase (start..end)
        walk_clockwise(starting_index, vnodes.len(), &vnodes, &mut replicas);
        // second walk clockwise phase (0..start)
        walk_clockwise(0, starting_index, &vnodes, &mut replicas);
        // create vnode
        chain.push((*left, *right, replicas));
    }
    chain
}

/// Initialize the ScyllaDB ring.
pub fn initialize_ring(version: u8, rebuild: bool) -> (ArcRing, Option<Box<Weak<GlobalRing>>>) {
    Ring::initialize_ring(version, rebuild)
}

#[test]
fn generate_and_compute_fake_ring() {
    use std::net::{
        IpAddr,
        Ipv4Addr,
    };
    let mut rng = thread_rng();
    let uniform = Uniform::new(MIN, MAX);
    // create test token_range vector // the token range should be fetched from scylla node.
    let mut tokens: Vec<(Token, SocketAddr, DC)> = Vec::new();
    // 4 us nodes ids
    let us_node_id_1: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 0);
    let us_node_id_2: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2)), 0);
    let us_node_id_3: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 3)), 0);
    let us_node_id_4: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 4)), 0);
    // 3 eu nodes ids
    let eu_node_id_1: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(128, 0, 0, 1)), 0);
    let eu_node_id_2: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(128, 0, 0, 2)), 0);
    let eu_node_id_3: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(128, 0, 0, 3)), 0);
    let us = "US".to_string();
    let eu = "EU".to_string();
    for _ in 0..256 {
        // 4 nodes in US Datacenter
        tokens.push((rng.sample(uniform), us_node_id_1, us.clone()));
        tokens.push((rng.sample(uniform), us_node_id_2, us.clone()));
        tokens.push((rng.sample(uniform), us_node_id_3, us.clone()));
        tokens.push((rng.sample(uniform), us_node_id_4, us.clone()));
        // 3 nodes in EU Datacenter
        tokens.push((rng.sample(uniform), eu_node_id_1, eu.clone()));
        tokens.push((rng.sample(uniform), eu_node_id_2, eu.clone()));
        tokens.push((rng.sample(uniform), eu_node_id_3, eu.clone()));
    }
    // sort tokens by token
    tokens.sort_unstable_by(|a, b| a.0.cmp(&b.0));
    // compute replicas for each vnode
    let mut vnodes = Vec::new();
    let mut recent_left = MIN;
    for (right, node_id, dc) in &tokens {
        // create vnode(starting from min)
        let vnode = (recent_left, *right, *node_id, dc.clone(), 12, 8); // fake msb/shardcount
                                                                        // push to vnodes
        vnodes.push(vnode);
        // update recent_left to right
        recent_left = *right;
    }
    // we don't forget to add max vnode to our token range
    let (_, recent_node_id, recent_dc) = tokens.last().unwrap();
    let max_vnode = (recent_left, MAX, *recent_node_id, recent_dc.clone(), 12, 8); //
    vnodes.push(max_vnode);
    // compute all possible replicas in advance for each vnode in vnodes
    // prepare ring chain
    let mut chain = Vec::new();
    let mut starting_index = 0;
    for (left, right, _, _, _, _) in &vnodes {
        let mut replicas: Replicas = HashMap::new();
        // first walk clockwise phase (start..end)
        walk_clockwise(starting_index, vnodes.len(), &vnodes, &mut replicas);
        // second walk clockwise phase (0..start)
        walk_clockwise(0, starting_index, &vnodes, &mut replicas);
        // update starting_index
        starting_index += 1;
        // create vnode
        chain.push((*left, *right, replicas));
    }
    // build computed binary search tree from chain
    // we start spliting from the root which is chain.len()/2
    // for example if chain length is 3 then the root vnode is at 3/2 = 1
    // and it will be mild where both of its childern are deadends.
    let _root = compute_vnode(&chain);
}
