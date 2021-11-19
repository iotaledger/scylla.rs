use super::*;
use arc_swap::ArcSwapOption;
use rand::{
    distributions::Uniform,
    thread_rng,
    Rng,
};

static SHARED_RING: ArcSwapOption<SharedRing> = ArcSwapOption::const_empty();

/// Shared ring
pub struct SharedRing {
    /// Local datacenter
    pub local_datacenter: String,
    /// Registry which holds all scylla reporters
    pub registry: Registry,
    /// static keyspaces
    pub keyspaces: HashMap<String, ReplicationInfo>,
    /// Uniform to sample reporter_id up to reporter_count == 255
    pub uniform: Uniform<u8>,
    /// Root of ring (binary tree)
    pub root: Vcell,
}

impl SharedRing {
    pub(crate) fn new<D: Into<String>>(
        local_datacenter: D,
        registry: Registry,
        keyspaces: HashMap<String, ReplicationInfo>,
        mut reporter_count: u8,
        nodes: &Nodes,
    ) -> Self {
        if reporter_count == 0 {
            reporter_count = 1
        }
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
        let root = compute_ring(&vnodes);
        Self {
            local_datacenter: local_datacenter.into(),
            registry,
            keyspaces,
            uniform: Uniform::new(0, reporter_count),
            root,
        }
    }
    /// Send request to the first local datacenter with the given token and a random replica.
    #[inline]
    pub fn send_local_random_replica(
        keyspace: Option<&str>,
        token: Token,
        request: ReporterEvent,
    ) -> Result<(), RingSendError> {
        if let Some(ring) = SHARED_RING.load().as_ref() {
            ring.local_random_replica(keyspace, token, request)
        } else {
            Err(RingSendError::NoRing(request))
        }
    }
    /// Send request to the global datacenter with the given token and a random replica.
    #[inline]
    pub fn send_global_random_replica(
        keyspace: Option<&str>,
        token: Token,
        request: ReporterEvent,
    ) -> Result<(), RingSendError> {
        if let Some(ring) = SHARED_RING.load().as_ref() {
            ring.global_random_replica(keyspace, token, request)
        } else {
            Err(RingSendError::NoRing(request))
        }
    }
    /// Make the ring empty: None
    pub fn drop() {
        SHARED_RING.swap(None);
    }
    /// Commit the ring
    pub fn commit(self) {
        SHARED_RING.store(Some(self.into()))
    }
    /// Check if the ring is some
    pub fn is_some() -> bool {
        SHARED_RING.load().is_some()
    }
    /// Check if the ring is none
    pub fn is_none() -> bool {
        SHARED_RING.load().is_none()
    }
    #[inline]
    fn global_random_replica(
        &self,
        keyspace: Option<&str>,
        token: Token,
        request: ReporterEvent,
    ) -> Result<(), RingSendError> {
        let mut rng = thread_rng();
        // send request.
        let (replica_index, dc) = keyspace
            .and_then(|keyspace| {
                self.keyspaces
                    .get(keyspace)
                    .and_then(|info| info.get_random_and_dc(&mut rng))
                    .and_then(|(rf, dc)| Some((rf.random(&mut rng), dc)))
            })
            .unwrap_or((0, &self.local_datacenter)); // default to (0, local_datacenter) for dyn non existing keyspace
        self.root.search(token).send(
            dc,
            replica_index,
            token,
            request,
            &self.registry,
            &mut rng,
            self.uniform,
        )
    }
    #[inline]
    fn local_random_replica(
        &self,
        keyspace: Option<&str>,
        token: Token,
        request: ReporterEvent,
    ) -> Result<(), RingSendError> {
        let mut rng = thread_rng();
        // send request.
        let replica_index = keyspace
            .and_then(|keyspace| {
                self.keyspaces
                    .get(keyspace)
                    .and_then(|info| info.get(&self.local_datacenter))
                    .and_then(|rf| Some(rf.random(&mut rng)))
            })
            .unwrap_or(0); // default to 0 for dyn non existing keyspace
        self.root.search(token).send(
            &self.local_datacenter,
            replica_index,
            token,
            request,
            &self.registry,
            &mut rng,
            self.uniform,
        )
    }
    #[allow(unused)]
    fn local(&self, replica_index: usize, token: Token, request: ReporterEvent) -> Result<(), RingSendError> {
        self.global(&self.local_datacenter, replica_index, token, request)
    }
    #[allow(unused)]
    fn global(
        &self,
        data_center: &str,
        replica_index: usize,
        token: Token,
        request: ReporterEvent,
    ) -> Result<(), RingSendError> {
        let mut rng = thread_rng();
        // send request.
        self.root.search(token).send(
            data_center,
            replica_index,
            token,
            request,
            &self.registry,
            &mut rng,
            self.uniform,
        )
    }
}

#[derive(Clone, Debug)]
/// Keyspace ReplicationInfo with rand::uniform to pick random data_center
pub struct ReplicationInfo {
    uniform: Uniform<usize>,
    datacenter: Vec<DC>,
    replication_factor: HashMap<DC, ReplicationFactor>,
}
impl Default for ReplicationInfo {
    fn default() -> Self {
        Self {
            uniform: Uniform::new(0, 1),
            datacenter: vec!["datacenter1".to_string()],
            replication_factor: maplit::hashmap! {"datacenter1".to_string() => ReplicationFactor::new(1)},
        }
    }
}
#[derive(Clone, Debug)]
/// ReplicationFactor with rand::uniform to sample random replica index
pub struct ReplicationFactor {
    uniform: Uniform<usize>,
    rf: usize,
}

impl ReplicationFactor {
    /// Create new replication factor
    pub fn new(rf: usize) -> Self {
        Self {
            uniform: Uniform::new(0, rf),
            rf,
        }
    }
    /// return random replica index
    pub fn random(&self, rng: &mut ThreadRng) -> usize {
        rng.sample(self.uniform)
    }
    /// return the replication factor
    pub fn rf(&self) -> usize {
        self.rf
    }
}

impl From<usize> for ReplicationFactor {
    fn from(rf: usize) -> Self {
        Self::new(rf)
    }
}

impl ReplicationInfo {
    /// Create replication info
    pub fn new<D: Into<String>, R: Into<ReplicationFactor>>(datacenter: D, rf: R) -> Self {
        let mut info = Self::empty();
        info.upsert(datacenter, rf);
        info
    }
    /// Create replication info
    pub fn empty() -> Self {
        Self {
            uniform: Uniform::new(0, 1),
            datacenter: Vec::new(),
            replication_factor: HashMap::new(),
        }
    }
    /// Insert/Update datacenter and its replication factor
    pub fn upsert<D: Into<String>, R: Into<ReplicationFactor>>(&mut self, datacenter: D, rf: R) {
        self.replication_factor.insert(datacenter.into(), rf.into());
        self.datacenter = self.replication_factor.clone().into_keys().collect();
        self.uniform = Uniform::new(0, self.datacenter.len());
    }
    /// NOTE this function will panic if removing the last datacenter
    pub fn remove<D: Into<String>>(&mut self, datacenter: D) -> Option<ReplicationFactor> {
        let datacenter: String = datacenter.into();
        self.datacenter.retain(|d| d != &datacenter);
        self.uniform = Uniform::new(0, self.datacenter.len());
        self.replication_factor.remove(&datacenter)
    }
    /// get the replication factor for a given datacenter
    pub fn get(&self, datacenter: &str) -> Option<&ReplicationFactor> {
        self.replication_factor.get(datacenter)
    }
    /// return random replication factor
    pub fn get_random(&self, rng: &mut ThreadRng) -> Option<&ReplicationFactor> {
        let dc_index = rng.sample(self.uniform);
        self.replication_factor.get(&self.datacenter[dc_index])
    }
    /// Get random replication_factor along the datacenter
    pub fn get_random_and_dc(&self, rng: &mut ThreadRng) -> Option<(&ReplicationFactor, &String)> {
        let dc_index = rng.sample(self.uniform);
        self.replication_factor
            .get(&self.datacenter[dc_index])
            .and_then(|d| Some((d, &self.datacenter[dc_index])))
    }
}

fn compute_ring(vnodes: &[VnodeTuple]) -> Vcell {
    // compute chain (vnodes with replicas)
    let chain = compute_chain(vnodes);
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

#[test]
fn generate_and_compute_fake_ring() {
    use std::net::{
        IpAddr,
        Ipv4Addr,
    };
    let mut rng = rand::thread_rng();
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
