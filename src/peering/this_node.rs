use rand::seq::IteratorRandom;
use std::collections::HashMap;
use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tokio::time::{Duration, Instant};

use crate::peering::client::Client;
use crate::peering::errors::*;

use tracing::{debug, error, info, instrument, span, warn, Level};

type Key = crate::peering::hash::Hash;

#[derive(Clone, Eq)]
pub struct OtherNode {
    pub addr: SocketAddr,
    pub key: Key,
    pub last_seen: Instant,
}

impl OtherNode {
    fn new(addr: SocketAddr, last_seen: Instant) -> Self {
        Self {
            addr,
            last_seen,
            key: Key::hash(&addr.to_string()),
        }
    }

    fn distance_to(&self, key: &Key) -> Key {
        self.key.cyclic_distance(&key)
    }

    fn time_since_last_seen(&self) -> Duration {
        Instant::now() - self.last_seen
    }
}

#[derive(Debug)]
pub struct ThisNode {
    pub addr: SocketAddr,
    hash: Key,
    pub peers: Peers,
    pub redundancy: u8,
    pub primary_store: Arc<RwLock<HashMap<Key, String>>>,
    pub secondary_store: Arc<RwLock<HashMap<SocketAddr, HashMap<Key, String>>>>,
    pub secondants: Arc<RwLock<HashMap<Key, OtherNode>>>,
}

impl ThisNode {
    pub fn new(addr: SocketAddr, redundancy: u8) -> Self {
        ThisNode {
            addr,
            redundancy,
            hash: Key::hash(&addr.to_string()),
            peers: Peers {
                known_peers: Arc::new(RwLock::new(HashMap::new())),
            },
            primary_store: Arc::new(RwLock::new(HashMap::new())),
            secondary_store: Arc::new(RwLock::new(HashMap::new())),
            secondants: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    fn distance_to(&self, key: &Key) -> Key {
        self.hash.cyclic_distance(&key)
    }

    #[instrument(skip(self), fields(this=?self.addr))]
    pub async fn mingle(&self, bootstrap_addr: &SocketAddr) {
        self.peers.introduce(bootstrap_addr).await;

        match Client::introduce(bootstrap_addr, &self.addr).await {
            Ok(new_peers) => {
                for addr in new_peers {
                    if addr != self.addr && &addr != bootstrap_addr {
                        match Client::introduce(&addr, &self.addr).await {
                            Ok(peers) => {
                                self.peers.introduce(&addr).await;
                                for peer_addr in peers {
                                    self.peers.introduce(&peer_addr).await;
                                }
                            }
                            Err(err) => {
                                error!("Error introducing myself to {}: {:?}", &addr, err);
                            }
                        }
                    }
                }
            }
            Err(err) => {
                error!(
                    "Error introducing myself to bootstrap peer {}: {:?}",
                    &bootstrap_addr, err
                );
            }
        };
    }

    #[instrument(skip(self), fields(this=?self.addr))]
    pub async fn introduction_rcvd(
        self: &Arc<Self>,
        sender_addr: SocketAddr,
    ) -> Result<Vec<OtherNode>, IntroductionError> {
        let (old_peers, new_peer) = self.peers.introduce(&sender_addr).await;
        debug!(peer=?new_peer, "New peer found");

        let self_clone = Arc::clone(self);
        let new_peer_clone = new_peer.clone();

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(500)).await; // FIXME: do retries instead of this
            self_clone.move_values_to_peer(&new_peer_clone).await;
            self_clone.clean_secondary_store(&new_peer_clone).await;
            self_clone.clean_secondants(&new_peer_clone).await;
        });

        Ok(old_peers)
    }

    async fn clean_secondary_store(self: &Arc<Self>, new_peer: &OtherNode) {
        let mut secondary_store = self.secondary_store.write().await;

        let to_remove = secondary_store
            .clone()
            .into_iter()
            .map(|(addr, store)| {
                store
                    .into_iter()
                    .map(move |(k, _)| (addr.clone(), k.clone()))
            })
            .flatten()
            // if the new peer is closer than the one we are secondant for:
            .filter(|(addr, key)| {
                new_peer.distance_to(key) < Key::hash(&addr.to_string()).cyclic_distance(key)
            })
            .collect::<Vec<_>>();

        for (addr, key) in to_remove {
            info!("cleaning out of secondary store: {:?}", (addr, key));
            secondary_store.get_mut(&addr).map(|store| {
                store.remove(&key);
            });
        }
    }

    async fn clean_secondants(self: &Arc<Self>, new_peer: &OtherNode) {
        let mut secondants = self.secondants.write().await;
        let to_remove = secondants
            .clone()
            .into_iter()
            .filter(|(key, peer)| new_peer.distance_to(key) < peer.key.cyclic_distance(key))
            .collect::<Vec<_>>();

        for (key, _) in to_remove {
            let old_secondant = secondants.remove(&key);
            info!("Removed {:?} as secondant for {}", old_secondant, key);
        }
    }

    #[instrument(skip(self), fields(this=?self.addr))]
    async fn move_values_to_peer(self: &Arc<Self>, peer: &OtherNode) {
        let primary_store = self.primary_store.read().await;
        let values_to_move = primary_store
            .iter()
            .filter(|(key, _value)| peer.distance_to(key) < self.distance_to(key))
            .map(|(_, v)| v)
            .cloned()
            .collect::<Vec<_>>();

        if values_to_move.len() == 0 {
            return;
        }

        drop(primary_store); // don't hold the lock while we wait for the other node.
                             // this allows other nodes that don't known about
                             // the new peer yet still get the value here in the meantime

        info!("Values to move: {:?}", values_to_move);

        match Client::move_values(&peer.addr, values_to_move).await {
            Ok(keys) => {
                let mut primary_store = self.primary_store.write().await;

                for key in keys {
                    let removed_value = primary_store.remove(&key);
                    match removed_value {
                        Some(removed_value) => {
                            info!("{} no longer held here", removed_value);
                        }
                        None => {
                            warn!("Tried to remove a value that wasn't here (anymore?)")
                        }
                    }
                }
            }
            Err(err) => {
                error!("Failed to move values: {:?}", err);
            }
        }
    }

    #[instrument(skip(self), fields(this=?self.addr))]
    pub async fn move_values_rcvd(
        self: &Arc<Self>,
        values: Vec<String>,
    ) -> Result<Vec<Key>, MoveValuesError> {
        let mut stored_keys = vec![];

        for value in values {
            let key = Key::hash(&value);

            let my_distance = self.distance_to(&key);

            match self
                .peers
                .nearest_to_but_less_than(&key, &my_distance.clone())
                .await
                .clone()
            {
                Some(peer) => {
                    warn!(value=?value, "Got value that was not meant for me, rerouting.");
                    let value = value.clone();
                    // FIXME: when this happens (succesfully or unsuccesfully) the value is not removed in the calling node.
                    tokio::spawn(async move { Client::store(&peer.addr, value, None).await });
                }
                None => {
                    self.store_here(&key, value, None).await;
                    stored_keys.push(key);
                }
            };
        }

        Ok(stored_keys)
    }

    #[instrument(skip(self), fields(this=?self.addr))]
    pub async fn store_rcvd(
        self: &Arc<Self>,
        value: String,
        corpse: Option<SocketAddr>,
    ) -> Result<(Key, SocketAddr), StoreError> {
        self.handle_possible_corpse(corpse).await;

        let key = Key::hash(&value);

        match self
            .peers
            .nearest_to_but_less_than(&key, &self.distance_to(&key))
            .await
        {
            Some(peer) => self.delegate_store(peer, value, corpse).await,
            None => {
                self.store_here(&key, value, None).await;
                Ok((key, self.addr))
            }
        }
    }

    #[instrument(skip(self), fields(this=?self.addr))]
    async fn handle_connection_failed(self: &Arc<Self>, peer: OtherNode) -> CommonError {
        warn!(dead_peer=?peer, "Connection to peer failed.");
        // self.mark_dead(&peer).await; // FIXME: causes a typecheck loop
        // FIXME: is there more we can try here? wait for the secondary to notice
        // this death or try to find the secondary ourselves?
        CommonError::Unavailable
    }

    #[instrument(skip(self), fields(this=?self.addr))]
    async fn delegate_store(
        self: &Arc<Self>,
        peer: OtherNode,
        value: String,
        corpse: Option<SocketAddr>,
    ) -> Result<(Key, SocketAddr), StoreError> {
        match Client::store(&peer.addr, value, corpse).await {
            Ok(res) => Ok(res),
            Err(ClientError::ConnectionFailed(_)) => Err(StoreError::Common(
                self.handle_connection_failed(peer).await,
            )),
            Err(ClientError::MalformedResponse) => {
                Err(StoreError::Common(CommonError::Internal(InternalError {
                    message: "got malformed response from client".to_string(),
                })))
            }
            Err(ClientError::Status(err)) => Err(StoreError::Common(CommonError::Status(err))),
        }
    }

    async fn store_in_primary(self: &Arc<Self>, key: &Key, value: String) -> Option<String> {
        let mut primary_store = self.primary_store.write().await;
        primary_store.insert(key.clone(), value)
    }

    #[instrument(skip(self), fields(this=?self.addr))]
    async fn store_here(
        self: &Arc<Self>,
        key: &Key,
        value: String,
        corpse: Option<&OtherNode>,
    ) -> () {
        let inserted = self.store_in_primary(key, value.clone()).await;

        if inserted.is_none() {
            let self_clone = Arc::clone(self);
            let value_clone = value.clone();
            let corpse_clone = corpse.cloned();
            tokio::task::spawn(async move {
                self_clone
                    .store_in_secondants(value_clone, self_clone.redundancy, corpse_clone.as_ref())
                    .await;
            });
        }
    }

    #[instrument(skip(self), fields(this=?self.addr))]
    async fn pick_secondants(self: &Arc<Self>, n: u8) -> Vec<OtherNode> {
        let secondants = self.secondants.read().await;
        let peers = self.peers.known_peers.read().await;

        let mut secondant_counts: HashMap<OtherNode, u64> = HashMap::new();

        for peer in secondants.values().cloned() {
            let count = secondant_counts.entry(peer).or_insert(0);
            *count += 1;
        }

        info!("secondants: {:?}", secondants.values().collect::<Vec<_>>());
        info!("counts: {:?}", secondant_counts);

        let peer_counts: Vec<_> = peers
            .values()
            .map(|p| (p, secondant_counts.get(p).cloned().unwrap_or(0)))
            .collect();

        info!("peer_counts: {:?}", peer_counts);

        let mut lowest = HashSet::new();

        for _ in 0..n {
            let next_lowest = peer_counts
                .iter()
                .filter(|tup| !lowest.contains(tup))
                .min_by_key(|(p, c)| (c, p.time_since_last_seen()));

            if let Some(next_lowest) = next_lowest {
                lowest.insert(next_lowest);
            } else {
                break;
            }
        }

        let mut lowest_vec: Vec<_> = lowest.into_iter().cloned().collect();
        lowest_vec.sort_by_key(|(p, c)| (c.clone(), p.time_since_last_seen().clone()));

        info!(
            "lowest n: {:?}",
            lowest_vec
                .iter()
                .map(|(p, c)| (c, p.time_since_last_seen(), p))
                .collect::<Vec<_>>()
        );

        lowest_vec.into_iter().map(|(p, c)| p.clone()).collect()
    }

    #[instrument(skip(self), fields(this=?self.addr))]
    async fn store_in_secondants(
        self: &Arc<Self>,
        value: String,
        n: u8,
        corpse: Option<&OtherNode>,
    ) {
        let tasks: Vec<_> = self
            .pick_secondants(n)
            .await
            .into_iter()
            .map(|peer| {
                let value = value.clone();
                let primary_holder = self.addr.clone();
                let peer_clone = peer.clone();
                let corpse_clone = corpse.cloned();
                let self_clone = Arc::clone(self);
                tokio::spawn(async move {
                    info!("Chose {:?} as secondant for {}", &peer, &value);
                    match Client::store_secondary(
                        &peer_clone.addr,
                        value,
                        &primary_holder,
                        corpse_clone.map(|p| p.addr),
                    )
                    .await
                    {
                        Ok(key) => Ok((peer_clone, key)),
                        Err(err) => Err(err),
                    }
                })
            })
            .collect();

        for task in tasks {
            match task.await.expect("join error?!") {
                Ok((peer, key)) => self.store_as_secondant(peer, key).await,
                Err(_) => {
                    error!("client error when assigning secondant");
                    // FIXME: restructure this fn so we can retry another node here
                }
            }
        }
    }

    #[instrument(skip(self), fields(this=?self.addr))]
    pub async fn store_as_secondant(&self, peer: OtherNode, key: Key) {
        let mut secondants = self.secondants.write().await;
        let replaced = secondants.insert(key, peer.clone());

        match replaced {
            Some(replaced) => {
                info!(
                    "Replaced {} as secondant for {} with {} (presumably because it died)",
                    replaced.addr, key, peer.addr,
                )
            }
            None => {
                info!("Stored {} as secondant for {:?} ", peer.addr, key);
            }
        }
    }

    #[instrument(skip(self), fields(this=?self.addr))]
    pub async fn secondary_store_rcvd(
        self: &Arc<Self>,
        value: String,
        primary_holder: SocketAddr,
        corpse_addr: Option<SocketAddr>,
    ) -> Result<Key, StoreError> {
        self.handle_possible_corpse(corpse_addr).await;

        let mut secondary_store = self.secondary_store.write().await;

        let key = Key::hash(&value);

        secondary_store
            .entry(primary_holder)
            .or_insert(HashMap::new())
            .insert(key, value);

        Ok(key)
    }

    #[instrument(skip(self), fields(this=?self.addr))]
    pub async fn get_rcvd(self: &Arc<Self>, key: Key) -> Result<(String, SocketAddr), GetError> {
        let primary_store = self.primary_store.read().await;

        if let Some(value) = primary_store.get(&key) {
            debug!("I have it!");
            return Ok((value.clone(), self.addr));
        }

        drop(primary_store);

        match self.peers.nearest_to(&key).await {
            Some(peer) => self.delegate_get(peer, key).await,
            None => Err(GetError::NotFound(NotFoundError {
                key,
                originating_node: self.addr,
            })),
        }
    }

    #[instrument(skip(self), fields(this=?self.addr))]
    async fn delegate_get(
        self: &Arc<Self>,
        peer: OtherNode,
        key: Key,
    ) -> Result<(String, SocketAddr), GetError> {
        info!("Delegating get");
        match Client::get(&peer.addr, &key).await {
            Ok(res) => Ok(res),
            Err(ClientError::ConnectionFailed(_)) => {
                Err(GetError::Common(self.handle_connection_failed(peer).await))
            }
            Err(ClientError::MalformedResponse) => {
                Err(GetError::Common(CommonError::Internal(InternalError {
                    message: "got malformed response from client".to_string(),
                })))
            }
            Err(ClientError::Status(err)) => Err(GetError::Common(CommonError::Status(err))),
        }
    }

    pub async fn mark_alive(&self, peer: &OtherNode) {
        let mut known_peers = self.peers.known_peers.write().await;

        let mut peer = known_peers
            .get_mut(&peer.addr)
            .expect("This really should be here...");

        peer.last_seen = Instant::now();
    }

    #[instrument(skip(self), fields(this=?self.addr))]
    pub async fn mark_dead(self: &Arc<Self>, dead_peer: &OtherNode) {
        warn!(this=?self.addr, dead_peer=?dead_peer, "Marking a death");
        info!("Waiting to acquire peer lock");
        let mut known_peers = self.peers.known_peers.write().await;
        info!("Peer lock acquired");

        if let Some(_) = known_peers.remove_entry(&dead_peer.addr) {
            drop(known_peers);
            let self_clone = Arc::clone(self);
            let dead_peer_clone = dead_peer.clone();

            tokio::spawn(async move {
                self_clone
                    .redistribute_primary_data_for(&dead_peer_clone)
                    .await;
            });

            let self_clone = Arc::clone(self);
            let dead_peer_clone = dead_peer.clone();

            tokio::spawn(async move {
                self_clone
                    .redistribute_secondary_data_for(&dead_peer_clone)
                    .await;
            });
        } else {
            info!("I had already forgotten.")
        }
    }

    #[instrument(skip(self), fields(this=?self.addr))]
    async fn redistribute_primary_data_for(self: &Arc<Self>, corpse: &OtherNode) {
        info!("Do i have any primary data to redistribute?");
        let secondants = self.secondants.read().await;

        let keys_to_find_new_secondants_for: Vec<_> = secondants
            .iter()
            .filter(|(_, peer)| peer.addr == corpse.addr)
            .map(|(key, _)| key)
            .collect();

        drop(&secondants); // we need to drop this immediately so the spawned tasks can write to them later.

        let primary_store = self.primary_store.read().await;
        info!(keys_to_find_new_secondants_for=?keys_to_find_new_secondants_for);

        for key in keys_to_find_new_secondants_for {
            info!("Finding new secondants for {}", key);
            let self_clone = Arc::clone(self);
            let value = primary_store
                .get(&key)
                .expect("Key in secondants was not in primary_store")
                .clone();

            let corpse_clone = corpse.clone();

            tokio::spawn(async move {
                self_clone
                    .store_in_secondants(value, 1, Some(&corpse_clone))
                    .await;
            });
        }
    }

    #[instrument(skip(self), fields(this=?self.addr))]
    async fn redistribute_secondary_data_for(self: &Arc<Self>, corpse: &OtherNode) {
        let secondary_store = self.secondary_store.read().await;

        info!("redistributing secondary data for {:?}", corpse);

        let tasks: Vec<JoinHandle<Result<(Key, SocketAddr), StoreError>>> =
            match secondary_store.get(&corpse.addr) {
                Some(store) => {
                    // drop(secondary_store)
                    store
                        .into_iter()
                        .map(|(_, value)| {
                            let value = value.clone();
                            let self_clone = Arc::clone(self);
                            let corpse_addr = corpse.addr.clone();

                            tokio::spawn(async move {
                                //     // self_clone.store_rcvd(value, Some(corpse_addr)).await // TYPECHECK LOOP ;_;
                                if let Some(freshest_peer) = self_clone.peers.freshest().await {
                                    self_clone
                                        .delegate_store(freshest_peer, value, Some(corpse_addr))
                                        .await
                                } else {
                                    warn!(
                                    "All alone. I can store {} myself, I guess, it's ok. I'm fine.",
                                    value
                                );
                                    let key = &Key::hash(&value);
                                    self_clone.store_in_primary(key, value).await;
                                    return Ok((key.clone(), self_clone.addr));
                                }
                            })
                        })
                        .collect()
                }
                None => {
                    debug!("Not holding any secondary data for {:?}", corpse);
                    return;
                }
            };

        drop(secondary_store);

        for task in tasks {
            match task.await.expect("join error?!") {
                Ok((key, peer)) => {
                    let mut secondary_store = self.secondary_store.write().await;
                    let store = secondary_store.get_mut(&corpse.addr);
                    match store {
                        Some(store) => {
                            let val = store.remove(&key);
                            if val.is_none() {
                                warn!("Tried to remove ({}, {}) from secondary store but {} wasn't there.", key, peer, key)
                            }
                        }
                        None => {
                            warn!("Tried to remove ({}, {}) from secondary store but {} wasn't there.", key, peer, peer);
                        }
                    }
                }
                Err(err) => {
                    error!("Restoring values from secondary failed: {:?}", err);
                }
            }
        }
    }

    #[instrument(skip(self), fields(this=?self.addr))]
    async fn handle_possible_corpse(self: &Arc<Self>, corpse_addr: Option<SocketAddr>) {
        if let Some(corpse_addr) = corpse_addr {
            warn!(corpse=?corpse_addr, "Marking corpse as dead thanks to context from other node");
            let peers = self.peers.known_peers.read().await;
            if let Some(corpse) = peers.get(&corpse_addr).cloned() {
                drop(peers);
                self.mark_dead(&corpse).await;
            } else {
                warn!("Could not find corpse in peers");
            }
        }
    }
}

#[derive(Debug)]
pub struct Peers {
    pub known_peers: Arc<RwLock<HashMap<SocketAddr, OtherNode>>>,
}

impl Peers {
    pub async fn stalest(&self) -> Option<OtherNode> {
        let known_peers = self.known_peers.read().await;

        known_peers
            .values()
            .cloned()
            .max_by_key(|p| Instant::now() - p.last_seen)
    }

    pub async fn freshest(&self) -> Option<OtherNode> {
        let known_peers = self.known_peers.read().await;

        known_peers
            .values()
            .cloned()
            .min_by_key(|p| Instant::now() - p.last_seen)
    }

    pub async fn nearest_to(&self, key: &Key) -> Option<OtherNode> {
        debug!("attempting to acquire known_peers lock");
        let known_peers = self.known_peers.read().await;
        debug!("known_peers lock acquired!");

        known_peers
            .values()
            .cloned()
            .min_by_key(|peer| peer.distance_to(key))
    }

    pub async fn nearest_to_but_less_than(
        &self,
        key: &Key,
        min_distance: &Key,
    ) -> Option<OtherNode> {
        self.nearest_to(&key).await.and_then(|peer| {
            if &peer.distance_to(&key) < min_distance {
                Some(peer)
            } else {
                None
            }
        })
    }

    pub async fn pick(&self, n: usize) -> Vec<OtherNode> {
        let peers = self.known_peers.read().await;

        let mut rng = rand::rngs::ThreadRng::default();
        peers
            .values()
            .cloned()
            // FIXME: this min here means that when the network is smaller than REDUNDANCY,
            // the number of stewards for this value will stay at the same size
            .choose_multiple(&mut rng, std::cmp::min(n, peers.len()))
    }

    // returns the nodes before the new one was added!
    pub async fn introduce(&self, addr: &SocketAddr) -> (Vec<OtherNode>, OtherNode) {
        let mut peers = self.known_peers.write().await;

        let old_peers = peers.values().cloned().collect();

        let new_peer = OtherNode::new(addr.clone(), Instant::now());
        peers.insert(new_peer.addr, new_peer.clone());
        info!("Introduced {}", &new_peer.addr);

        (old_peers, new_peer)
    }
}

impl std::hash::Hash for OtherNode {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.addr.hash(state);
    }
}

impl PartialEq for OtherNode {
    fn eq(&self, other: &Self) -> bool {
        self.addr == other.addr
    }
}

impl std::fmt::Debug for OtherNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OtherNode")
            .field("addr", &self.addr)
            .finish()
    }
}
