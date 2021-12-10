use mockall_double::double;
use rand::seq::IteratorRandom;
use std::collections::HashMap;
use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use tokio::task::JoinHandle;
use tokio::time::{timeout, Duration, Instant};

#[double]
use crate::peering::client::Client;

use crate::peering::errors::*;

use tracing::*;

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
    pub secondant_map: Arc<RwLock<HashMap<Key, HashSet<OtherNode>>>>,
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
            secondant_map: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    fn distance_to(&self, key: &Key) -> Key {
        self.hash.cyclic_distance(&key)
    }

    #[instrument(skip(self))]
    pub async fn acquire_primary_store_read(&self) -> RwLockReadGuard<'_, HashMap<Key, String>> {
        match timeout(Duration::from_secs(3), self.primary_store.read()).await {
            Ok(x) => x,
            Err(_) => {
                error!("Failed to acquire primary data read lock.");
                panic!("Deadlocked")
            }
        }
    }

    #[instrument(skip(self))]
    pub async fn acquire_primary_store_write(&self) -> RwLockWriteGuard<'_, HashMap<Key, String>> {
        match timeout(Duration::from_secs(3), self.primary_store.write()).await {
            Ok(x) => x,
            Err(_) => {
                error!("Failed to acquire primary data write lock.");
                panic!("Deadlocked")
            }
        }
    }

    #[instrument(skip(self))]
    pub async fn acquire_secondary_store_read(
        &self,
    ) -> RwLockReadGuard<'_, HashMap<SocketAddr, HashMap<Key, String>>> {
        match timeout(Duration::from_secs(3), self.secondary_store.read()).await {
            Ok(x) => x,
            Err(_) => {
                error!("Failed to acquire secondary data read lock.");
                panic!("Deadlocked")
            }
        }
    }

    #[instrument(skip(self))]
    pub async fn acquire_secondary_store_write(
        &self,
    ) -> RwLockWriteGuard<'_, HashMap<SocketAddr, HashMap<Key, String>>> {
        match timeout(Duration::from_secs(3), self.secondary_store.write()).await {
            Ok(x) => x,
            Err(_) => {
                error!("Failed to acquire secondary data write lock.");
                panic!("Deadlocked")
            }
        }
    }

    #[instrument(skip(self))]
    pub async fn acquire_secondant_map_read(
        &self,
    ) -> RwLockReadGuard<'_, HashMap<Key, HashSet<OtherNode>>> {
        match timeout(Duration::from_secs(3), self.secondant_map.read()).await {
            Ok(x) => x,
            Err(_) => {
                error!("Failed to acquire secondant map read lock.");
                panic!("Deadlocked")
            }
        }
    }
    #[instrument(skip(self), fields(this=?self.addr))]
    pub async fn acquire_secondant_map_write(
        &self,
    ) -> RwLockWriteGuard<'_, HashMap<Key, HashSet<OtherNode>>> {
        match timeout(Duration::from_secs(3), self.secondant_map.write()).await {
            Ok(x) => x,
            Err(_) => {
                error!("Failed to acquire secondant map write lock.");
                panic!("Deadlocked")
            }
        }
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
            self_clone.clean_for(&new_peer_clone).await;
        });

        Ok(old_peers)
    }

    #[instrument(skip(self), fields(this=?self.addr))]
    async fn clean_for(self: &Arc<Self>, peer: &OtherNode) {
        let moved_keys = self.move_values_to(&peer).await;

        let moved_keys: HashSet<Key> = HashSet::from_iter(moved_keys.into_iter());
        self.clean_secondant_map(&moved_keys).await;

        self.clean_secondary_store_after_move_to(&peer).await;
    }

    #[instrument(skip(self), fields(this=?self.addr))]
    async fn clean_secondary_store_after_move_to(self: &Arc<Self>, peer: &OtherNode) {
        let mut secondary_store = self.acquire_secondary_store_write().await;

        // if the new peer is closer than the one we are secondant for:
        let to_remove = secondary_store
            .clone()
            .into_iter()
            .map(|(addr, store)| {
                store
                    .into_iter()
                    .map(move |(k, _)| (addr.clone(), k.clone()))
            })
            .flatten()
            .filter(|(addr, key)| {
                peer.distance_to(key) < Key::hash(&addr.to_string()).cyclic_distance(key)
            })
            .collect::<Vec<_>>();

        info!(removing=?to_remove, "Cleaning peer references out of secondary store");

        for (addr, key) in to_remove {
            info!("cleaning out of secondary store: {:?}", (addr, key));
            secondary_store.get_mut(&addr).map(|store| {
                store.remove(&key);
            });
        }
    }

    #[instrument(skip(self), fields(this=?self.addr))]
    async fn clean_secondant_map(self: &Arc<Self>, keys: &HashSet<Key>) {
        info!(keys=?keys, "Cleaning keys from secondant map");

        let mut secondant_map = self.acquire_secondant_map_write().await;
        let to_remove = secondant_map
            .clone()
            .into_iter()
            .filter(|(key, _)| keys.contains(key))
            .collect::<Vec<_>>();

        for (key, _) in to_remove {
            let old_secondant = secondant_map.remove(&key);
            info!("Removed {:?} as secondant for {}", old_secondant, key);
        }
    }

    #[instrument(skip(self), fields(this=?self.addr))]
    async fn move_values_to(self: &Arc<Self>, peer: &OtherNode) -> Vec<Key> {
        let primary_store = self.acquire_primary_store_read().await;
        let values_to_move = primary_store
            .iter()
            .filter(|(key, _value)| peer.distance_to(key) < self.distance_to(key))
            .map(|(_, v)| v)
            .cloned()
            .collect::<Vec<_>>();

        if values_to_move.len() == 0 {
            return vec![];
        }

        drop(primary_store); // don't hold the lock while we wait for the other node.
                             // this allows other nodes that don't known about
                             // the new peer yet still get the value here in the meantime

        info!("Values to move: {:?}", values_to_move);

        match Client::move_values(&peer.addr, values_to_move).await {
            Ok(keys) => {
                let mut primary_store = self.acquire_primary_store_write().await;

                for key in &keys {
                    let removed_value = primary_store.remove(&key);
                    match removed_value {
                        Some(removed_value) => {
                            info!("{} removed from primary store", removed_value);
                        }
                        None => {
                            warn!("Tried to remove a value that wasn't here (anymore?)")
                        }
                    }
                }
                return keys;
            }
            Err(err) => {
                error!("Failed to move values: {:?}", err);
                return vec![];
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

        info!(key=%key);

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
        info!("Delegating store");
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
        let mut primary_store = self.acquire_primary_store_write().await;
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
        info!("Stored here.");

        if inserted.is_none() {
            let self_clone = Arc::clone(self);
            let value_clone = value.clone();
            let corpse_clone = corpse.cloned();
            tokio::task::spawn(async move {
                self_clone
                    .store_in_secondant_map(
                        value_clone,
                        self_clone.redundancy,
                        corpse_clone.as_ref(),
                    )
                    .await;
            });
        }
    }

    #[instrument(skip(self), fields(this=?self.addr))]
    async fn pick_secondants(self: &Arc<Self>, n: u8) -> Vec<OtherNode> {
        if n == 0 {
            return vec![];
        }

        let secondant_map = self.acquire_secondant_map_read().await;
        let peers = self.peers.acquire_read().await;

        let mut secondant_counts: HashMap<OtherNode, u64> = HashMap::new();

        for secondants in secondant_map.values() {
            for peer in secondants.iter().cloned() {
                let count = secondant_counts.entry(peer).or_insert(0);
                *count += 1;
            }
        }

        let peer_counts: Vec<_> = peers
            .values()
            .map(|p| (p, secondant_counts.get(p).cloned().unwrap_or(0)))
            .collect();

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

        lowest_vec.into_iter().map(|(p, _)| p.clone()).collect()
    }

    #[instrument(skip(self), fields(this=?self.addr))]
    async fn store_in_secondant_map(
        self: &Arc<Self>,
        value: String,
        n_secondants: u8,
        corpse: Option<&OtherNode>,
    ) {
        let mut n_secondants_assigned = 0;

        while n_secondants_assigned < n_secondants {
            info!(
                "Picking {} new secondant for {}",
                n_secondants - n_secondants_assigned,
                value
            );
            let new_secondants = self
                .pick_secondants(n_secondants - n_secondants_assigned)
                .await;

            if new_secondants.len() == 0 {
                warn!(
                    "All my friends are dead (╥_╥. Can't choose enough secondants for {})",
                    value
                );
                return;
            }

            let tasks: Vec<_> = new_secondants
                .into_iter()
                .map(|peer| {
                    let value = value.clone();
                    let primary_holder = self.addr.clone();
                    let peer_clone = peer.clone();
                    let corpse_clone = corpse.cloned();
                    info!("Chose {:?} as secondant for {}", &peer, &value);
                    tokio::spawn(async move {
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
                    Ok((peer, key)) => {
                        self.store_as_secondant(peer, key).await;
                        n_secondants_assigned += 1;
                    }
                    Err(_) => {
                        warn!("client error when assigning secondant, will attempt to choose a new one");
                    }
                }
            }
        }
    }

    #[instrument(skip(self), fields(this=?self.addr))]
    pub async fn store_as_secondant(&self, peer: OtherNode, key: Key) {
        let mut secondant_map = self.acquire_secondant_map_write().await;
        let secondants = secondant_map.entry(key).or_insert(HashSet::new());
        secondants.insert(peer);
    }

    #[instrument(skip(self), fields(this=?self.addr))]
    pub async fn secondary_store_rcvd(
        self: &Arc<Self>,
        value: String,
        primary_holder: SocketAddr,
        corpse_addr: Option<SocketAddr>,
    ) -> Result<Key, StoreError> {
        self.handle_possible_corpse(corpse_addr).await;

        let mut secondary_store = self.acquire_secondary_store_write().await;

        let key = Key::hash(&value);

        secondary_store
            .entry(primary_holder)
            .or_insert(HashMap::new())
            .insert(key, value);

        Ok(key)
    }

    #[instrument(skip(self), fields(this=?self.addr))]
    pub async fn get_rcvd(self: &Arc<Self>, key: Key) -> Result<(String, SocketAddr), GetError> {
        let primary_store = self.acquire_primary_store_read().await;

        if let Some(value) = primary_store.get(&key) {
            debug!("I have it!");
            return Ok((value.clone(), self.addr));
        }

        drop(primary_store);

        match self
            .peers
            .nearest_to_but_less_than(&key, &self.distance_to(&key))
            .await
        {
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

    #[instrument(skip(self), fields(this=?self.addr))]
    pub async fn mark_alive(&self, peer: &OtherNode) {
        let mut known_peers = self.peers.acquire_write().await;

        let mut peer = known_peers
            .get_mut(&peer.addr)
            .expect("This really should be here...");

        peer.last_seen = Instant::now();
    }

    #[instrument(skip(self), fields(this=?self.addr))]
    pub async fn mark_dead(self: &Arc<Self>, dead_peer: &OtherNode) {
        warn!(this=?self.addr, dead_peer=?dead_peer, "Marking a death");
        let mut known_peers = self.peers.acquire_write().await;

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
        let secondant_map = self.acquire_secondant_map_read().await;

        let keys_to_find_new_secondants_for: Vec<_> = secondant_map
            .iter()
            .filter(|(_, secondants)| secondants.contains(corpse))
            .map(|(key, _)| key)
            .collect();

        drop(&secondant_map); // we need to drop this immediately so the spawned tasks can write to them later.

        let primary_store = self.acquire_primary_store_read().await;
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
                    .store_in_secondant_map(value, 1, Some(&corpse_clone))
                    .await;
            });
        }
    }

    #[instrument(skip(self), fields(this=?self.addr))]
    async fn redistribute_secondary_data_for(self: &Arc<Self>, corpse: &OtherNode) {
        let secondary_store = self.acquire_secondary_store_read().await;

        info!("redistributing secondary data for {:?}", corpse);

        let mut values_to_redistribute: HashSet<String> = match secondary_store.get(&corpse.addr) {
            Some(store) => HashSet::from_iter(store.values().cloned()),
            None => {
                debug!("Not holding any secondary data for {:?}", corpse);
                return;
            }
        };
        drop(secondary_store);
        let mut tries = 0;

        while values_to_redistribute.len() > 0 && tries < self.peers.len().await {
            let tasks: Vec<JoinHandle<Result<(Key, String, SocketAddr), StoreError>>> =
                values_to_redistribute
                    .iter()
                    .map(|value| {
                        let value = value.clone();
                        let self_clone = Arc::clone(self);
                        let corpse_addr = corpse.addr.clone();
                        tries += 1;
                        tokio::spawn(async move {
                             // self_clone.store_rcvd(value, Some(corpse_addr)).await // TYPECHECK LOOP ;_;
                            match self_clone.peers.freshest().await {
                                Some(freshest_peer) => {
                                    let (key, addr) = self_clone
                                        .delegate_store(freshest_peer, value.clone(), Some(corpse_addr))
                                        .await?;
                                    Ok((key, value, addr))
                                },
                                None => {
                                    warn!(
                                        "All alone. I can store {} myself, I guess, it's ok. I'm fine.",
                                        value
                                    );
                                    let key = &Key::hash(&value);
                                    self_clone.store_in_primary(key, value.clone()).await;
                                    return Ok((key.clone(), value, self_clone.addr));
                                }
                            }})
                        })
                        .collect();

            for task in tasks {
                match task.await.expect("join error?!") {
                    Ok((key, value, peer)) => {
                        let mut secondary_store = self.acquire_secondary_store_write().await;
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

                        values_to_redistribute.remove(&value);
                    }
                    Err(err) => {
                        warn!(
                            "Restoring value from secondary failed (but retrying): {:?}",
                            err
                        );
                    }
                }
            }
        }
        if values_to_redistribute.len() != 0 {
            error!(
                "Restoring values {:?} from secondary failed completely:",
                values_to_redistribute
            )
        }
    }

    #[instrument(skip(self), fields(this=?self.addr))]
    async fn handle_possible_corpse(self: &Arc<Self>, corpse_addr: Option<SocketAddr>) {
        if let Some(corpse_addr) = corpse_addr {
            warn!(corpse=?corpse_addr, "Marking corpse as dead thanks to context from other node");
            let peers = self.peers.acquire_read().await;
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
    #[instrument(skip(self))]
    pub async fn acquire_read(&self) -> RwLockReadGuard<'_, HashMap<SocketAddr, OtherNode>> {
        match timeout(Duration::from_secs(3), self.known_peers.read()).await {
            Ok(x) => x,
            Err(_) => {
                error!("Failed to acquire peers read lock.");
                panic!("Deadlocked")
            }
        }
    }

    #[instrument(skip(self))]
    pub async fn acquire_write(&self) -> RwLockWriteGuard<'_, HashMap<SocketAddr, OtherNode>> {
        match timeout(Duration::from_secs(3), self.known_peers.write()).await {
            Ok(x) => x,
            Err(_) => {
                error!("Failed to acquire peers write lock.");
                panic!("Deadlocked")
            }
        }
    }

    pub async fn stalest(&self) -> Option<OtherNode> {
        let known_peers = self.acquire_read().await;

        known_peers
            .values()
            .cloned()
            .max_by_key(|p| Instant::now() - p.last_seen)
    }

    pub async fn freshest(&self) -> Option<OtherNode> {
        let known_peers = self.acquire_read().await;

        known_peers
            .values()
            .cloned()
            .min_by_key(|p| Instant::now() - p.last_seen)
    }

    #[instrument(skip(self))]
    pub async fn nearest_to(&self, key: &Key) -> Option<OtherNode> {
        let known_peers = self.acquire_read().await;

        known_peers
            .values()
            .cloned()
            .min_by_key(|peer| peer.distance_to(key))
    }

    #[instrument(skip(self))]
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

    #[instrument(skip(self))]
    pub async fn pick(&self, n: usize) -> Vec<OtherNode> {
        let known_peers = self.acquire_read().await;

        let mut rng = rand::rngs::ThreadRng::default();
        known_peers
            .values()
            .cloned()
            // FIXME: this min here means that when the network is smaller than REDUNDANCY,
            // the number of stewards for this value will stay at the same size
            .choose_multiple(&mut rng, std::cmp::min(n, known_peers.len()))
    }

    // returns the nodes before the new one was added!
    #[instrument(skip(self))]
    pub async fn introduce(&self, addr: &SocketAddr) -> (Vec<OtherNode>, OtherNode) {
        let mut peers = self.acquire_write().await;

        let old_peers = peers.values().cloned().collect();

        let new_peer = OtherNode::new(addr.clone(), Instant::now());
        peers.insert(new_peer.addr, new_peer.clone());
        info!("Introduced {}", &new_peer.addr);

        (old_peers, new_peer)
    }

    #[instrument(skip(self))]
    pub async fn len(&self) -> usize {
        let known_peers = self.acquire_read().await;

        known_peers.len()
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

#[cfg(test)]
mod tests {
    use tokio::test;

    use super::{Client, Key, ThisNode};
    use crate::peering::utils::ipv6_loopback_socketaddr as a;
    use lazy_static::lazy_static;
    use maplit::hashmap;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    fn h(s: &str) -> Key {
        h_s(s.to_string())
    }

    fn h_s(s: String) -> Key {
        Key::hash(&s)
    }

    fn s(s: &str) -> String {
        s.to_string()
    }

    lazy_static! {
        static ref CLIENT_MTX: Mutex<()> = Mutex::new(());
    }

    #[test]
    async fn test_get_here() {
        let _m = CLIENT_MTX.lock().unwrap();

        let ctx = Client::get_context();
        ctx.expect().never();

        let k = h("test");
        let node = Arc::new(ThisNode::new(a(1234), 1));

        node.peers.introduce(&a(4567)).await;
        node.store_in_primary(&k, s("test")).await;

        let (value, addr) = node.get_rcvd(k).await.unwrap();

        assert_eq!(value, "test");
        assert_eq!(addr, a(1234));
        ctx.checkpoint();
    }

    #[test]
    async fn test_get_delegated() {
        let _m = CLIENT_MTX.lock().unwrap();

        let ctx = Client::get_context();
        ctx.expect()
            .returning(|_, _| Ok(("avalue".to_string(), a(1234))));

        let node = Arc::new(ThisNode::new(a(1234), 1));

        let (_, peer4567) = node.peers.introduce(&a(4567)).await;

        let k = h("bear");

        assert!(node.distance_to(&k) > peer4567.distance_to(&k));

        let (value, addr) = node.get_rcvd(k).await.unwrap();

        assert_eq!(value, "avalue");
        assert_eq!(addr, a(1234));
        ctx.checkpoint();
    }

    #[test]
    async fn test_mingle() {
        let _m = CLIENT_MTX.lock().unwrap();

        let ctx = Client::introduce_context();
        ctx.expect().times(3).returning(|addr, sender_addr| {
            assert_eq!(sender_addr, &a(1111));
            match addr.port() {
                2222 => Ok(vec![a(3333), a(4444)]),
                3333 => Ok(vec![a(2222), a(4444)]),
                4444 => Ok(vec![a(2222), a(3333)]),
                _ => {
                    panic!("unexpected call")
                }
            }
        });

        let node = Arc::new(ThisNode::new(a(1111), 1));

        node.mingle(&a(2222)).await;

        let peers = node.peers.known_peers.read().await;
        assert_eq!(peers.len(), 3);
        assert!(peers.get(&a(2222)).is_some());
        assert!(peers.get(&a(3333)).is_some());
        assert!(peers.get(&a(4444)).is_some());
        ctx.checkpoint();
    }

    #[test]
    async fn test_clean_secondary_store() {
        let _m = CLIENT_MTX.lock().unwrap();
        let node = Arc::new(ThisNode::new(a(1111), 2));

        let ctx = Client::store_context();
        ctx.expect().returning(|addr, value, corpse| {
            println!("Client::store({}, {}, {:?})", addr, value, corpse);
            Ok((h_s(value), addr.clone()))
        });

        fn map(vs: Vec<&str>) -> HashMap<Key, String> {
            HashMap::from_iter(vs.iter().map(|v| (h(v), s(v))))
        }

        let (_, peer2) = node.peers.introduce(&a(2222)).await;
        let (_, peer3) = node.peers.introduce(&a(3333)).await;
        let (_, peer4) = node.peers.introduce(&a(4444)).await;
        let (_, peer5) = node.peers.introduce(&a(5555)).await;

        node.store_rcvd(s("foo"), None).await.unwrap(); // for 2222
        node.store_rcvd(s("bar"), None).await.unwrap(); // for 2222 but will be moved
        node.store_rcvd(s("bear"), None).await.unwrap(); // for 3333 but will be moved
        node.store_rcvd(s("doggo"), None).await.unwrap(); // for 4444
        node.store_rcvd(s("foxxie"), None).await.unwrap(); // for 5555

        {
            let mut secondary_store = node.acquire_secondary_store_write().await;
            secondary_store.insert(a(2222), map(vec!["foo", "bar"]));
            secondary_store.insert(a(3333), map(vec!["bear"]));
            secondary_store.insert(a(4444), map(vec!["doggo"]));
            secondary_store.insert(a(5555), map(vec!["foxxie", "bar"]));
            // (^ bar should not really be here but it should still be removed)
            println!("{:#?}", secondary_store);
        }
        //
        let (_, peer6) = node.peers.introduce(&a(9393)).await;

        println!("peer2: {:?}", peer2.key);
        println!("peer3: {:?}", peer3.key);
        println!("peer4: {:?}", peer4.key);
        println!("peer5: {:?}", peer5.key);
        println!("peer6: {:?}", peer6.key);

        node.clean_secondary_store_after_move_to(&peer6).await;
        let secondary_store = node.acquire_secondary_store_read().await;
        println!("{:#?}", secondary_store);

        assert_eq!(
            secondary_store.clone(),
            hashmap! {
                a(2222) => hashmap!{h("foo") => s("foo")},
                a(3333) => hashmap!{h("bear") => s("bear")},
                a(4444) => hashmap!{},
                a(5555) => hashmap!{h("foxxie") => s("foxxie")},
            }
        );
    }
}
