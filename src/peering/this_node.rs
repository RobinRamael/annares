use rand::seq::IteratorRandom;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tokio::time::{Duration, Instant};

use crate::peering::client::Client;
use crate::peering::errors::*;
use crate::peering::utils::shorten;

use tracing::{error, info, instrument, warn};

type Key = crate::peering::hash::Hash;

const REDUNDANCY: usize = 1;

#[derive(Clone)]
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
}

#[derive(Debug)]
pub struct ThisNode {
    pub addr: SocketAddr,
    hash: Key,
    pub peers: Peers,
    pub primary_store: Arc<RwLock<HashMap<Key, String>>>,
    pub secondary_store: Arc<RwLock<HashMap<SocketAddr, Vec<(Key, String)>>>>,
    pub secondants: Arc<RwLock<HashMap<Key, OtherNode>>>,
}

impl ThisNode {
    pub fn new(addr: SocketAddr) -> Self {
        ThisNode {
            addr,
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

    pub async fn mingle(&self, bootstrap_addr: &SocketAddr) {
        self.peers.introduce(bootstrap_addr).await;

        match Client::introduce(bootstrap_addr, &self.addr).await {
            Ok(new_peers) => {
                for addr in new_peers {
                    if addr != self.addr && &addr != bootstrap_addr {
                        match Client::introduce(&addr, &self.addr).await {
                            Ok(peers) => {
                                for peer in peers {
                                    self.peers.introduce(&peer).await;
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

    #[instrument(skip(self), fields(port=?self.addr))]
    pub async fn introduction_rcvd(
        self: &Arc<Self>,
        sender_addr: SocketAddr,
    ) -> Result<Vec<OtherNode>, IntroductionError> {
        let (old_peers, new_peer) = self.peers.introduce(&sender_addr).await;
        info!(peer=?new_peer, "New peer just dropped");

        let self_clone = Arc::clone(self);
        let new_peer_clone = new_peer.clone();

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await; // FIXME: do retries instead of this
            info!("Moving values to {:?}", new_peer_clone);
            self_clone.move_values_to_peer(new_peer_clone).await;
        });

        Ok(old_peers)
    }

    #[instrument(skip(self), fields(port=?self.addr))]
    async fn move_values_to_peer(self: &Arc<Self>, peer: OtherNode) {
        let primary_store = self.primary_store.read().await;
        let values_to_move = primary_store
            .iter()
            .filter(|(key, _value)| peer.distance_to(key) < self.distance_to(key))
            .map(|(_, v)| v)
            .cloned()
            .collect();

        info!("Values to move: {:?}", values_to_move);

        drop(primary_store); // don't hold the lock while we wait for the other node.
                             // this allows other nodes that don't known about
                             // the new peer yet still get the value here in the meantime

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

    #[instrument(skip(self), fields(port=?self.addr))]
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
                    tokio::spawn(async move { Client::store(&peer.addr, value).await });
                }
                None => {
                    self.store_here(&key, value).await;
                    stored_keys.push(key);
                }
            };
        }

        Ok(stored_keys)
    }

    #[instrument(skip(self), fields(port=?self.addr))]
    pub async fn store_rcvd(
        self: &Arc<Self>,
        value: String,
    ) -> Result<(Key, SocketAddr), StoreError> {
        let key = Key::hash(&value);

        match self
            .peers
            .nearest_to_but_less_than(&key, &self.distance_to(&key))
            .await
        {
            Some(peer) => self.delegate_store(peer, value).await,
            None => {
                self.store_here(&key, value).await;
                Ok((key, self.addr))
            }
        }
    }

    #[instrument(skip(self), fields(port=?self.addr))]
    async fn handle_connection_failed(self: &Arc<Self>, peer: OtherNode) -> CommonError {
        warn!("A body was found!");
        // self.mark_dead(&peer).await; // FIXME: causes a typecheck loop
        // FIXME: is there more we can try here? wait for the secondary to notice
        // this death or try to find the secondary ourselves?
        CommonError::Unavailable
    }

    #[instrument(skip(self), fields(port=?self.addr))]
    async fn delegate_store(
        self: &Arc<Self>,
        peer: OtherNode,
        value: String,
    ) -> Result<(Key, SocketAddr), StoreError> {
        match Client::store(&peer.addr, value).await {
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

    #[instrument(skip(self), fields(port=?self.addr))]
    async fn store_here(self: &Arc<Self>, key: &Key, value: String) -> () {
        let mut primary_store = self.primary_store.write().await;
        let inserted = primary_store.insert(key.clone(), value.clone());

        if inserted.is_none() {
            let self_clone = Arc::clone(self);
            let value_clone = value.clone();
            tokio::task::spawn(async move {
                self_clone
                    .store_in_secondants(value_clone, REDUNDANCY)
                    .await;
            });
        }
    }

    #[instrument(skip(self), fields(port=?self.addr))]
    async fn store_in_secondants(self: &Arc<Self>, value: String, n: usize) {
        let tasks: Vec<_> = self
            .peers
            .pick(n)
            .await
            .into_iter()
            .map(|peer| {
                let value = value.clone();
                let primary_holder = self.addr.clone();
                let peer_clone = peer.clone();
                tokio::spawn(async move {
                    match Client::store_secondary(&peer_clone.addr, value, &primary_holder).await {
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

    #[instrument(skip(self), fields(port=?self.addr))]
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
                info!("Stored {:?} as secondant for {:?} ", key, peer.addr);
            }
        }
    }

    #[instrument(skip(self), fields(port=?self.addr))]
    pub async fn secondary_store_rcvd(
        self: &Arc<Self>,
        value: String,
        primary_holder: SocketAddr,
    ) -> Result<Key, StoreError> {
        let mut secondary_store = self.secondary_store.write().await;

        let key = Key::hash(&value);

        secondary_store
            .entry(primary_holder)
            .or_insert(vec![])
            .push((key.clone(), value));

        Ok(key)
    }

    #[instrument(skip(self), fields(port=?self.addr))]
    pub async fn get_rcvd(self: &Arc<Self>, key: Key) -> Result<(String, SocketAddr), GetError> {
        let primary_store = self.primary_store.read().await;

        if let Some(value) = primary_store.get(&key) {
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

    #[instrument(skip(self), fields(port=?self.addr))]
    async fn delegate_get(
        self: &Arc<Self>,
        peer: OtherNode,
        key: Key,
    ) -> Result<(String, SocketAddr), GetError> {
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

    pub async fn mark_dead(self: &Arc<Self>, dead_peer: &OtherNode) {
        let mut known_peers = self.peers.known_peers.write().await;

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
        } // else another call got to it first?
    }

    async fn redistribute_primary_data_for(self: &Arc<Self>, dead_peer: &OtherNode) {
        let secondants = self.secondants.read().await;

        let keys_to_find_new_secondants_for: Vec<_> = secondants
            .iter()
            .filter(|(_, peer)| peer.addr == dead_peer.addr)
            .map(|(key, _)| key)
            .collect();

        drop(&secondants); // we need to drop this immediately so the spawned tasks can write to them later.

        let primary_store = self.primary_store.read().await;

        for key in keys_to_find_new_secondants_for {
            let self_clone = Arc::clone(self);
            let value = primary_store
                .get(&key)
                .expect("Key in secondants was not in primary_store")
                .clone();

            tokio::spawn(async move {
                self_clone.store_in_secondants(value, 1).await;
            });
        }
    }

    async fn redistribute_secondary_data_for(self: &Arc<Self>, dead_peer: &OtherNode) {
        let secondary_store = self.secondary_store.read().await;

        let tasks: Vec<JoinHandle<Result<(Key, SocketAddr), StoreError>>> =
            match secondary_store.get(&dead_peer.addr) {
                Some(store) => {
                    // drop(secondary_store)
                    store
                        .into_iter()
                        .map(|(_, value)| {
                            let value = value.clone();
                            let self_clone = Arc::clone(self);

                            // we need the value to end up somewhere in the network, so
                            // we can just pretend we received a store call for it.
                            tokio::spawn(async move { self_clone.store_rcvd(value).await })
                        })
                        .collect()
                }
                None => {
                    info!("Not holding any secondary data for {:?}", dead_peer);
                    return;
                }
            };
        drop(secondary_store);

        for task in tasks {
            match task.await.expect("join error?!") {
                Ok((key, peer)) => {
                    let mut secondary_store = self.secondary_store.write().await;
                    let kvs = secondary_store.get_mut(&dead_peer.addr);
                    match kvs {
                        Some(kvs) => {
                            info!("looking for {:?} in {:?}", &key, &kvs);
                            if let Some(idx) = kvs.iter().position(|(k, _)| *k == key) {
                                // TODO: why is this not just as hashset?
                                kvs.swap_remove(idx);
                            } else {
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

    pub async fn nearest_to(&self, key: &Key) -> Option<OtherNode> {
        let known_peers = self.known_peers.read().await;

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

impl std::fmt::Debug for OtherNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OtherNode")
            .field("addr", &self.addr)
            .finish()
    }
}
