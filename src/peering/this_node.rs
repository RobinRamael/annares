use futures::future;
use futures::stream::FuturesUnordered;
use rand::seq::IteratorRandom;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;
use tokio::task::JoinError;

use crate::peering::client::Client;
use crate::peering::errors::*;

use tracing::{error, info};

type Key = crate::peering::hash::Hash;

const REDUNDANCY: usize = 1;

#[derive(Debug, Clone)]
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
    pub fn new(addr: SocketAddr, bootstrap_addrs: Vec<SocketAddr>) -> Self {
        let peer_map = HashMap::from_iter(
            bootstrap_addrs
                .into_iter()
                .map(|addr| (addr, OtherNode::new(addr, Instant::now()))),
        );

        ThisNode {
            addr,
            hash: Key::hash(&addr.to_string()),
            peers: Peers {
                known_peers: Arc::new(RwLock::new(peer_map)),
            },
            primary_store: Arc::new(RwLock::new(HashMap::new())),
            secondary_store: Arc::new(RwLock::new(HashMap::new())),
            secondants: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn introduction_rcvd(
        self: &Arc<Self>,
        sender_addr: SocketAddr,
    ) -> Result<(Vec<OtherNode>, Vec<String>), IntroductionError> {
        todo!();
    }

    pub async fn store_rcvd(
        self: &Arc<Self>,
        value: String,
    ) -> Result<(Key, SocketAddr), StoreError> {
        let key = Key::hash(&value);

        let nearest = self.peers.nearest_to(&key).await.and_then(|peer| {
            if peer.distance_to(&key) < self.hash.cyclic_distance(&key) {
                Some(peer)
            } else {
                None
            }
        });

        match nearest {
            Some(peer) => self.delegate_store(peer, value).await,
            None => {
                self.store_here(&key, value).await;
                Ok((key, self.addr))
            }
        }
    }

    async fn delegate_store(
        self: &Arc<Self>,
        peer: OtherNode,
        value: String,
    ) -> Result<(Key, SocketAddr), StoreError> {
        todo!()
    }

    async fn store_here(self: &Arc<Self>, key: &Key, value: String) -> () {
        let mut primary_store = self.primary_store.write().await;
        primary_store.insert(key.clone(), value.clone());

        let self_clone = Arc::clone(self);
        let value_clone = value.clone();
        tokio::task::spawn(async move {
            self_clone.store_in_secondants(value_clone).await;
        });
    }

    async fn store_in_secondants(self: &Arc<Self>, value: String) {
        let tasks: Vec<_> = self
            .peers
            .pick(REDUNDANCY)
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

        for t in tasks {
            match t.await {
                Ok(res) => match res {
                    Ok((peer, key)) => self.store_as_secondant(peer, key).await,
                    Err(_) => {
                        error!("client error when assigning secondant");
                        // TODO: restructure this fn so we can retry another node here
                    }
                },
                Err(_) => {
                    error!("join error!?");
                }
            }
        }
    }

    pub async fn store_as_secondant(&self, peer: OtherNode, key: Key) {
        let mut secondants = self.secondants.write().await;
        secondants.insert(key, peer);
    }

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

    async fn delegate_get(
        &self,
        peer: OtherNode,
        key: Key,
    ) -> Result<(String, SocketAddr), GetError> {
        match Client::get(&peer.addr, &key).await {
            Ok(res) => Ok(res),
            Err(ClientError::ConnectionFailed(_)) => {
                self.peers.mark_dead(peer).await;
                // TODO: is there more we can try here? wait for the secondary to notice
                // this death or try to find the secondary ourselves?
                Err(GetError::Common(CommonError::Unavailable))
            }
            Err(ClientError::MalformedResponse) => {
                Err(GetError::Common(CommonError::Internal(InternalError {
                    message: "got malformed response from client".to_string(),
                })))
            }
            Err(ClientError::Status(err)) => Err(GetError::Common(CommonError::Status(err))),
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

    pub async fn mark_alive(&self, peer: OtherNode) {
        let mut known_peers = self.known_peers.write().await;

        let mut peer = known_peers
            .get_mut(&peer.addr)
            .expect("This really should be here...");

        peer.last_seen = Instant::now();
    }
    pub async fn mark_dead(&self, peer: OtherNode) {
        let mut known_peers = self.known_peers.write().await;

        if let Some((addr, peer)) = known_peers.remove_entry(&peer.addr) {
            // TODO!
        } // else someone got to it first?
    }

    pub async fn nearest_to(&self, key: &Key) -> Option<OtherNode> {
        let known_peers = self.known_peers.read().await;

        known_peers
            .values()
            .cloned()
            .min_by_key(|peer| peer.distance_to(key))
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
}

impl std::hash::Hash for OtherNode {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.addr.hash(state);
    }
}
