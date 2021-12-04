use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;

use crate::peering::errors::*;

type Key = crate::peering::hash::Hash;

#[derive(Debug)]
pub struct OtherNode {
    pub addr: SocketAddr,
    pub last_seen: Instant,
}

impl OtherNode {
    fn new(addr: SocketAddr, last_seen: Instant) -> Self {
        Self { addr, last_seen }
    }
    pub async fn stalest_peer() -> Option<OtherNode> {
        todo!()
    }
}

#[derive(Debug)]
pub struct ThisNode {
    pub addr: SocketAddr,
    hash: Key,
    pub known_peers: Arc<RwLock<HashMap<SocketAddr, OtherNode>>>,
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
            known_peers: Arc::new(RwLock::new(peer_map)),
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
        todo!();
    }

    pub async fn secondary_store_rcvd(self: &Arc<Self>, value: String) -> Result<(), StoreError> {
        todo!();
    }

    pub async fn get_rcvd(self: &Arc<Self>, key: Key) -> Result<String, GetError> {
        todo!();
    }

    pub async fn stalest_peer(self: &Arc<Self>) -> Option<OtherNode> {
        todo!()
    }

    pub async fn mark_alive(self: &Arc<Self>, peer: OtherNode) {}
    pub async fn mark_dead(self: &Arc<Self>, peer: OtherNode) {}
}
