#![allow(dead_code)]

use crate::peering::grpc;
use crate::peering::grpc::peering_node_client::PeeringNodeClient;
use crate::peering::grpc::peering_node_server::PeeringNode;
use crate::peering::grpc::{
    GetKeyReply, GetKeyRequest, GetStatusReply, GetStatusRequest, HealthCheckReply,
    HealthCheckRequest, IntroductionReply, IntroductionRequest, KeyValuePair, ListPeersReply,
    ListPeersRequest, ShutDownReply, ShutDownRequest, Stewardship, StoreReply, StoreRequest,
    TransferReply, TransferRequest,
};
use crate::peering::hash::Hash;
use crate::peering::peer::KnownPeer;
use crate::peering::utils;
use std::iter;

use rand::seq::IteratorRandom;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tonic::{Request, Response, Status};

use tracing::{debug, error, info, instrument};

const REDUNDANCY: usize = 1;

#[derive(Debug)]
pub struct NodeData {
    pub addr: SocketAddr,
    pub hash: Hash,
    pub known_peers: Arc<RwLock<HashMap<SocketAddr, KnownPeer>>>,
    pub data_shard: Arc<RwLock<HashMap<Hash, String>>>,
    pub secondary_stewardships: Arc<RwLock<HashMap<KnownPeer, HashSet<Hash>>>>,
}

impl NodeData {
    pub fn new(port: u16, known_peers: Vec<KnownPeer>) -> NodeData {
        let addr = utils::ipv6_loopback_socketaddr(port);
        let peer_map = HashMap::from_iter(known_peers.into_iter().map(|p| (p.addr, p)));

        NodeData {
            addr,
            hash: Hash::hash(&addr.to_string()),
            known_peers: Arc::new(RwLock::new(peer_map)),
            data_shard: Arc::new(RwLock::new(HashMap::new())),
            secondary_stewardships: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    async fn purge(&self, peer: &KnownPeer) {
        // println!("Purging {}", peer);
        info!("purging {}", peer = &peer);
        let mut peers = self.known_peers.write().await;
        assert!(peers.contains_key(&peer.addr));
        peers.remove(&peer.addr);
    }

    async fn mark_healthy(&self, peer: &KnownPeer) {
        let mut locked_peers = self.known_peers.write().await;
        let healthy_peer = locked_peers.get_mut(&peer.addr).unwrap();

        healthy_peer.last_contact = Instant::now();
        healthy_peer.contacts += 1;
    }

    pub async fn peer_check_loop(
        self: &Arc<Self>,
        interval: Duration,
    ) -> tokio::task::JoinHandle<()> {
        let data = Arc::clone(self);

        let forever = tokio::task::spawn(async move {
            let mut interval = tokio::time::interval(interval);

            loop {
                interval.tick().await;

                if let Some(peer) = data.stalest_peer().await {
                    match PeeringNodeClient::connect(utils::build_grpc_url(&peer.addr))
                        .await
                        .as_mut()
                    {
                        Ok(client) => {
                            match client
                                .check_health(Request::new(HealthCheckRequest {}))
                                .await
                            {
                                Ok(_) => data.mark_healthy(&peer).await,
                                Err(err) => {
                                    error!("Request failed: {}", err = err);
                                    data.purge(&peer).await;
                                    self.clone().reassign_secondary_stewardships(&peer).await;
                                }
                            }
                        }
                        Err(err) => {
                            error!("connection failed: {}", err = err);
                            data.purge(&peer).await;
                            self.clone().reassign_secondary_stewardships(&peer).await;
                        }
                    };
                }
            }
        });
        forever
    }

    async fn stewardship_reassignment(self: &Arc<Self>, peer: &KnownPeer) {
        // let self_clone = self.clone();
        let to_transfer = self.reasses_stewardship_for(&peer).await;
        let transferred_keys = peer.transfer_stewardship(to_transfer).await.unwrap();

        for key in &transferred_keys {
            info!(key=?key.as_hex_string(), peer=?&peer, "Reassigning");
        }

        self.remove_keys(transferred_keys).await;
    }

    async fn reassign_secondary_stewardships(self: &Arc<Self>, peer: &KnownPeer) {
        let secondary_stewardships = self.secondary_stewardships.read().await;

        if let Some(keys) = secondary_stewardships.get(peer) {
            for key in keys {
                let value = self
                    .get_key(key)
                    .await
                    .expect("this key should be here...")
                    .clone();

                self.clone()
                    .assign_secondary_stewardships(&key, &value)
                    .await;
            }
        }
    }

    async fn assign_secondary_stewardships(self: Arc<Self>, key: &Hash, value: &String) {
        let peers = self.known_peers.read().await;
        let secondary_stewards;
        {
            let mut rng = rand::rngs::ThreadRng::default();
            secondary_stewards = peers
                .values()
                .cloned()
                // FIXME: this min here means that when the network is smaller than REDUNDANCY,
                // the number of stewards for this value will stay at the same size
                .choose_multiple(&mut rng, std::cmp::min(REDUNDANCY, peers.len()));
        }

        for steward in secondary_stewards {
            info!(
                "assigning secondary stewardship: {}, {}",
                peer = steward,
                value = value
            );
            steward
                .assign_secondary_stewardship(key, value)
                .await
                .expect("assign failed :(");
            self.store_secondary_stewardship(steward, key).await;
        }
    }

    async fn store_secondary_stewardship(&self, steward: KnownPeer, key: &Hash) {
        let mut secondary_stewardships = self.secondary_stewardships.write().await;
        if !secondary_stewardships.contains_key(&steward) {
            let key_set = HashSet::from_iter(iter::once(key.clone()));
            secondary_stewardships.insert(steward, key_set);
        } else {
            secondary_stewardships
                .get_mut(&steward)
                .unwrap()
                .insert(key.clone());
        }
    }

    async fn stalest_peer(&self) -> Option<KnownPeer> {
        let peers = self.known_peers.read();

        peers
            .await
            .values()
            .cloned()
            .max_by_key(|p| Instant::now() - p.last_contact)
    }

    async fn reasses_stewardship_for(&self, peer: &KnownPeer) -> Vec<(Hash, String)> {
        let data_shard = self.data_shard.read().await.clone();

        data_shard
            .into_iter()
            .filter(|(key, _val)| peer.distance_to(key) < self.distance_to(key))
            .collect()
    }

    async fn remove_keys(&self, keys: Vec<Hash>) {
        let mut data_shard = self.data_shard.write().await;

        for key in keys {
            // println!("deleting {}", &key);
            data_shard.remove(&key);
        }
    }

    async fn get_key(&self, key: &Hash) -> Result<String, Status> {
        let data_shard = self.data_shard.read().await;

        match data_shard.get(key).cloned() {
            Some(value) => Ok(value),
            None => Err(Status::new(
                tonic::Code::NotFound,
                format!("key {} not found", key.as_hex_string()),
            )),
        }
    }

    pub async fn store(&self, key: &Hash, value: &String) -> Result<SocketAddr, Status> {
        info!("storing {} here", value);
        let mut data_shard = self.data_shard.write().await;
        data_shard.insert(key.clone(), value.clone());
        Ok(self.addr)
    }

    pub fn distance_to(&self, key: &Hash) -> Hash {
        self.hash.cyclic_distance(key)
    }
}

pub struct Node {
    pub name: String,
    pub data: Arc<NodeData>,
}

impl Node {
    pub fn new(data: Arc<NodeData>, name: String) -> Node {
        Node { data, name }
    }

    pub async fn mingle(&self) -> Result<(), tonic::transport::Error> {
        let mut locked_peers = self.data.known_peers.write().await;

        let mut all_new_peers = Vec::new();

        for known_peer in locked_peers.values() {
            let new_peers = self.send_introduction(&known_peer.addr).await?;

            for new_peer in new_peers.iter().filter(|p| p.addr != self.data.addr) {
                self.send_introduction(&new_peer.addr).await?;
            }

            all_new_peers.extend(new_peers.clone());
        }

        for p in all_new_peers {
            locked_peers.insert(p.addr, p);
        }

        Ok(())
    }

    async fn send_introduction(
        &self,
        target_addr: &SocketAddr,
    ) -> Result<Vec<KnownPeer>, tonic::transport::Error> {
        let target_url = utils::build_grpc_url(target_addr);

        // println!("sending intro to {}", target_addr);

        let mut client = PeeringNodeClient::connect(target_url).await?;

        let request = Request::new(IntroductionRequest {
            sender_adress: self.data.addr.to_string(),
        });

        let response = client.introduce(request).await.unwrap();

        let new_peers = response
            .into_inner()
            .known_peers
            .into_iter()
            .filter_map(|peer| peer.try_into().ok())
            .collect();

        Ok(new_peers)
    }

    async fn get_nearest(&self, key: &Hash) -> Option<KnownPeer> {
        let peers = self.data.known_peers.read().await;

        let nearest_peer = peers
            .values()
            .cloned()
            .min_by_key(|peer| peer.distance_to(key));

        match nearest_peer {
            Some(peer) => {
                if self.data.distance_to(key) > peer.distance_to(key) {
                    Some(peer)
                } else {
                    None
                }
            }
            None => None,
        }
    }
}

#[tonic::async_trait]
impl PeeringNode for Node {
    #[instrument(skip(request), fields(from=?request.get_ref().sender_adress))]
    async fn introduce(
        &self,
        request: Request<IntroductionRequest>,
    ) -> Result<Response<IntroductionReply>, Status> {
        let new_peer_addr = request.into_inner().sender_adress.parse().unwrap();

        // println!("received introduction from {:?}", new_peer_addr);

        let mut locked_peers = self.data.known_peers.write().await;

        let known_peers = locked_peers
            .values()
            .cloned()
            .map(grpc::Peer::from)
            .collect();

        let new_peer = KnownPeer::new(new_peer_addr);
        let new_peer_clone = new_peer.clone();
        locked_peers.insert(new_peer_addr, new_peer);

        let data = self.data.clone();

        tokio::task::spawn(async move {
            // println!("spawned!");
            tokio::time::sleep(Duration::from_millis(100)).await;
            data.stewardship_reassignment(&new_peer_clone).await;
        });

        debug!(
            "peers are now {}",
            &locked_peers
                .values()
                .cloned()
                .map(|p| p.addr.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        );

        Ok(Response::new(IntroductionReply { known_peers }))
    }

    #[instrument(skip(request), fields(key=?utils::shorten(&request.get_ref().key)))]
    async fn get_key(
        &self,
        request: Request<GetKeyRequest>,
    ) -> Result<Response<GetKeyReply>, Status> {
        let GetKeyRequest { key, .. } = request.into_inner();

        let hash: Hash = Hash::try_from(key).unwrap();

        let value = match self.get_nearest(&hash).await {
            Some(nearest) => nearest.get_key(&hash).await?,
            None => self.data.get_key(&hash).await?,
        };

        Ok(Response::new(GetKeyReply { value }))
    }

    #[instrument(skip(request), fields(value=?request.get_ref().value))]
    async fn store(&self, request: Request<StoreRequest>) -> Result<Response<StoreReply>, Status> {
        let StoreRequest {
            value,
            as_secondary,
            ..
        } = request.into_inner();

        let key = Hash::hash(&value);
        let addr;

        if as_secondary {
            info!("secondary store rcvd for {}", value = value);
            addr = self.data.store(&key, &value).await?;
        } else {
            info!("primary store rcvd for {}", value = value);
            addr = match self.get_nearest(&key).await {
                Some(nearest) => {
                    info!("nearest for {} is {}", key, nearest);
                    nearest.store(&key, &value, false).await?
                }
                None => {
                    let my_addr = self.data.store(&key, &value).await?;

                    let key_clone = key.clone();
                    let value_clone = value.clone();
                    let self_clone = Arc::clone(&self.data);
                    tokio::spawn(async move {
                        self_clone
                            .assign_secondary_stewardships(&key_clone, &value_clone)
                            .await;
                    });
                    my_addr
                }
            };
        }
        Ok(Response::new(StoreReply {
            key: key.as_hex_string(),
            stored_in: addr.to_string(),
        }))
    }

    #[instrument(skip(_request), level = "debug")]
    async fn check_health(
        &self,
        _request: Request<HealthCheckRequest>,
    ) -> Result<Response<HealthCheckReply>, Status> {
        Ok(Response::new(HealthCheckReply {}))
    }

    #[instrument(skip(_request))]
    async fn shut_down(
        &self,
        _request: Request<ShutDownRequest>,
    ) -> Result<Response<ShutDownReply>, Status> {
        tokio::spawn(async {
            tokio::time::sleep(Duration::from_millis(50)).await;
            std::process::exit(0)
        });
        Ok(Response::new(ShutDownReply {}))
    }

    #[instrument(skip(request), fields(n=?request.get_ref().to_store.len()))]
    async fn transfer(
        &self,
        request: Request<TransferRequest>,
    ) -> Result<Response<TransferReply>, Status> {
        let TransferRequest { to_store, .. } = request.into_inner();

        let mut data_shard = self.data.data_shard.write().await;

        let transferred_keys: Vec<String> = to_store
            .into_iter()
            .map(|kv| {
                let KeyValuePair { key, value } = kv;
                (Hash::try_from(key).unwrap(), value)
            })
            .map(|(key, value)| {
                data_shard.insert(key.clone(), value);
                debug!("Reassigned {} to me!", &key);
                key
            })
            .map(|key| key.as_hex_string())
            .collect();

        Ok(Response::new(TransferReply { transferred_keys }))
    }

    #[instrument(skip(_request))]
    async fn list_peers(
        &self,
        _request: Request<ListPeersRequest>,
    ) -> Result<Response<ListPeersReply>, Status> {
        info!("received list peers");
        let locked_peers = self.data.known_peers.read().await.clone();

        let peers: Vec<_> = locked_peers
            .values()
            .cloned()
            .chain(iter::once(KnownPeer::new(self.data.addr)))
            .map(grpc::Peer::from)
            .collect();

        Ok(Response::new(ListPeersReply { known_peers: peers }))
    }

    #[instrument(skip(_request))]
    async fn get_status(
        &self,
        _request: Request<GetStatusRequest>,
    ) -> Result<Response<GetStatusReply>, Status> {
        debug!("received get data shard");
        let shard = self.data.data_shard.read().await;
        let sec_stewardships = self.data.secondary_stewardships.read().await;

        Ok(Response::new(GetStatusReply {
            shard: shard
                .clone()
                .into_iter()
                .map(|(key, value)| KeyValuePair {
                    key: key.as_hex_string(),
                    value,
                })
                .collect(),
            secondary_stewardships: sec_stewardships
                .clone()
                .into_iter()
                .map(|(peer, keys)| Stewardship {
                    addr: peer.addr.to_string(),
                    keys: keys.iter().cloned().map(|k| k.as_hex_string()).collect(),
                })
                .collect(),
        }))
    }
}

impl Debug for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        f.debug_struct("Node").field("name", &self.name).finish()
    }
}

pub fn print_peers(peers: &HashMap<SocketAddr, KnownPeer>) {
    if peers.is_empty() {
        println!("Known peers: None")
    } else {
        println!("KNOWN PEERS: ");
        for p in peers.values() {
            println!("   - {}", p);
        }
    }
}
