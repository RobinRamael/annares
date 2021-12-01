#![allow(dead_code)]

use crate::peering::grpc;
use crate::peering::grpc::peering_node_client::PeeringNodeClient;
use crate::peering::grpc::peering_node_server::PeeringNode;
use crate::peering::grpc::{
    GetKeyReply, GetKeyRequest, HealthCheckReply, HealthCheckRequest, IntroductionReply,
    IntroductionRequest, ListPeersReply, ListPeersRequest, StoreReply, StoreRequest,
};
use crate::peering::hash::Hash;
use crate::peering::utils;
use std::iter;

use std::collections::HashMap;
use std::fmt::{Debug, Display};
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use tonic::{Request, Response, Status};

#[tonic::async_trait]
trait GetStore {
    async fn get_key(&self, key: &Hash) -> Result<String, Status>;
    async fn store(&self, key: &Hash, value: String, hops: u32) -> Result<SocketAddr, Status>;

    fn distance_to(&self, key: &Hash) -> Hash;
}

#[derive(Debug)]
pub struct MyPeeringNode {
    pub data: Arc<NodeData>,
}

#[derive(Debug)]
pub struct NodeData {
    pub addr: SocketAddr,
    pub hash: Hash,
    pub known_peers: Arc<RwLock<HashMap<SocketAddr, KnownPeer>>>,
    pub data_shard: Arc<RwLock<HashMap<Hash, String>>>,
}

#[derive(PartialEq, Eq, std::hash::Hash, Clone)]
pub struct KnownPeer {
    pub addr: SocketAddr,
    pub hash: Hash,
    pub last_contact: Instant,
    pub contacts: u64,
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
        }
    }

    fn purge(&self, peer: &KnownPeer) {
        println!("Purging {}", peer);
        let mut peers = self.known_peers.write().unwrap();
        assert!(peers.contains_key(&peer.addr));
        peers.remove(&peer.addr);
    }

    fn mark_healthy(&self, peer: &KnownPeer) {
        let mut locked_peers = self.known_peers.write().unwrap();
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

                if let Some(peer) = data.stalest_peer() {
                    match PeeringNodeClient::connect(utils::build_grpc_url(&peer.addr))
                        .await
                        .as_mut()
                    {
                        Ok(client) => {
                            match client
                                .check_health(Request::new(HealthCheckRequest {}))
                                .await
                            {
                                Ok(_) => data.mark_healthy(&peer),
                                Err(err) => {
                                    println!("Request failed: {}", err);
                                    data.purge(&peer);
                                }
                            }
                        }
                        Err(err) => {
                            println!("Connection failed: {}", err);
                            data.purge(&peer);
                        }
                    };
                }
            }
        });
        forever
    }

    fn stalest_peer(&self) -> Option<KnownPeer> {
        let peers = self.known_peers.read();

        peers
            .unwrap()
            .values()
            .cloned()
            .max_by_key(|p| Instant::now() - p.last_contact)
    }

    fn store_here(&self, key: &Hash, value: String) {
        let mut data_shard = self.data_shard.write().unwrap();
        data_shard.insert(key.clone(), value);
    }

    fn get_here(&self, key: &Hash) -> Option<String> {
        let data_shard = self.data_shard.read().unwrap();

        data_shard.get(key).cloned()
    }
}

#[tonic::async_trait]
impl GetStore for NodeData {
    async fn get_key(&self, key: &Hash) -> Result<String, Status> {
        let data_shard = self.data_shard.read().unwrap();

        match data_shard.get(key).cloned() {
            Some(value) => Ok(value),
            None => Err(Status::new(
                tonic::Code::NotFound,
                format!("key {} not found", key.as_hex_string()),
            )),
        }
    }

    async fn store(&self, key: &Hash, value: String, _hops: u32) -> Result<SocketAddr, Status> {
        let mut data_shard = self.data_shard.write().unwrap();
        data_shard.insert(key.clone(), value);
        Ok(self.addr)
    }

    fn distance_to(&self, key: &Hash) -> Hash {
        self.hash.cyclic_distance(key)
    }
}

impl KnownPeer {
    pub fn new(addr: SocketAddr) -> Self {
        KnownPeer {
            addr,
            hash: Hash::hash(&addr.to_string()),
            last_contact: Instant::now(),
            contacts: 0,
        }
    }

    async fn connect(&self) -> Result<PeeringNodeClient<tonic::transport::Channel>, Status> {
        let target_url = utils::build_grpc_url(&self.addr);

        match PeeringNodeClient::connect(target_url).await {
            Ok(client) => Ok(client),
            Err(_) => Err(Status::new(
                tonic::Code::Internal,
                "Failed to connect to delegated node",
            )),
        }
    }
}
#[tonic::async_trait]
impl GetStore for KnownPeer {
    async fn get_key(&self, key: &Hash) -> Result<String, Status> {
        println!("delegating get to {}", &self.addr);

        let mut client = self.connect().await?;
        let request = Request::new(GetKeyRequest {
            key: key.as_hex_string(),
        });

        match client.get_key(request).await {
            Ok(response) => {
                let GetKeyReply { value, .. } = response.into_inner();
                Ok(value)
            }
            Err(err) => Err(err),
        }
    }

    async fn store(&self, _key: &Hash, value: String, hops: u32) -> Result<SocketAddr, Status> {
        println!("delegating store to {}", &self.addr);
        let mut client = self.connect().await?;

        let request = Request::new(StoreRequest {
            value,
            hops: hops + 1,
        });

        match client.store(request).await {
            Ok(response) => {
                let StoreReply { stored_in, .. } = response.into_inner();
                Ok(stored_in.parse().unwrap())
            }
            Err(err) => Err(err),
        }
    }

    fn distance_to(&self, key: &Hash) -> Hash {
        self.hash.cyclic_distance(key)
    }
}

impl MyPeeringNode {
    pub fn new(data: Arc<NodeData>) -> MyPeeringNode {
        MyPeeringNode { data }
    }

    pub async fn mingle(&self) -> Result<(), tonic::transport::Error> {
        let mut locked_peers = self.data.known_peers.write().unwrap();

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

        println!("sending intro to {}", target_addr);

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

    async fn delegate_store(
        &self,
        peer: &KnownPeer,
        value: String,
        hops: u32,
    ) -> Result<(SocketAddr, String), Status> {
        let target_url = utils::build_grpc_url(&peer.addr);

        println!("delegating store to {}", &peer.addr);

        if let Ok(client) = PeeringNodeClient::connect(target_url).await.as_mut() {
            let request = Request::new(StoreRequest {
                value,
                hops: hops + 1,
            });

            match client.store(request).await {
                Ok(response) => {
                    let StoreReply { key, stored_in, .. } = response.into_inner();
                    Ok((stored_in.parse().unwrap(), key))
                }
                Err(err) => Err(err),
            }
        } else {
            return Err(Status::new(
                tonic::Code::Internal,
                "Failed to connect to delegated node",
            ));
        }
    }

    async fn delegate_get(&self, peer: &KnownPeer, key: &String) -> Result<String, Status> {
        let target_url = utils::build_grpc_url(&peer.addr);

        println!("delegating get to {}", &peer.addr);

        if let Ok(client) = PeeringNodeClient::connect(target_url).await.as_mut() {
            let request = Request::new(GetKeyRequest { key: key.clone() });

            match client.get_key(request).await {
                Ok(response) => {
                    let GetKeyReply { value, .. } = response.into_inner();
                    Ok(value)
                }
                Err(err) => Err(err),
            }
        } else {
            return Err(Status::new(
                tonic::Code::Internal,
                "Failed to connect to delegated node",
            ));
        }
    }

    fn get_nearest(&self, hash: &Hash) -> Arc<dyn GetStore + Send + Sync> {
        let this_node = self.data.clone();
        let peers = self.data.known_peers.read();

        peers
            .unwrap()
            .values()
            .cloned()
            .map(|p| Arc::new(p) as Arc<dyn GetStore + Send + Sync>)
            .chain(iter::once(this_node as Arc<dyn GetStore + Send + Sync>))
            .min_by_key(|peer| peer.distance_to(hash))
            .unwrap() // we can unwrap here because we're sure the iterator has at least self in it
    }
}

#[tonic::async_trait]
impl PeeringNode for MyPeeringNode {
    async fn introduce(
        &self,
        request: Request<IntroductionRequest>,
    ) -> Result<Response<IntroductionReply>, Status> {
        let new_peer_addr = request.into_inner().sender_adress.parse().unwrap();

        println!("received introduction from {:?}", new_peer_addr);

        let mut locked_peers = self.data.known_peers.write().unwrap();

        let known_peers = locked_peers
            .values()
            .cloned()
            .map(grpc::Peer::from)
            .collect();

        locked_peers.insert(new_peer_addr, KnownPeer::new(new_peer_addr));

        print_peers(&locked_peers);

        Ok(Response::new(IntroductionReply { known_peers }))
    }

    async fn list_peers(
        &self,
        _: Request<ListPeersRequest>,
    ) -> Result<Response<ListPeersReply>, Status> {
        let locked_peers = self.data.known_peers.read().unwrap().clone();

        let peers: Vec<_> = locked_peers
            .values()
            .cloned()
            .chain(iter::once(KnownPeer::new(self.data.addr)))
            .map(grpc::Peer::from)
            .collect();

        Ok(Response::new(ListPeersReply { known_peers: peers }))
    }

    async fn get_key(
        &self,
        request: Request<GetKeyRequest>,
    ) -> Result<Response<GetKeyReply>, Status> {
        let GetKeyRequest { key, .. } = request.into_inner();

        let hash: Hash = Hash::try_from(key).unwrap();

        let value = self.get_nearest(&hash).get_key(&hash).await?;

        Ok(Response::new(GetKeyReply { value }))
    }

    async fn store(&self, request: Request<StoreRequest>) -> Result<Response<StoreReply>, Status> {
        let StoreRequest { value, hops, .. } = request.into_inner();

        println!("received store request for {}", &value);

        let hash = Hash::hash(&value);

        let addr = self.get_nearest(&hash).store(&hash, value, hops).await?;

        Ok(Response::new(StoreReply {
            key: hash.as_hex_string(),
            stored_in: addr.to_string(),
        }))
    }

    async fn check_health(
        &self,
        _: Request<HealthCheckRequest>,
    ) -> Result<Response<HealthCheckReply>, Status> {
        Ok(Response::new(HealthCheckReply {}))
    }
}

impl Display for KnownPeer {
    fn fmt(&self, fmtr: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(fmtr, "{}", self.addr)
    }
}

impl From<KnownPeer> for grpc::Peer {
    fn from(peering_node: KnownPeer) -> Self {
        grpc::Peer {
            addr: peering_node.addr.to_string(),
            hash: peering_node.hash.as_hex_string(),
        }
    }
}

impl TryFrom<grpc::Peer> for KnownPeer {
    type Error = std::net::AddrParseError;

    fn try_from(grpc_peer: grpc::Peer) -> Result<Self, Self::Error> {
        let addr = grpc_peer.addr.parse::<SocketAddr>()?;

        Ok(KnownPeer::new(addr))
    }
}

impl Debug for KnownPeer {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("KnownPeer")
            .field("addr", &self.addr)
            .field("contacts", &self.contacts)
            .finish()
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
