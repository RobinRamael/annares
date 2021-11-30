use crate::peering::grpc;
use crate::peering::grpc::peering_node_client::PeeringNodeClient;
use crate::peering::grpc::peering_node_server::PeeringNode;
use crate::peering::grpc::{
    GetKeyReply, GetKeyRequest, HealthCheckReply, HealthCheckRequest, IntroductionReply,
    IntroductionRequest, ListPeersReply, ListPeersRequest, StoreReply, StoreRequest,
};
use crate::peering::hash::Hash;
use crate::peering::utils;
use futures::TryFuture;

use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Display};
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use tonic::{Request, Response, Status};

#[derive(Debug)]
pub struct MyPeeringNode {
    pub data: Arc<NodeData>,
}

#[derive(Debug)]
pub struct NodeData {
    pub addr: SocketAddr,
    pub hash: Hash,
    pub known_peers: Arc<RwLock<HashSet<KnownPeer>>>,
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
    pub fn new(port: u16, known_peers: HashSet<KnownPeer>) -> NodeData {
        let addr = utils::ipv6_loopback_socketaddr(port);
        NodeData {
            addr,
            hash: Hash::hash(addr.to_string()),
            known_peers: Arc::new(RwLock::new(known_peers)),
            data_shard: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    fn purge(&self, peer: &KnownPeer) {
        println!("Purging {}", peer);
        let mut peers = self.known_peers.write().unwrap();
        assert!(peers.contains(peer));
        peers.remove(peer);
    }

    pub async fn peer_check_loop(
        self: &Arc<Self>,
        interval: Duration,
    ) -> tokio::task::JoinHandle<()> {
        let data = self.clone();
        {
            dbg!(&data.known_peers.read().unwrap());
        }

        let forever = tokio::task::spawn(async move {
            let mut interval = tokio::time::interval(interval);

            loop {
                interval.tick().await;

                if let Some(peer) = data.stalest_peer().as_mut() {
                    print!("Checking health of {}... ", &peer);

                    match PeeringNodeClient::connect(utils::build_grpc_url(&peer.addr))
                        .await
                        .as_mut()
                    {
                        Ok(client) => {
                            match client
                                .check_health(Request::new(HealthCheckRequest {}))
                                .await
                            {
                                Ok(_) => {
                                    println!("Healthy!");
                                }
                                Err(err) => {
                                    println!("Request failed: {}", err);
                                    data.purge(peer);
                                }
                            }
                        }
                        Err(err) => {
                            println!("Connection failed: {}", err);
                            data.purge(peer);
                        }
                    };
                }
            }
        });
        forever
    }

    fn stalest_peer(&self) -> Option<KnownPeer> {

        let peers = self.known_peers.read();

        dbg!(&peers);
        peers
            .unwrap()
            .clone()
            .into_iter()
            .max_by_key(|p| Instant::now() - p.last_contact)
    }
}

impl MyPeeringNode {
    pub fn new(data: Arc<NodeData>) -> MyPeeringNode {
        MyPeeringNode { data }
    }

    pub async fn mingle(&self) -> Result<(), tonic::transport::Error> {
        // let data = self.data.to_owned();
        let mut locked_peers = self.data.known_peers.write().unwrap();

        let mut all_new_peers: HashSet<KnownPeer> = HashSet::new();

        for known_peer in locked_peers.iter() {
            let new_peers = self.send_introduction(&known_peer.addr).await?;

            print_peers(&new_peers);

            for new_peer in new_peers.iter().filter(|p| p.addr != self.data.addr) {
                self.send_introduction(&new_peer.addr).await?;
            }

            all_new_peers.extend(new_peers.clone());
        }

        locked_peers.extend(all_new_peers);

        Ok(())
    }

    async fn send_introduction(
        &self,
        target_addr: &SocketAddr,
    ) -> Result<HashSet<KnownPeer>, tonic::transport::Error> {
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
}

#[tonic::async_trait]
impl PeeringNode for MyPeeringNode {
    async fn introduce(
        &self,
        request: Request<IntroductionRequest>,
    ) -> Result<Response<IntroductionReply>, Status> {
        let new_peer_addr = request.into_inner().sender_adress;

        println!("received introduction from {:?}", new_peer_addr);

        let mut locked_peers = self.data.known_peers.write().unwrap();

        let known_peers = locked_peers
            .iter()
            .map(|p| grpc::Peer::from(p.clone()))
            .collect();

        locked_peers.insert(KnownPeer::new(new_peer_addr.parse().unwrap()));

        print_peers(&locked_peers);

        Ok(Response::new(IntroductionReply { known_peers }))
    }

    async fn list_peers(
        &self,
        _: Request<ListPeersRequest>,
    ) -> Result<Response<ListPeersReply>, Status> {
        let mut peers = self.data.known_peers.read().unwrap().clone();

        // include this node as well
        peers.insert(KnownPeer::new(self.data.addr));

        let ps = peers.iter().map(|p| p.clone().into()).collect();

        Ok(Response::new(ListPeersReply { known_peers: ps }))
    }

    async fn get_key(
        &self,
        _request: Request<GetKeyRequest>,
    ) -> Result<Response<GetKeyReply>, Status> {
        unimplemented!();
    }

    async fn check_health(
        &self,
        _: Request<HealthCheckRequest>,
    ) -> Result<Response<HealthCheckReply>, Status> {
        Ok(Response::new(HealthCheckReply {}))
    }

    async fn store(&self, _request: Request<StoreRequest>) -> Result<Response<StoreReply>, Status> {
        todo!()
    }
}

impl KnownPeer {
    pub fn new(addr: SocketAddr) -> Self {
        KnownPeer {
            addr,
            hash: Hash::hash(addr.to_string()),
            last_contact: Instant::now(),
            contacts: 0,
        }
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

pub fn print_peers(peers: &HashSet<KnownPeer>) {
    if peers.is_empty() {
        println!("Known peers: None")
    } else {
        println!("KNOWN PEERS: ");
        for p in peers {
            println!("   - {}", p);
        }
    }
}
