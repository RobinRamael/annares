use crate::peering::grpc;
use crate::peering::grpc::peering_node_client::PeeringNodeClient;
use crate::peering::grpc::peering_node_server::PeeringNode;
use crate::peering::grpc::{
    GetKeyReply, GetKeyRequest, IntroductionReply, IntroductionRequest, ListPeersReply,
    ListPeersRequest, StoreReply, StoreRequest,
};
use crate::peering::hash::Hash;
use crate::peering::utils;

use std::collections::{HashMap, HashSet};
use std::fmt::Display;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, RwLock};
use tonic::{Request, Response, Status};

#[derive(Debug, PartialEq, Eq, std::hash::Hash, Clone)]
pub struct KnownPeer {
    pub addr: SocketAddr,
    pub hash: Hash,
}

#[derive(Debug)]
pub struct MyPeeringNode {
    pub addr: SocketAddr,
    pub hash: Hash,
    pub known_peers: Arc<Mutex<HashSet<KnownPeer>>>,
    pub data_shard: Arc<RwLock<HashMap<Hash, String>>>,
}

#[tonic::async_trait]
impl PeeringNode for MyPeeringNode {
    async fn introduce(
        &self,
        request: Request<IntroductionRequest>,
    ) -> Result<Response<IntroductionReply>, Status> {
        let new_peer_addr = request.into_inner().sender_adress;

        println!("received introduction from {:?}", new_peer_addr);

        let mut locked_peers = self.known_peers.lock().unwrap();

        locked_peers.insert(KnownPeer::new(new_peer_addr.parse().unwrap()));

        print_peers(&locked_peers);

        let known_peers = locked_peers
            .iter()
            .map(|p| grpc::Peer::from(p.clone()))
            .collect();

        Ok(Response::new(IntroductionReply { known_peers }))
    }

    async fn list_peers(
        &self,
        _: Request<ListPeersRequest>,
    ) -> Result<Response<ListPeersReply>, Status> {
        let locked_peers = self.known_peers.lock().unwrap();

        let ps = locked_peers.iter().map(|p| p.clone().into()).collect();

        Ok(Response::new(ListPeersReply { known_peers: ps }))
    }

    async fn get_key(
        &self,
        _request: Request<GetKeyRequest>,
    ) -> Result<Response<GetKeyReply>, Status> {
        unimplemented!();
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

pub enum PeerFromGRPCError {
    Hex(hex::FromHexError),
    Addr(std::net::AddrParseError),
}

impl TryFrom<grpc::Peer> for KnownPeer {
    type Error = PeerFromGRPCError;

    fn try_from(grpc_peer: grpc::Peer) -> Result<Self, Self::Error> {
        let addr = match grpc_peer.addr.parse::<SocketAddr>() {
            Err(e) => return Err(PeerFromGRPCError::Addr(e)),
            Ok(a) => a,
        };

        let hash = match grpc_peer.hash.try_into() {
            Err(e) => return Err(PeerFromGRPCError::Hex(e)),
            Ok(h) => h,
        };

        Ok(KnownPeer { addr, hash })
    }
}

impl MyPeeringNode {
    pub fn new(port: u16, known_peers: HashSet<KnownPeer>) -> MyPeeringNode {
        let addr = utils::ipv6_loopback_socketaddr(port);
        MyPeeringNode {
            addr,
            hash: Hash::hash(addr.to_string()),
            known_peers: Arc::new(Mutex::new(known_peers)),
            data_shard: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn mingle(&self) -> Result<(), tonic::transport::Error> {
        let mut locked_peers = self.known_peers.lock().unwrap();

        let mut all_new_peers: HashSet<KnownPeer> = HashSet::new();

        for known_peer in locked_peers.iter() {
            println!("Sending introduction to {:?}", known_peer);

            let new_peers = self.send_introduction(&known_peer.addr).await?;

            print_peers(&new_peers);

            for new_peer in new_peers.iter().filter(|p| p.addr != self.addr) {
                self.send_introduction(&new_peer.addr).await?;
            }

            all_new_peers.extend(new_peers.clone());

            println!("received peers {:?}", &new_peers);
        }
        locked_peers.extend(all_new_peers);

        Ok(())
    }

    async fn send_introduction(
        &self,
        target_addr: &SocketAddr,
    ) -> Result<HashSet<KnownPeer>, tonic::transport::Error> {
        let target_url = utils::build_grpc_url(target_addr);
        dbg!(&target_url);

        let mut client = PeeringNodeClient::connect(target_url).await?;

        let request = tonic::Request::new(IntroductionRequest {
            sender_adress: self.addr.to_string(),
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
