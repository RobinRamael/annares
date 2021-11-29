use std::net::SocketAddr;
use structopt::StructOpt;

mod utils;

mod grpc;
use grpc::peering_node_client::PeeringNodeClient;
use grpc::peering_node_server::{PeeringNode, PeeringNodeServer};
use grpc::{
    GetKeyReply, GetKeyRequest, IntroductionReply, IntroductionRequest, ListPeersReply,
    ListPeersRequest, StoreReply, StoreRequest,
};

mod hash;
use hash::Hash;

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, RwLock};

use tonic::transport::Server;
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

impl KnownPeer {
    fn new(addr: SocketAddr) -> Self {
        KnownPeer {
            addr,
            hash: Hash::hash(format_addr(&addr)),
        }
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

        let mut locked_peers = self.known_peers.lock().unwrap();

        // format peer socket addrs into strings
        // let peer_addrs = format_addrs(&locked_peers);

        locked_peers.insert(KnownPeer::new(new_peer_addr.parse().unwrap()));

        // utils::print_peers(&locked_peers);

        let known_peers = locked_peers
            .iter()
            .map(|p| grpc::Peer::from(p.clone()))
            .collect();

        println!("responding with my known peers");

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

fn format_addr(addr: &SocketAddr) -> String {
    format!("{}", addr)
}

impl From<KnownPeer> for grpc::Peer {
    fn from(peering_node: KnownPeer) -> Self {
        grpc::Peer {
            addr: format_addr(&peering_node.addr),
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
    fn new(port: u16, known_peers: HashSet<KnownPeer>) -> MyPeeringNode {
        let addr = utils::ipv6_loopback_socketaddr(port);
        MyPeeringNode {
            addr,
            hash: Hash::hash(format_addr(&addr)),
            known_peers: Arc::new(Mutex::new(known_peers)),
            data_shard: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    async fn mingle(&self) -> Result<(), tonic::transport::Error> {
        let mut locked_peers = self.known_peers.lock().unwrap();

        let mut all_new_peers: HashSet<KnownPeer> = HashSet::new();

        for known_peer in locked_peers.iter() {
            println!("Sending introduction to {:?}", known_peer);

            let new_peers = self.send_introduction(&known_peer.addr).await?;

            println!("Received new peers {:?}", &new_peers);

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

#[derive(StructOpt, Debug)]
struct Cli {
    port: u16,

    #[structopt(short = "p", long = "peer")]
    pub bootstrap_peer: Option<SocketAddr>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Cli::from_args();

    let peers = if args.bootstrap_peer.is_some() {
        HashSet::from_iter(vec![KnownPeer::new(args.bootstrap_peer.unwrap())])
    } else {
        HashSet::new()
    };

    println!("Running on port {:?}", args.port);
    println!("bootstrapping using {:?}", args.bootstrap_peer);
    println!("");
    let my_peering_node = MyPeeringNode::new(args.port, peers);

    my_peering_node.mingle().await?;

    println!("awaiting further introductions...");

    Server::builder()
        .add_service(PeeringNodeServer::new(my_peering_node))
        .serve(utils::ipv6_loopback_socketaddr(args.port))
        .await?;

    Ok(())
}
