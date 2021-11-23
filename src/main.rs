use std::net::{IpAddr, Ipv6Addr, SocketAddr};
use structopt::StructOpt;

use peering::peering_node_client::PeeringNodeClient;
use peering::peering_node_server::{PeeringNode, PeeringNodeServer};
use peering::{IntroductionReply, IntroductionRequest};
use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use tonic::transport::Server;
use tonic::{Request, Response, Status};

pub mod peering {
    tonic::include_proto!("peering");
}

#[derive(Debug)]
pub struct MyPeeringNode {
    pub addr: SocketAddr,
    pub known_peers: Arc<Mutex<HashSet<SocketAddr>>>,
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
        println!("I currently know {:?}", locked_peers);

        // format peer socket addrs into strings
        let peer_addrs = locked_peers
            .iter()
            .map(|addr| format!("{}", addr))
            .collect();

        locked_peers.insert(new_peer_addr.parse().unwrap());

        println!("known peers: {:?}", locked_peers);

        Ok(Response::new(IntroductionReply {
            known_peers: peer_addrs,
        }))
    }
}

fn build_grpc_url(addr: SocketAddr) -> String {
    format!("http://{}", addr)
}

impl MyPeeringNode {
    fn new(port: u16, known_peers: HashSet<SocketAddr>) -> MyPeeringNode {
        MyPeeringNode {
            addr: ipv6_loopback_socketaddr(port),
            known_peers: Arc::new(Mutex::new(known_peers)),
        }
    }
}

async fn send_introduction(
    source_addr: SocketAddr,
    target_addr: SocketAddr,
) -> Result<HashSet<SocketAddr>, tonic::transport::Error> {
    let mut client = PeeringNodeClient::connect(build_grpc_url(target_addr)).await?;

    let request = tonic::Request::new(IntroductionRequest {
        sender_adress: source_addr.to_string(),
    });

    let response = client.introduce(request).await.unwrap();

    let new_peers = response
        .get_ref()
        .known_peers
        .iter()
        .filter_map(|addr_string| addr_string.parse::<SocketAddr>().ok())
        .collect();

    Ok(new_peers)
}

async fn mingle(
    source_addr: SocketAddr,
    bootstrap_addr: SocketAddr,
    known_peers: HashSet<SocketAddr>,
) -> Result<HashSet<SocketAddr>, tonic::transport::Error> {
    // let locked_peers = known_peers.lock().unwrap();

    println!("mingling...");
    dbg!(bootstrap_addr, known_peers);
    let new_peers: HashSet<SocketAddr> = send_introduction(source_addr, bootstrap_addr)
        .await?
        .into_iter()
        .map(|peer| {
            tokio::task::spawn(async move {
                println!("sending intro to {:?}", peer);
                send_introduction(source_addr, peer).await.unwrap();
            });
            peer
        })
        .collect();

    dbg!(&new_peers);

    Ok(new_peers)
}

fn ipv6_loopback_socketaddr(port: u16) -> SocketAddr {
    SocketAddr::new(IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 0x1)), port)
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

    let peers;

    dbg!(&args);

    if args.bootstrap_peer.is_some() {
        let peer = args.bootstrap_peer.unwrap();
        peers = mingle(
            ipv6_loopback_socketaddr(args.port),
            peer,
            HashSet::from_iter(vec![peer]),
        )
        .await?;
    } else {
        peers = HashSet::new()
    }

    let my_peering_node = MyPeeringNode::new(args.port, peers);

    println!("awaiting further introductions...");

    Server::builder()
        .add_service(PeeringNodeServer::new(my_peering_node))
        .serve(ipv6_loopback_socketaddr(args.port))
        .await?;

    Ok(())
}
