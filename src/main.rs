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

        let mut locked_peers = self.known_peers.lock().unwrap();

        // format peer socket addrs into strings
        let peer_addrs = locked_peers
            .iter()
            .map(|addr| format!("{}", addr))
            .collect();

        locked_peers.insert(new_peer_addr.parse().unwrap());

        Ok(Response::new(IntroductionReply {
            known_peers: peer_addrs,
        }))
    }
}

fn build_grpc_url(addr: SocketAddr) -> String {
    format!("http://{}", addr)
}

fn spawn_introduce_myself<'a>(node: &'static MyPeeringNode, peer: SocketAddr) {
    tokio::spawn(async move {
        node.introduce_myself(peer).await;
    });
}

impl MyPeeringNode {
    fn new(port: u16, known_peers: HashSet<SocketAddr>) -> MyPeeringNode {
        MyPeeringNode {
            addr: ipv6_loopback_socketaddr(port),
            known_peers: Arc::new(Mutex::new(known_peers)),
        }
    }

    async fn introduce_myself(
        &self,
        bootstrap_addr: SocketAddr,
    ) -> Result<(), tonic::transport::Error> {
        let mut client = PeeringNodeClient::connect(build_grpc_url(bootstrap_addr)).await?;

        let request = tonic::Request::new(IntroductionRequest {
            sender_adress: self.addr.to_string(),
        });

        let response = client.introduce(request).await.unwrap();

        let mut locked_peers = self.known_peers.lock().unwrap();

        response
            .get_ref()
            .known_peers
            .iter()
            .filter_map(|addr_string| addr_string.parse::<SocketAddr>().ok())
            .for_each(|new_peer| {
                if locked_peers.insert(new_peer) {
                    spawn_introduce_myself(self, new_peer);
                };
            });

        Ok(())
    }
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

    let my_peering_node;

    if args.bootstrap_peer.is_some() {
        my_peering_node = MyPeeringNode::new(
            args.port,
            HashSet::from_iter(vec![args.bootstrap_peer.unwrap()]),
        );

        my_peering_node
            .introduce_myself(args.bootstrap_peer.unwrap())
            .await?;
    } else {
        my_peering_node = MyPeeringNode::new(args.port, HashSet::new());
    }

    Server::builder()
        .add_service(PeeringNodeServer::new(my_peering_node))
        .serve(ipv6_loopback_socketaddr(args.port))
        .await?;

    Ok(())
}
