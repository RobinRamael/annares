use std::net::{IpAddr, Ipv6Addr, SocketAddr};
use structopt::StructOpt;

mod utils;

mod peering;
use peering::peering_node_client::PeeringNodeClient;
use peering::peering_node_server::{PeeringNode, PeeringNodeServer};
use peering::{IntroductionReply, IntroductionRequest, ListPeersReply, ListPeersRequest};

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use tonic::transport::Server;
use tonic::{Request, Response, Status};

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

        // format peer socket addrs into strings
        let peer_addrs = format_addrs(&locked_peers);

        locked_peers.insert(new_peer_addr.parse().unwrap());

        utils::print_peers(&locked_peers);

        Ok(Response::new(IntroductionReply {
            known_peers: peer_addrs,
        }))
    }
    async fn list_peers(
        &self,
        _: Request<ListPeersRequest>,
    ) -> Result<Response<ListPeersReply>, Status> {
        Ok(Response::new(ListPeersReply {
            known_peers: format_addrs(&self.known_peers.lock().unwrap()),
        }))
    }
}

fn format_addrs(addrs: &HashSet<SocketAddr>) -> Vec<String> {
    addrs.iter().map(|addr| format!("{}", addr)).collect()
}

impl MyPeeringNode {
    fn new(port: u16, known_peers: HashSet<SocketAddr>) -> MyPeeringNode {
        MyPeeringNode {
            addr: ipv6_loopback_socketaddr(port),
            known_peers: Arc::new(Mutex::new(known_peers)),
        }
    }

    async fn mingle(&self) -> Result<(), tonic::transport::Error> {
        let mut locked_peers = self.known_peers.lock().unwrap();

        let mut all_new_peers: HashSet<SocketAddr> = HashSet::new();

        for known_peer in locked_peers.iter() {
            println!("Sending introduction to {:?}", known_peer);
            let new_peers = self.send_introduction(known_peer).await?;

            for new_peer in &new_peers {
                self.send_introduction(&new_peer).await?;
            }

            println!("received peers {:?}", &new_peers);

            all_new_peers.extend(new_peers);
        }

        locked_peers.extend(all_new_peers.iter());

        utils::print_peers(&locked_peers);

        Ok(())
    }

    async fn send_introduction(
        &self,
        target_addr: &SocketAddr,
    ) -> Result<HashSet<SocketAddr>, tonic::transport::Error> {
        let mut client = PeeringNodeClient::connect(utils::build_grpc_url(target_addr)).await?;

        let request = tonic::Request::new(IntroductionRequest {
            sender_adress: self.addr.to_string(),
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
        HashSet::from_iter(vec![args.bootstrap_peer.unwrap()])
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
