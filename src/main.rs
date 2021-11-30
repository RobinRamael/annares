use std::net::SocketAddr;
use structopt::StructOpt;
use std::sync::Arc;

mod peering;

use peering::grpc::peering_node_server::PeeringNodeServer;

use peering::node::{print_peers, KnownPeer, MyPeeringNode, NodeData};
use peering::utils;

use std::collections::HashSet;
use tonic::transport::Server;

use tokio::time::Duration;

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


    let my_data = Arc::new(NodeData::new(args.port, peers));

    let my_peering_node = MyPeeringNode::new(my_data);

    my_peering_node.mingle().await?;

    // let peer_check = &my_peering_node.initialize_peer_check(Duration::from_secs(10));

    print_peers(&my_peering_node.data.known_peers.read().unwrap());

    println!("awaiting further introductions...");

    let server = Server::builder()
        .add_service(PeeringNodeServer::new(my_peering_node))
        .serve(utils::ipv6_loopback_socketaddr(args.port))
        .await?;


    Ok(())
}
