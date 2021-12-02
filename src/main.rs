use std::net::SocketAddr;
use std::sync::Arc;
use structopt::StructOpt;

mod peering;

use peering::grpc::peering_node_server::PeeringNodeServer;

use peering::node::{print_peers, KnownPeer, MyPeeringNode, NodeData};
use peering::utils;

use tonic::transport::Server;

use tokio::time::Duration;
use tracing::{info, instrument};
use tracing_subscriber;

#[derive(StructOpt, Debug)]
struct Cli {
    port: u16,

    #[structopt(short = "p", long = "peer")]
    pub bootstrap_peer: Option<SocketAddr>,

    #[structopt(short = "i", long = "--check-interval", default_value = "20")]
    pub check_interval: u64,
}

#[instrument]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let args = Cli::from_args();

    let peers = if args.bootstrap_peer.is_some() {
        vec![KnownPeer::new(args.bootstrap_peer.unwrap())]
    } else {
        vec![]
    };

    info!("bootstrapping using {:?}", args.bootstrap_peer);

    let my_data = Arc::new(NodeData::new(args.port, peers));

    let my_peering_node = MyPeeringNode::new(my_data.clone());

    my_peering_node.mingle().await?;

    let data_clone = my_data.clone();

    tokio::task::spawn(async move {
        info!("starting peer check loop");
        data_clone
            .peer_check_loop(Duration::from_secs(args.check_interval))
            .await
    });

    info!("starting server on port {:?}", args.port);
    Server::builder()
        .add_service(PeeringNodeServer::new(my_peering_node))
        .serve(utils::ipv6_loopback_socketaddr(args.port))
        .await?;

    Ok(())
}
