use std::net::SocketAddr;
use structopt::StructOpt;

use std::sync::Arc;
use tracing::{debug, info};
use tracing_appender;
use tracing_subscriber;
use tracing_subscriber::prelude::*;

mod peering;
use peering::health_check::spawn_health_check;
use peering::service::run_service;
use peering::this_node::ThisNode;
use peering::utils;
use tokio::time::Duration;

#[derive(StructOpt, Debug)]
struct Cli {
    port: u16,

    #[structopt(short = "p", long = "peer")]
    pub bootstrap_peer: Option<SocketAddr>,

    #[structopt(short = "i", long = "--check-interval", default_value = "2")]
    pub check_interval: u64,

    #[structopt(short = "r", long = "--redundancy", default_value = "3")]
    pub redundancy: u8,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let file_appender = tracing_appender::rolling::daily("/tmp/logs", "annares.log");
    let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::filter::EnvFilter::from_default_env())
        .event_format(tracing_subscriber::fmt::format().compact())
        .with_writer(non_blocking)
        .init();

    let args = Cli::from_args();
    info!("Initializing {}", args.port);

    let addr = utils::ipv6_loopback_socketaddr(args.port);

    let this_node = Arc::new(ThisNode::new(addr, args.redundancy));

    if let Some(bootstrap_peer) = args.bootstrap_peer {
        info!("Mingling with {}...", bootstrap_peer);
        let this_node_clone = Arc::clone(&this_node);
        tokio::spawn(async move {
            this_node_clone.mingle(&bootstrap_peer).await;
        });
    }

    info!("Spawning health check...");

    spawn_health_check(&this_node, Duration::from_secs(args.check_interval));

    info!(port = args.port, "Starting server...");
    run_service(this_node.clone(), args.port)
        .await
        .expect("Server crashed");

    Ok(())
}
