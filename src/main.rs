#![allow(dead_code)]
use std::net::SocketAddr;
use structopt::StructOpt;

use std::sync::Arc;
use tracing::*;

mod peering;
use opentelemetry::global;
use opentelemetry::sdk::propagation::TraceContextPropagator;
// use peering::health_check::spawn_health_check;
// use peering::service::run_service;
// use peering::this_node::ThisNode;

use tokio::time::Duration;
use tracing_subscriber::prelude::*;

mod chord;
use chord::node::ChordNode;
use chord::service::run_service;

mod keys;
use self::keys::Key;

mod utils;
use utils::ipv6_loopback_socketaddr;

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
    // Create a tracing layer with the configured tracer
    let args = Cli::from_args();

    global::set_text_map_propagator(TraceContextPropagator::new());
    let tracer = opentelemetry_jaeger::new_pipeline()
        .with_service_name(format!("annares-{}", args.port))
        // .with_max_packet_size(9_216)
        .install_batch(opentelemetry::runtime::Tokio)?;
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new("INFO"))
        .with(tracing_opentelemetry::layer().with_tracer(tracer))
        .try_init()?;

    let root = span!(tracing::Level::INFO, "app_start", work_units = 2);
    let _enter = root.enter();

    info!("Initializing {}", args.port);

    let addr = ipv6_loopback_socketaddr(args.port);

    let node = Arc::new(match args.bootstrap_peer {
        Some(peer) => ChordNode::join(addr, peer)
            .await
            .expect("Failed to join network"),
        None => ChordNode::new(addr, addr),
    });

    info!(port = args.port, "Starting stabilize loop...");
    let node_clone = Arc::clone(&node);
    tokio::spawn(async move {
        node_clone
            .stabilization_loop(Duration::from_secs(args.check_interval))
            .await
    });

    info!(port = args.port, "Starting server...");
    run_service(node, args.port).await.expect("Server crashed");

    opentelemetry::global::shutdown_tracer_provider();

    Ok(())
}
