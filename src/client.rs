use std::net::{AddrParseError, SocketAddr};

use structopt::StructOpt;

mod peering;
use peering::peering_node_client::PeeringNodeClient;
use peering::ListPeersRequest;

mod utils;
use utils::build_grpc_url;

#[derive(StructOpt, Debug)]
#[structopt(name = "client")]
enum Cli {
    #[structopt(name = "list-peers")]
    ListPeers(ListPeersArgs),
}

fn parse_peer_flag(addr_s: &str) -> Result<SocketAddr, AddrParseError> {
    addr_s
        .parse::<SocketAddr>()
        .map_or_else(|_| format!("[::1]{:}", addr_s).parse(), |s| Ok(s))
}

#[derive(StructOpt, Debug)]
struct ListPeersArgs {
    #[structopt(short = "p", long = "peer", parse(try_from_str=parse_peer_flag))]
    bootstrap_peer: SocketAddr,
}

async fn list_peers(peer: &SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = PeeringNodeClient::connect(build_grpc_url(peer)).await?;

    let request = tonic::Request::new(ListPeersRequest {});

    let response = client.list_peers(request).await.unwrap();

    for peer in response.get_ref().known_peers.iter() {
        println!("{}", peer);
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    match Cli::from_args() {
        Cli::ListPeers(cfg) => {
            list_peers(&cfg.bootstrap_peer).await?;
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv6Addr};
    #[test]
    fn parse_regular_addr() {
        assert_eq!(
            parse_peer_flag("[::1]:5000"),
            Ok(SocketAddr::new(
                IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 0x1)),
                5000
            ))
        )
    }

    #[test]
    fn parse_just_the_port() {
        assert_eq!(
            parse_peer_flag(":1234"),
            Ok(SocketAddr::new(
                IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 0x1)),
                1234
            ))
        )
    }
}
