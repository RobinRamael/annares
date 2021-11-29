mod peering;
use peering::grpc::peering_node_client::PeeringNodeClient;
use peering::grpc::ListPeersRequest;
use peering::hash::Hash;
use peering::node::KnownPeer;
use peering::utils::build_grpc_url;

use std::net::{AddrParseError, SocketAddr};

use structopt::StructOpt;

#[derive(StructOpt, Debug)]
struct BaseCli {
    #[structopt(short = "p", long = "peer", parse(try_from_str=parse_peer_flag))]
    peer: SocketAddr,
}

#[derive(StructOpt, Debug)]
#[structopt(name = "client")]
enum Cli {
    #[structopt(name = "list-peers")]
    ListPeers(ListPeersArgs),

    #[structopt(name = "store")]
    StoreValue(StoreValueArgs),

    #[structopt(name = "get")]
    GetKey(GetKeyArgs),
}

fn parse_peer_flag(addr_s: &str) -> Result<SocketAddr, AddrParseError> {
    addr_s
        .parse::<SocketAddr>()
        .map_or_else(|_| format!("[::1]{:}", addr_s).parse(), |s| Ok(s))
}

#[derive(StructOpt, Debug)]
struct ListPeersArgs {
    #[structopt(flatten)]
    base: BaseCli,
}

async fn list_peers(peer: &SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = PeeringNodeClient::connect(build_grpc_url(peer)).await?;

    let request = tonic::Request::new(ListPeersRequest {});

    let response = client.list_peers(request).await.unwrap();

    for peer in response
        .get_ref()
        .known_peers
        .iter()
        .filter_map(|p| KnownPeer::try_from(p.clone()).ok())
    {
        println!("{:?}", peer);
    }

    Ok(())
}

#[derive(StructOpt, Debug)]
struct GetKeyArgs {
    #[structopt(flatten)]
    base: BaseCli,

    key: String,
}

async fn get_key(_peer: &SocketAddr, _key: String) -> Result<(), Box<dyn std::error::Error>> {
    todo!()
}

#[derive(StructOpt, Debug)]
struct StoreValueArgs {
    #[structopt(flatten)]
    base: BaseCli,

    value: String,
}

async fn store_value(_peer: &SocketAddr, value: String) -> Result<(), Box<dyn std::error::Error>> {
    let h1 = Hash::hash(value);
    println!("{:?}", h1.arr);
    let s = format!("{:}", h1.as_hex_string());
    let h_again: Hash = s.try_into()?;
    println!("{:?}", h_again.arr);

    // let h2 = Hash::hash("hello!".into());
    // println!("{}", &h2.as_hex_string());

    // let dist = h1.cyclic_distance(h2);

    // println!("----------------------------------------------------------------------------");
    // println!("{}", &dist.as_hex_string());
    // println!("");
    // println!("");

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    match Cli::from_args() {
        Cli::ListPeers(cfg) => {
            list_peers(&cfg.base.peer).await?;
        }
        Cli::StoreValue(cfg) => {
            store_value(&cfg.base.peer, cfg.value).await?;
        }
        Cli::GetKey(cfg) => {
            get_key(&cfg.base.peer, cfg.key).await?;
        }
    }
    Ok(())
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
