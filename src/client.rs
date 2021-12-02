mod peering;
use peering::grpc::peering_node_client::PeeringNodeClient;
use peering::grpc::{
    GetDataShardReply, GetDataShardRequest, GetKeyReply, GetKeyRequest, KeyValuePair,
    ListPeersRequest, StoreReply, StoreRequest,
};
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

    #[structopt(name = "get-all")]
    GetAll(BaseCli),
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

    let peers: Vec<KnownPeer> = response
        .get_ref()
        .known_peers
        .iter()
        .filter_map(|p| KnownPeer::try_from(p.clone()).ok())
        .collect();

    let mut leading_hashes: Vec<Vec<KnownPeer>> = vec![vec![]; 255];

    peers
        .into_iter()
        .map(|peer| (peer.hash.arr[31], peer))
        .for_each(|(ph, peer)| {
            leading_hashes.get_mut(ph as usize).unwrap().push(peer);
        });

    for (i, peers) in leading_hashes.into_iter().enumerate() {
        println!("{:02x}: {:?}", i, peers);
    }

    Ok(())
}

#[derive(StructOpt, Debug)]
struct GetArgs {
    #[structopt(flatten)]
    base: BaseCli,
}

#[derive(StructOpt, Debug)]
struct GetKeyArgs {
    #[structopt(flatten)]
    base: BaseCli,

    key: String,
}

async fn get_key(peer: &SocketAddr, key: String) -> Result<(), Box<dyn std::error::Error>> {
    let target_url = build_grpc_url(&peer);

    let mut client = PeeringNodeClient::connect(target_url).await.unwrap();

    match client
        .get_key(tonic::Request::new(GetKeyRequest { key }))
        .await
    {
        Ok(response) => {
            let GetKeyReply { value } = response.get_ref();

            println!("Value: {}", value);
        }
        Err(err) => {
            println!("Error occured: {}", err.message());
        }
    }

    Ok(())
}

async fn get_all(peer: &SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
    let target_url = build_grpc_url(&peer);

    let mut client = PeeringNodeClient::connect(target_url).await.unwrap();

    let response = client
        .get_data_shard(tonic::Request::new(GetDataShardRequest {}))
        .await?;

    let GetDataShardReply { shard } = response.get_ref();

    for KeyValuePair { key: _, value } in shard {
        println!("- {}", value);
    }

    Ok(())
}

#[derive(StructOpt, Debug)]
struct StoreValueArgs {
    #[structopt(flatten)]
    base: BaseCli,

    value: String,
}

async fn store_value(peer: &SocketAddr, value: String) -> Result<(), Box<dyn std::error::Error>> {
    let target_url = build_grpc_url(&peer);

    let mut client = PeeringNodeClient::connect(target_url).await.unwrap();

    let response = client
        .store(tonic::Request::new(StoreRequest { value, hops: 0 }))
        .await
        .unwrap();

    let StoreReply { stored_in, key } = response.get_ref();

    println!("Stored in {}", stored_in);
    println!("at {}", key);

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
        Cli::GetAll(cfg) => {
            get_all(&cfg.peer).await?;
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
