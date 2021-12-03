mod peering;
use peering::grpc::peering_node_client::PeeringNodeClient;
use peering::grpc::{
    GetKeyReply, GetKeyRequest, GetStatusReply, GetStatusRequest, KeyValuePair, ListPeersReply,
    ListPeersRequest, ShutDownReply, ShutDownRequest, Stewardship, StoreReply, StoreRequest,
};
use peering::peer::KnownPeer;
use peering::utils::{build_grpc_url, shorten};

use std::net::{AddrParseError, SocketAddr};
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
struct BaseCli {
    #[structopt(short = "p", long = "peer", parse(try_from_str=parse_peer_flag))]
    peer: SocketAddr,
}

fn parse_peer_flag(addr_s: &str) -> Result<SocketAddr, AddrParseError> {
    addr_s
        .parse::<SocketAddr>()
        .map_or_else(|_| format!("[::1]{:}", addr_s).parse(), |s| Ok(s))
}

#[tonic::async_trait]
trait ClientAction {
    type Request: Send;
    type Response;

    fn build_request(&self) -> Self::Request;
    async fn call(
        &self,
        client: PeeringNodeClient<tonic::transport::Channel>,
        request: tonic::Request<Self::Request>,
    ) -> Result<tonic::Response<Self::Response>, tonic::Status>;

    fn handle_response(&self, response: &Self::Response);
    fn handle_err(&self, err: tonic::Status) {
        println!("Error occured: {}", err);
    }

    async fn perform(&self, client: PeeringNodeClient<tonic::transport::Channel>) -> () {
        let request = self.build_request();

        match self.call(client, tonic::Request::new(request)).await {
            Ok(response) => {
                self.handle_response(response.get_ref());
            }
            Err(err) => {
                self.handle_err(err);
            }
        }
    }
}

struct ListPeersAction {}

impl ListPeersAction {
    fn new() -> Self {
        Self {}
    }
}

#[tonic::async_trait]
impl ClientAction for ListPeersAction {
    type Request = ListPeersRequest;
    type Response = ListPeersReply;

    fn build_request(&self) -> Self::Request {
        Self::Request {}
    }

    async fn call(
        &self,
        mut client: PeeringNodeClient<tonic::transport::Channel>,
        request: tonic::Request<Self::Request>,
    ) -> Result<tonic::Response<Self::Response>, tonic::Status> {
        client.list_peers(request).await
    }

    fn handle_response(&self, response: &Self::Response) {
        let peers: Vec<KnownPeer> = response
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
    }
}

struct GetKeyAction {
    key: String,
}

impl GetKeyAction {
    fn new(key: String) -> Self {
        Self { key }
    }
}

#[tonic::async_trait]
impl ClientAction for GetKeyAction {
    type Request = GetKeyRequest;
    type Response = GetKeyReply;

    fn build_request(&self) -> Self::Request {
        Self::Request {
            key: self.key.clone(),
        }
    }

    async fn call(
        &self,
        mut client: PeeringNodeClient<tonic::transport::Channel>,
        request: tonic::Request<Self::Request>,
    ) -> Result<tonic::Response<Self::Response>, tonic::Status> {
        client.get_key(request).await
    }

    fn handle_response(&self, response: &Self::Response) {
        println!("Value: {}", response.value);
    }
}

struct GetStatusAction {}

impl GetStatusAction {
    fn new() -> Self {
        Self {}
    }
}

#[tonic::async_trait]
impl ClientAction for GetStatusAction {
    type Request = GetStatusRequest;
    type Response = GetStatusReply;

    fn build_request(&self) -> Self::Request {
        Self::Request {}
    }

    async fn call(
        &self,
        mut client: PeeringNodeClient<tonic::transport::Channel>,
        request: tonic::Request<Self::Request>,
    ) -> Result<tonic::Response<Self::Response>, tonic::Status> {
        client.get_status(request).await
    }

    fn handle_response(&self, response: &Self::Response) {
        let GetStatusReply {
            shard,
            secondary_shard,
            secondary_stewardships,
        } = &response;

        println!("Primary data:");

        for KeyValuePair { key, value } in shard {
            println!("- {}: {}", shorten(key), value);
        }

        println!("Secondary data:");

        for KeyValuePair { key, value } in secondary_shard {
            println!("- {}: {}", shorten(key), value);
        }

        println!("Secondary stewardships:");

        for Stewardship { addr, keys } in secondary_stewardships {
            print!("{} :", addr);
            for key in keys {
                println!(" {}", shorten(key));
            }
        }
    }
}

struct ShutDownAction {}

impl ShutDownAction {
    fn new() -> Self {
        Self {}
    }
}

#[tonic::async_trait]
impl ClientAction for ShutDownAction {
    type Request = ShutDownRequest;
    type Response = ShutDownReply;

    fn build_request(&self) -> Self::Request {
        Self::Request {}
    }

    async fn call(
        &self,
        mut client: PeeringNodeClient<tonic::transport::Channel>,
        request: tonic::Request<Self::Request>,
    ) -> Result<tonic::Response<Self::Response>, tonic::Status> {
        client.shut_down(request).await
    }

    fn handle_response(&self, _: &Self::Response) {}
}

struct StoreValueAction {
    value: String,
}

impl StoreValueAction {
    fn new(value: String) -> Self {
        Self { value }
    }
}

#[tonic::async_trait]
impl ClientAction for StoreValueAction {
    type Request = StoreRequest;
    type Response = StoreReply;

    fn build_request(&self) -> Self::Request {
        Self::Request {
            value: self.value.clone(),
            as_secondary: false,
        }
    }

    async fn call(
        &self,
        mut client: PeeringNodeClient<tonic::transport::Channel>,
        request: tonic::Request<Self::Request>,
    ) -> Result<tonic::Response<Self::Response>, tonic::Status> {
        client.store(request).await
    }

    fn handle_response(&self, response: &Self::Response) {
        let Self::Response { stored_in, key } = response;

        println!("Stored in {}", stored_in);
        println!("at {}", key);
    }
}

async fn connect(addr: &SocketAddr) -> PeeringNodeClient<tonic::transport::Channel> {
    let target_url = build_grpc_url(addr);
    // println!("connecting to {}", &target_url);, Status

    match PeeringNodeClient::connect(target_url).await {
        Ok(client) => client,
        Err(err) => {
            println!("Error connecting: {}", err = err);
            std::process::exit(1);
        }
    }
}

#[derive(StructOpt, Debug)]
struct ListPeersArgs {
    #[structopt(flatten)]
    base: BaseCli,
}

#[derive(StructOpt, Debug)]
struct StoreValueArgs {
    #[structopt(flatten)]
    base: BaseCli,

    value: String,
}

#[derive(StructOpt, Debug)]
struct GetKeyArgs {
    #[structopt(flatten)]
    base: BaseCli,

    key: String,
}

#[derive(StructOpt, Debug)]
struct GetStatusArgs {
    #[structopt(flatten)]
    base: BaseCli,
}

#[derive(StructOpt, Debug)]
struct ShutDownArgs {
    #[structopt(flatten)]
    base: BaseCli,
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

    #[structopt(name = "status")]
    GetStatus(GetStatusArgs),

    #[structopt(name = "shutdown")]
    ShutDown(ShutDownArgs),
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    match Cli::from_args() {
        Cli::ListPeers(cfg) => {
            let client = connect(&cfg.base.peer).await;
            ListPeersAction::new().perform(client).await;
        }
        Cli::StoreValue(cfg) => {
            let client = connect(&cfg.base.peer).await;
            StoreValueAction::new(cfg.value).perform(client).await;
        }
        Cli::GetKey(cfg) => {
            let client = connect(&cfg.base.peer).await;
            GetKeyAction::new(cfg.key).perform(client).await;
        }
        Cli::GetStatus(cfg) => {
            let client = connect(&cfg.base.peer).await;
            println!("Connected to {}", &cfg.base.peer);
            println!("");
            GetStatusAction::new().perform(client).await;
        }
        Cli::ShutDown(cfg) => {
            let client = connect(&cfg.base.peer).await;
            ShutDownAction::new().perform(client).await;
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
