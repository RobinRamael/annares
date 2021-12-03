use crate::peering::grpc;
use crate::peering::grpc::peering_node_client::PeeringNodeClient;
use crate::peering::grpc::{
    GetKeyReply, GetKeyRequest, KeyValuePair, StoreReply, StoreRequest, TransferReply,
    TransferRequest,
};
use crate::peering::hash::Hash;
use crate::peering::utils;

use std::fmt::Debug;
use std::fmt::Display;
use std::net::SocketAddr;
use std::time::Instant;
use tonic::{Request, Status};
use tracing::{error, instrument};

#[derive(Clone)]
pub struct KnownPeer {
    pub addr: SocketAddr,
    pub hash: Hash,
    pub last_contact: Instant,
    pub contacts: u64,
}

impl KnownPeer {
    pub fn new(addr: SocketAddr) -> Self {
        KnownPeer {
            addr,
            hash: Hash::hash(&addr.to_string()),
            last_contact: Instant::now(),
            contacts: 0,
        }
    }

    async fn connect(
        &self,
    ) -> Result<PeeringNodeClient<tonic::transport::Channel>, tonic::transport::Error> {
        let target_url = utils::build_grpc_url(&self.addr);
        // println!("connecting to {}", &target_url);

        match PeeringNodeClient::connect(target_url).await {
            Ok(client) => Ok(client),
            Err(error) => {
                error!("Error connecting: {}", err = error);
                Err(error)
            }
        }
    }

    pub async fn transfer_stewardship(
        &self,
        kv_pairs: Vec<(Hash, String)>,
    ) -> Result<Vec<Hash>, tonic::transport::Error> {
        match self.connect().await {
            Ok(mut client) => {
                let request = Request::new(TransferRequest {
                    to_store: kv_pairs
                        .into_iter()
                        .map(|kv| {
                            let (key, value) = kv;
                            KeyValuePair {
                                key: key.as_hex_string(),
                                value,
                            }
                        })
                        .collect(),
                });

                let keys_transferred = client
                    .transfer(request)
                    .await
                    .map(|response| {
                        let TransferReply { transferred_keys } = response.get_ref();
                        transferred_keys
                            .iter()
                            .cloned()
                            .map(|s| s.try_into().unwrap())
                            .collect()
                    })
                    .unwrap();

                Ok(keys_transferred)
            }
            Err(err) => Err(err),
        }
    }

    pub async fn get_key(&self, key: &Hash) -> Result<String, Status> {
        match self.connect().await {
            Ok(mut client) => {
                // let mut c = client;
                let request = Request::new(GetKeyRequest {
                    key: key.as_hex_string(),
                });

                match client.get_key(request).await {
                    Ok(response) => {
                        let GetKeyReply { value, .. } = response.into_inner();
                        Ok(value)
                    }
                    Err(err) => Err(err),
                }
            }
            Err(_) => return Err(Status::new(tonic::Code::Internal, "connection failed")),
        }
    }

    #[instrument(skip(self))]
    pub async fn store(
        &self,
        _key: &Hash,
        value: &String,
        as_secondary: bool,
    ) -> Result<SocketAddr, Status> {
        match self.connect().await {
            Ok(mut client) => {
                // let mut c = client;
                let request = Request::new(StoreRequest {
                    value: value.clone(),
                    as_secondary,
                });

                match client.store(request).await {
                    Ok(response) => {
                        let StoreReply { stored_in, .. } = response.into_inner();
                        Ok(stored_in.parse().unwrap())
                    }
                    Err(err) => Err(err),
                }
            }
            Err(_) => return Err(Status::new(tonic::Code::Internal, "connection failed")),
        }
    }

    pub async fn assign_secondary_stewardship(
        &self,
        key: &Hash,
        value: &String,
    ) -> Result<(), Status> {
        match self.store(key, value, true).await {
            Ok(_) => Ok(()),
            Err(err) => Err(err),
        }
    }

    pub fn distance_to(&self, key: &Hash) -> Hash {
        self.hash.cyclic_distance(key)
    }
}

impl Display for KnownPeer {
    fn fmt(&self, fmtr: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(fmtr, "{}", self.addr)
    }
}

impl From<KnownPeer> for grpc::Peer {
    fn from(peering_node: KnownPeer) -> Self {
        grpc::Peer {
            addr: peering_node.addr.to_string(),
            hash: peering_node.hash.as_hex_string(),
        }
    }
}

impl TryFrom<grpc::Peer> for KnownPeer {
    type Error = std::net::AddrParseError;

    fn try_from(grpc_peer: grpc::Peer) -> Result<Self, Self::Error> {
        let addr = grpc_peer.addr.parse::<SocketAddr>()?;

        Ok(KnownPeer::new(addr))
    }
}

impl PartialEq for KnownPeer {
    fn eq(&self, other: &Self) -> bool {
        self.addr == other.addr
    }
}

impl std::hash::Hash for KnownPeer {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.addr.hash(state);
    }
}

impl Eq for KnownPeer {}

impl Debug for KnownPeer {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("KnownPeer")
            .field("addr", &self.addr)
            .field("contacts", &self.contacts)
            .finish()
    }
}
