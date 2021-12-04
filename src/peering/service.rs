use crate::peering::errors::*;
use crate::peering::grpc::*;
use crate::peering::hash::Key;
use crate::peering::this_node::{OtherNode, ThisNode};
use std::net::SocketAddr;

use std::sync::Arc;
use tonic::async_trait;

use crate::peering::utils;
use tokio::time::Duration;

use tonic::transport::Server;
use tonic::{Code, Request, Response, Status};
use tracing::instrument;

pub async fn run_service(
    this_node: Arc<ThisNode>,
    port: u16,
) -> Result<(), Box<dyn std::error::Error>> {
    let node_service = ThisNodeService::new(this_node);

    Server::builder()
        .add_service(node_service_server::NodeServiceServer::new(node_service))
        .serve(utils::ipv6_loopback_socketaddr(port))
        .await?;

    Ok(())
}

#[derive(Debug)]
struct ThisNodeService {
    node: Arc<ThisNode>,
}

impl ThisNodeService {
    fn new(node: Arc<ThisNode>) -> Self {
        ThisNodeService { node }
    }
}

#[async_trait]
impl NodeService for ThisNodeService {
    #[instrument(skip(request))]
    async fn introduce(
        &self,
        request: Request<IntroductionRequest>,
    ) -> Result<Response<IntroductionReply>, Status> {
        let IntroductionRequest { sender_addr, .. } = request.into_inner();

        let parsed_addr = sender_addr.parse().or(Err(Status::new(
            Code::InvalidArgument,
            "Malformed sender address",
        )))?;

        match self.node.introduction_rcvd(parsed_addr).await {
            Ok((known_peers, data)) => Ok(Response::new(IntroductionReply {
                known_peers: known_peers.iter().map(KnownPeer::from).collect(),
                data,
            })),
            Err(IntroductionError::Common(_)) => Err(Status::new(Code::Internal, "Internal Error")),
        }
    }
    #[instrument(skip(request))]
    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetReply>, Status> {
        let GetRequest { key, .. } = request.into_inner();

        let parsed_key = key
            .try_into()
            .or(Err(Status::new(Code::InvalidArgument, "Malformed Key")))?;

        match self.node.get_rcvd(parsed_key).await {
            Ok(value) => Ok(Response::new(GetReply { value })),
            Err(GetError::NotFound(NotFoundError {
                key,
                originating_node,
            })) => Err(Status::new(
                Code::NotFound,
                format!(
                    "key {} not found (primary steward would have been {})",
                    key.as_hex_string(),
                    originating_node.to_string()
                ),
            )),
            Err(GetError::Common(_)) => Err(Status::new(Code::Internal, "Internal error")),
        }
    }

    #[instrument(skip(request))]
    async fn store(&self, request: Request<StoreRequest>) -> Result<Response<StoreReply>, Status> {
        let StoreRequest { value, .. } = request.into_inner();

        match self.node.store_rcvd(value).await {
            Ok((key, stored_in)) => Ok(Response::new(StoreReply {
                key: key.as_hex_string(),
                stored_in: stored_in.to_string(),
            })),
            Err(StoreError::Common(_)) => Err(Status::new(Code::Internal, "Internal Error")),
        }
    }
    #[instrument(skip(request))]
    async fn store_secondary(
        &self,
        request: Request<SecondaryStoreRequest>,
    ) -> Result<Response<SecondaryStoreReply>, Status> {
        let SecondaryStoreRequest { value, .. } = request.into_inner();

        match self.node.secondary_store_rcvd(value).await {
            Ok(()) => Ok(Response::new(SecondaryStoreReply {})),
            Err(StoreError::Common(_)) => Err(Status::new(Code::Internal, "Internal Error")),
        }
    }
    #[instrument(skip(_request))]
    async fn check_health(
        &self,
        _request: Request<HealthCheckRequest>,
    ) -> Result<Response<HealthCheckReply>, Status> {
        Ok(Response::new(HealthCheckReply {}))
    }

    #[instrument(skip(_request))]
    async fn get_status(
        &self,
        _request: Request<GetStatusRequest>,
    ) -> Result<Response<GetStatusReply>, Status> {
        let primary_store = self.node.primary_store.read().await;
        let secondary_store = self.node.secondary_store.read().await;
        let secondants = self.node.secondants.read().await;

        let ser_prim_store = primary_store.iter().map(KeyValuePair::from).collect();
        let ser_sec_store = secondary_store
            .iter()
            .map(SecondaryStoreEntry::from)
            .collect();
        let ser_secs = secondants.iter().map(Secondant::from).collect();

        Ok(Response::new(GetStatusReply {
            primary_store: ser_prim_store,
            secondary_store: ser_sec_store,
            secondants: ser_secs,
        }))
    }

    #[instrument(skip(_request))]
    async fn shut_down(
        &self,
        _request: Request<ShutDownRequest>,
    ) -> Result<Response<ShutDownReply>, Status> {
        tokio::spawn(async {
            tokio::time::sleep(Duration::from_millis(50)).await;
            std::process::exit(0)
        });
        Ok(Response::new(ShutDownReply {}))
    }
}

impl From<(Key, String)> for KeyValuePair {
    fn from((key, value): (Key, String)) -> Self {
        KeyValuePair {
            key: key.into(),
            value,
        }
    }
}

impl From<(&Key, &String)> for KeyValuePair {
    fn from((key, value): (&Key, &String)) -> Self {
        KeyValuePair {
            key: key.as_hex_string(),
            value: value.clone(),
        }
    }
}

impl From<(&Key, &OtherNode)> for Secondant {
    fn from((key, other_node): (&Key, &OtherNode)) -> Self {
        Secondant {
            key: key.as_hex_string(),
            addr: other_node.addr.to_string(),
        }
    }
}

impl From<&OtherNode> for KnownPeer {
    fn from(peering_node: &OtherNode) -> Self {
        KnownPeer {
            addr: peering_node.addr.to_string(),
        }
    }
}

impl From<(&SocketAddr, &Vec<(Key, String)>)> for SecondaryStoreEntry {
    fn from((addr, entries): (&SocketAddr, &Vec<(Key, String)>)) -> Self {
        SecondaryStoreEntry {
            addr: addr.to_string(),
            entries: entries
                .clone()
                .into_iter()
                .map(KeyValuePair::from)
                .collect(),
        }
    }
}
