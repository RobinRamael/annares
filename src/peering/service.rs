use crate::keys::Key;
use crate::peering::errors::*;
use crate::peering::grpc::*;
use crate::peering::this_node::{OtherNode, ThisNode};
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;

use std::sync::Arc;
use tonic::async_trait;

use tokio::time::Duration;

use crate::utils;

use tonic::transport::Server;
use tonic::{Code, Request, Response, Status};

use opentelemetry::{global, propagation::Extractor};
use tracing::*;
use tracing_opentelemetry::OpenTelemetrySpanExt;

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

    fn common_error_into_status(&self, err: CommonError) -> Status {
        match err {
            CommonError::Status(err) => Status::new(
                err.cause.code(),
                format!("{}: {}", self.node.addr, err.cause.message()),
            ),
            CommonError::Unavailable => Status::new(
                Code::Unavailable,
                "Network still recovering. Try again later.",
            ),
            _ => Status::new(Code::Internal, "Internal Error"),
        }
    }
}

#[async_trait]
impl NodeService for ThisNodeService {
    #[instrument]
    async fn introduce(
        &self,
        request: Request<IntroductionRequest>,
    ) -> Result<Response<IntroductionReply>, Status> {
        let parent_cx =
            global::get_text_map_propagator(|prop| prop.extract(&MetadataMap(request.metadata())));
        tracing::Span::current().set_parent(parent_cx);

        let IntroductionRequest { sender_addr, .. } = request.into_inner();

        let parsed_addr = sender_addr.parse().or(Err(Status::new(
            Code::InvalidArgument,
            "Malformed sender address",
        )))?;

        match self.node.introduction_rcvd(parsed_addr).await {
            Ok(known_peers) => Ok(Response::new(IntroductionReply {
                known_peers: known_peers.iter().map(KnownPeer::from).collect(),
            })),
            Err(IntroductionError::Common(err)) => Err(self.common_error_into_status(err)), // pass on status errors to the next node
        }
    }
    #[instrument]
    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetReply>, Status> {
        let parent_cx =
            global::get_text_map_propagator(|prop| prop.extract(&MetadataMap(request.metadata())));
        tracing::Span::current().set_parent(parent_cx);

        let GetRequest { key, .. } = request.into_inner();

        let parsed_key = key
            .try_into()
            .or(Err(Status::new(Code::InvalidArgument, "Malformed Key")))?;

        match self.node.get_rcvd(parsed_key).await {
            Ok((value, primary_holder)) => Ok(Response::new(GetReply {
                value,
                primary_holder: primary_holder.to_string(),
            })),
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
            Err(GetError::Common(err)) => Err(self.common_error_into_status(err)), // pass on status errors to the next node
        }
    }

    #[instrument]
    async fn store(&self, request: Request<StoreRequest>) -> Result<Response<StoreReply>, Status> {
        let parent_cx =
            global::get_text_map_propagator(|prop| prop.extract(&MetadataMap(request.metadata())));
        tracing::Span::current().set_parent(parent_cx);
        let StoreRequest { value, corpse, .. } = request.into_inner();

        let parsed_corpse = corpse
            .map(|p| {
                p.parse()
                    .or(Err(invalid_argument("Malformed corpse address")))
            })
            .transpose()?;

        match self.node.store_rcvd(value, parsed_corpse).await {
            Ok((key, stored_in)) => Ok(Response::new(StoreReply {
                key: key.as_hex_string(),
                stored_in: stored_in.to_string(),
            })),
            Err(StoreError::Common(err)) => Err(self.common_error_into_status(err)),
        }
    }

    #[instrument]
    async fn store_secondary(
        &self,
        request: Request<SecondaryStoreRequest>,
    ) -> Result<Response<SecondaryStoreReply>, Status> {
        let parent_cx =
            global::get_text_map_propagator(|prop| prop.extract(&MetadataMap(request.metadata())));
        tracing::Span::current().set_parent(parent_cx);

        let SecondaryStoreRequest {
            value,
            primary_holder,
            corpse,
        } = request.into_inner();

        let parsed_addr = primary_holder
            .parse()
            .or(Err(invalid_argument("Malformed primary holder address")))?;

        let parsed_corpse = corpse
            .map(|p| {
                p.parse()
                    .or(Err(invalid_argument("Malformed corpse address")))
            })
            .transpose()?;

        match self
            .node
            .secondary_store_rcvd(value, parsed_addr, parsed_corpse)
            .await
        {
            Ok(key) => Ok(Response::new(SecondaryStoreReply {
                key: key.as_hex_string(),
            })),
            Err(StoreError::Common(_)) => Err(Status::new(Code::Internal, "Internal Error")),
        }
    }
    #[instrument]
    async fn move_values(
        &self,
        request: Request<MoveValuesRequest>,
    ) -> Result<Response<MoveValuesReply>, Status> {
        let parent_cx =
            global::get_text_map_propagator(|prop| prop.extract(&MetadataMap(request.metadata())));
        tracing::Span::current().set_parent(parent_cx);
        let MoveValuesRequest { values } = request.into_inner();

        match self.node.move_values_rcvd(values).await {
            Ok(keys) => Ok(Response::new(MoveValuesReply {
                keys: keys.iter().map(Key::as_hex_string).collect(),
            })),
            Err(MoveValuesError::Common(err)) => Err(self.common_error_into_status(err)),
        }
    }
    #[instrument]
    async fn check_health(
        &self,
        request: Request<HealthCheckRequest>,
    ) -> Result<Response<HealthCheckReply>, Status> {
        let parent_cx =
            global::get_text_map_propagator(|prop| prop.extract(&MetadataMap(request.metadata())));
        tracing::Span::current().set_parent(parent_cx);
        Ok(Response::new(HealthCheckReply {}))
    }

    #[instrument]
    async fn get_status(
        &self,
        request: Request<GetStatusRequest>,
    ) -> Result<Response<GetStatusReply>, Status> {
        let parent_cx =
            global::get_text_map_propagator(|prop| prop.extract(&MetadataMap(request.metadata())));
        tracing::Span::current().set_parent(parent_cx);
        let primary_store = self.node.primary_store.read().await;
        let secondary_store = self.node.secondary_store.read().await;
        let secondant_map = self.node.secondant_map.read().await;
        let peers = self.node.peers.known_peers.read().await;

        let ser_prim_store = primary_store.iter().map(KeyValuePair::from).collect();
        let ser_sec_store = secondary_store
            .iter()
            .map(SecondaryStoreEntry::from)
            .collect();
        let ser_secs = secondant_map
            .iter()
            .map(SecondantStoreEntry::from)
            .collect();
        let ser_peers = peers.values().map(|p| p.into()).collect();

        Ok(Response::new(GetStatusReply {
            primary_store: ser_prim_store,
            secondary_store: ser_sec_store,
            secondants: ser_secs,
            peers: ser_peers,
        }))
    }

    #[instrument]
    async fn shut_down(
        &self,
        request: Request<ShutDownRequest>,
    ) -> Result<Response<ShutDownReply>, Status> {
        let parent_cx =
            global::get_text_map_propagator(|prop| prop.extract(&MetadataMap(request.metadata())));
        tracing::Span::current().set_parent(parent_cx);
        tokio::spawn(async {
            tokio::time::sleep(Duration::from_millis(50)).await;
            std::process::exit(0)
        });
        Ok(Response::new(ShutDownReply {}))
    }
}

struct MetadataMap<'a>(&'a tonic::metadata::MetadataMap);

impl<'a> Extractor for MetadataMap<'a> {
    /// Get a value for a key from the MetadataMap.  If the value can't be converted to &str,
    /// returns None
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|metadata| metadata.to_str().ok())
    }

    /// Collect all the keys from the MetadataMap.
    fn keys(&self) -> Vec<&str> {
        self.0
            .keys()
            .map(|key| match key {
                tonic::metadata::KeyRef::Ascii(v) => v.as_str(),
                tonic::metadata::KeyRef::Binary(v) => v.as_str(),
            })
            .collect::<Vec<_>>()
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

impl From<(&Key, &HashSet<OtherNode>)> for SecondantStoreEntry {
    fn from((key, peers): (&Key, &HashSet<OtherNode>)) -> Self {
        SecondantStoreEntry {
            key: key.as_hex_string(),
            addrs: peers
                .clone()
                .into_iter()
                .map(|p| p.addr.to_string())
                .collect(),
        }
    }
}

impl From<(&SocketAddr, &HashMap<Key, String>)> for SecondaryStoreEntry {
    fn from((addr, entries): (&SocketAddr, &HashMap<Key, String>)) -> Self {
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

fn invalid_argument(s: &str) -> Status {
    Status::new(Code::InvalidArgument, s)
}
