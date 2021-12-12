use opentelemetry::{global, propagation::Extractor};
use std::sync::Arc;
use tonic::transport::Server;
use tonic::{Code, Request, Response, Status};
use tracing::*;
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::chord::errors::*;
use crate::chord::grpc;
use crate::chord::grpc::*;
use crate::chord::node::ChordNode;
use crate::utils::ipv6_loopback_socketaddr;

pub async fn run_service(
    node: Arc<ChordNode>,
    port: u16,
) -> Result<(), Box<dyn std::error::Error>> {
    let node_service = ChordNodeService::new(node);

    Server::builder()
        .add_service(chord_service_server::ChordServiceServer::new(node_service))
        .serve(ipv6_loopback_socketaddr(port))
        .await?;

    Ok(())
}

struct ChordNodeService {
    node: Arc<ChordNode>,
}

impl ChordNodeService {
    fn new(node: Arc<ChordNode>) -> Self {
        ChordNodeService { node }
    }
}

#[tonic::async_trait]
impl grpc::ChordService for ChordNodeService {
    #[instrument(skip(self))]
    async fn find_successor(
        &self,
        request: Request<grpc::FindSuccessorRequest>,
    ) -> Result<Response<grpc::FindSuccessorReply>, Status> {
        link_remote_span(&request);

        let grpc::FindSuccessorRequest { key } = request.into_inner();

        let key = key
            .try_into()
            .or(Err(Status::new(Code::InvalidArgument, "Malformed Key")))?;

        match Arc::clone(&self.node).find_successor(key).await {
            Ok(succ) => Ok(Response::new(FindSuccessorReply {
                addr: succ.clone().to_string(),
            })),
            Err(err) => Err(Status::new(Code::Internal, err.message)),
        }
    }

    #[instrument(skip(self))]
    async fn get_predecessor(
        &self,
        request: Request<grpc::GetPredecessorRequest>,
    ) -> Result<Response<grpc::GetPredecessorReply>, Status> {
        link_remote_span(&request);

        match Arc::clone(&self.node).get_predecessor().await {
            Ok(pred_addr) => Ok(Response::new(GetPredecessorReply {
                addr: pred_addr.and_then(|a| Some(a.to_string())),
            })),
            Err(err) => Err(Status::new(Code::Internal, err.message)),
        }
    }

    #[instrument(skip(self))]
    async fn notify(
        &self,
        request: Request<grpc::NotifyRequest>,
    ) -> Result<Response<grpc::NotifyReply>, Status> {
        link_remote_span(&request);

        let grpc::NotifyRequest { addr } = request.into_inner();

        let addr = addr
            .parse()
            .or(Err(Status::new(Code::InvalidArgument, "Malformed Addr")))?;

        match Arc::clone(&self.node).notify(addr).await {
            Ok(()) => Ok(Response::new(NotifyReply {})),
            Err(err) => Err(Status::new(Code::Internal, err.message)),
        }
    }

    #[instrument(skip(self))]
    async fn get_key(
        &self,
        request: Request<grpc::GetRequest>,
    ) -> Result<Response<grpc::GetReply>, Status> {
        link_remote_span(&request);

        let grpc::GetRequest { key } = request.into_inner();

        let key = key
            .try_into()
            .or(Err(Status::new(Code::InvalidArgument, "Malformed Key")))?;

        match Arc::clone(&self.node).get_key(key).await {
            Ok(value) => Ok(Response::new(GetReply { value })),
            Err(GetError::BadLocation) => Err(Status::new(
                Code::FailedPrecondition,
                "The princess is in another castle",
            )),
            Err(GetError::NotFound) => Err(Status::new(Code::NotFound, "Something went wrong")),
            Err(GetError::Internal(err)) => Err(Status::new(Code::Internal, err.message)),
        }
    }

    #[instrument(skip(self))]
    async fn store_value(
        &self,
        request: Request<grpc::StoreRequest>,
    ) -> Result<Response<grpc::StoreReply>, Status> {
        link_remote_span(&request);

        let grpc::StoreRequest { value } = request.into_inner();

        match Arc::clone(&self.node).store_value(value).await {
            Ok(key) => Ok(Response::new(StoreReply {
                key: key.as_hex_string(),
            })),
            Err(StoreError::BadLocation) => Err(Status::new(
                Code::FailedPrecondition,
                "The princess should be in another castle",
            )),
            Err(StoreError::Internal(err)) => Err(Status::new(Code::Internal, err.message)),
        }
    }

    #[instrument(skip(self))]
    async fn check_health(
        &self,
        request: Request<HealthCheckRequest>,
    ) -> Result<Response<HealthCheckReply>, Status> {
        link_remote_span(&request);

        Ok(Response::new(HealthCheckReply {}))
    }

    #[instrument(skip(self))]
    async fn shut_down(
        &self,
        request: Request<ShutDownRequest>,
    ) -> Result<Response<ShutDownReply>, Status> {
        link_remote_span(&request);

        Arc::clone(&self.node).shut_down().await;
        Ok(Response::new(ShutDownReply {}))
    }

    #[instrument(skip(self))]
    async fn get_status(
        &self,
        request: Request<GetStatusRequest>,
    ) -> Result<Response<GetStatusReply>, Status> {
        todo!()
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

fn link_remote_span<T>(request: &Request<T>) {
    let parent_cx =
        global::get_text_map_propagator(|prop| prop.extract(&MetadataMap(request.metadata())));
    tracing::Span::current().set_parent(parent_cx);
}
