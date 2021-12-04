use crate::peering::errors::*;
use crate::peering::grpc::node_service_client::NodeServiceClient;
use crate::peering::grpc::*;
use crate::peering::utils;
use std::net::SocketAddr;
use tonic::{Request, Response};

pub struct Client {
    grpc_client: NodeServiceClient<tonic::transport::Channel>,
}

impl Client {
    pub async fn connect(addr: &SocketAddr) -> Result<Self, ClientError> {
        match NodeServiceClient::connect(utils::build_grpc_url(&addr)).await {
            Ok(grpc_client) => Ok(Client { grpc_client }),
            Err(err) => Err(ClientError::ConnectionFailed(ConnectionFailedError {
                addr: addr.clone(),
                cause: err,
            })),
        }
    }

    pub async fn health_check(addr: &SocketAddr) -> Result<(), ClientError> {
        let mut client = Self::connect(addr).await?;

        match client
            .grpc_client
            .check_health(Request::new(HealthCheckRequest {}))
            .await
        {
            Ok(_) => Ok(()),
            Err(err) => Err(ClientError::Status(StatusError {
                addr: addr.clone(),
                cause: err,
            })),
        }
    }
}
