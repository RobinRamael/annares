use crate::peering::errors::*;
use crate::peering::grpc::node_service_client::NodeServiceClient;
use crate::peering::grpc::*;
use crate::peering::hash::Key;
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

    pub async fn get(addr: &SocketAddr, key: &Key) -> Result<(String, SocketAddr), ClientError> {
        let mut client = Self::connect(addr).await?;

        match client
            .grpc_client
            .get(Request::new(GetRequest {
                key: key.as_hex_string(),
            }))
            .await
        {
            Ok(response) => {
                let GetReply {
                    value,
                    primary_holder,
                } = response.into_inner();

                let parsed_holder =
                    primary_holder
                        .parse()
                        .or(Err(ClientError::MalformedResponse(
                            MalformedResponseError {},
                        )))?;

                Ok((value, parsed_holder))
            }
            Err(err) => Err(ClientError::Status(StatusError {
                addr: addr.clone(),
                cause: err,
            })),
        }
    }

    pub async fn store(addr: &SocketAddr, value: String) -> Result<(Key, SocketAddr), ClientError> {
        let mut client = Self::connect(addr).await?;

        match client
            .grpc_client
            .store(Request::new(StoreRequest { value }))
            .await
        {
            Ok(resp) => {
                let StoreReply { key, stored_in } = resp.into_inner();

                let parsed_holder = stored_in.parse().or(Err(ClientError::MalformedResponse(
                    MalformedResponseError {},
                )))?;

                let parsed_key = Key::try_from(key).or(Err(ClientError::MalformedResponse(
                    MalformedResponseError {},
                )))?;

                Ok((parsed_key, parsed_holder))
            }
            Err(err) => Err(ClientError::Status(StatusError {
                addr: addr.clone(),
                cause: err,
            })),
        }
    }
}
