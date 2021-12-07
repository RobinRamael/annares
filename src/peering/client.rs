use crate::peering::errors::*;
use crate::peering::grpc::node_service_client::NodeServiceClient;
use crate::peering::grpc::*;
use crate::peering::hash::Key;
use crate::peering::utils;
use std::net::SocketAddr;
use tokio_retry::strategy::{jitter, ExponentialBackoff};
use tokio_retry::Retry;
use tonic::{Request, Status};
use tracing::error;

pub struct Client {
    grpc_client: NodeServiceClient<tonic::transport::Channel>,
}

async fn _connect(
    addr: &SocketAddr,
) -> Result<NodeServiceClient<tonic::transport::Channel>, tonic::transport::Error> {
    match NodeServiceClient::connect(utils::build_grpc_url(&addr)).await {
        Ok(res) => Ok(res),
        Err(err) => {
            warn!("Connection to {} failed, retrying...", addr);
            Err(err)
        }
    }
}

impl Client {
    pub async fn connect(addr: &SocketAddr) -> Result<Self, ClientError> {
        let retry_strategy = ExponentialBackoff::from_millis(10)
            .map(jitter) // add jitter to delays
            .take(3); // limit to 3 retries

        match Retry::spawn(retry_strategy, || _connect(addr)).await {
            Ok(grpc_client) => Ok(Client { grpc_client }),
            Err(err) => {
                error!("Connection to {} completely failed after 3 retries", addr);
                Err(ClientError::ConnectionFailed(ConnectionFailedError {
                    addr: addr.clone(),
                    cause: err,
                }))
            }
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

    pub async fn introduce(
        addr: &SocketAddr,
        sender_addr: &SocketAddr,
    ) -> Result<Vec<SocketAddr>, ClientError> {
        let mut client = Self::connect(addr).await?;

        match client
            .grpc_client
            .introduce(Request::new(IntroductionRequest {
                sender_addr: sender_addr.to_string(),
            }))
            .await
        {
            Ok(resp) => {
                let IntroductionReply { known_peers } = resp.into_inner();

                let mut addrs = vec![];
                for peer in known_peers {
                    let addr = peer.addr.parse().or(Err(ClientError::MalformedResponse))?;
                    addrs.push(addr)
                }
                Ok(addrs)
            }
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

                let parsed_holder = primary_holder
                    .parse()
                    .or(Err(ClientError::MalformedResponse))?;

                Ok((value, parsed_holder))
            }
            Err(err) => Err(to_status_err(err, addr)),
        }
    }

    pub async fn store(
        addr: &SocketAddr,
        value: String,
        corpse_addr: Option<SocketAddr>,
    ) -> Result<(Key, SocketAddr), ClientError> {
        let mut client = Self::connect(addr).await?;

        match client
            .grpc_client
            .store(Request::new(StoreRequest {
                value,
                corpse: corpse_addr.map(|p| p.to_string()),
            }))
            .await
        {
            Ok(resp) => {
                let StoreReply { key, stored_in } = resp.into_inner();

                let parsed_holder = stored_in.parse().or(Err(ClientError::MalformedResponse))?;

                let parsed_key = Key::try_from(key).or(Err(ClientError::MalformedResponse))?;

                Ok((parsed_key, parsed_holder))
            }
            Err(err) => Err(to_status_err(err, addr)),
        }
    }

    pub async fn move_values(
        addr: &SocketAddr,
        values: Vec<String>,
    ) -> Result<Vec<Key>, ClientError> {
        let mut client = Self::connect(addr).await?;

        match client
            .grpc_client
            .move_values(Request::new(MoveValuesRequest { values }))
            .await
        {
            Ok(resp) => {
                let MoveValuesReply { keys } = resp.into_inner();

                let parsed_keys = keys
                    .into_iter()
                    .map(|key| Key::try_from(key).or(Err(ClientError::MalformedResponse)))
                    .collect();

                parsed_keys
            }
            Err(err) => Err(to_status_err(err, addr)),
        }
    }

    pub async fn store_secondary(
        addr: &SocketAddr,
        value: String,
        primary_holder: &SocketAddr,
        corpse: Option<SocketAddr>,
    ) -> Result<Key, ClientError> {
        let mut client = Self::connect(addr).await?;

        match client
            .grpc_client
            .store_secondary(Request::new(SecondaryStoreRequest {
                value,
                primary_holder: primary_holder.to_string(),
                corpse: corpse.map(|p| p.to_string()),
            }))
            .await
        {
            Ok(resp) => {
                let resp = resp.into_inner();
                let key = resp
                    .key
                    .try_into()
                    .or(Err(ClientError::MalformedResponse))?;

                Ok(key)
            }
            Err(err) => Err(to_status_err(err, addr)),
        }
    }

    pub async fn get_status(
        addr: &SocketAddr,
    ) -> Result<
        (
            Vec<KeyValuePair>,
            Vec<SecondaryStoreEntry>,
            Vec<Secondant>,
            Vec<KnownPeer>,
        ),
        ClientError,
    > {
        let mut client = Self::connect(addr).await?;

        match client
            .grpc_client
            .get_status(Request::new(GetStatusRequest {}))
            .await
        {
            Ok(resp) => {
                let GetStatusReply {
                    primary_store,
                    secondary_store,
                    secondants,
                    peers,
                } = resp.into_inner();

                Ok((primary_store, secondary_store, secondants, peers))
            }
            Err(err) => Err(ClientError::Status(StatusError {
                addr: addr.clone(),
                cause: err,
            })),
        }
    }
    pub async fn shut_down(addr: &SocketAddr) -> Result<(), ClientError> {
        let mut client = Self::connect(addr).await?;

        match client
            .grpc_client
            .shut_down(Request::new(ShutDownRequest {}))
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

fn to_status_err(err: Status, addr: &SocketAddr) -> ClientError {
    ClientError::Status(StatusError {
        addr: addr.clone(),
        cause: err,
    })
}
