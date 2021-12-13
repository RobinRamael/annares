use opentelemetry::{global, propagation::Injector};
use std::collections::HashMap;
use std::net::SocketAddr;
use tokio_retry::strategy::{jitter, ExponentialBackoff};
use tokio_retry::Retry;
use tonic::Code;
use tonic::Request;
use tracing::*;
use tracing_futures::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::keys::Key;

use crate::chord::errors::*;
use crate::chord::grpc::*;
use crate::utils;

pub struct Client {
    grpc_client: ChordServiceClient<tonic::transport::Channel>,
}

macro_rules! inject_span {
    ($expression:expr) => {
        global::get_text_map_propagator(|propagator| {
            propagator.inject_context(
                &tracing::Span::current().context(),
                &mut MetadataMap($expression.metadata_mut()),
            )
        });
    };
}

async fn _connect(
    addr: &SocketAddr,
) -> Result<ChordServiceClient<tonic::transport::Channel>, tonic::transport::Error> {
    match ChordServiceClient::connect(utils::build_grpc_url(&addr))
        .instrument(debug_span!("client connect"))
        .await
    {
        Ok(res) => Ok(res),
        Err(err) => {
            warn!("Connection to {} failed, retrying...", addr);
            Err(err)
        }
    }
}

#[mockall::automock]
impl Client {
    #[instrument(level = "debug")]
    async fn connect(addr: &SocketAddr) -> Result<Self, ConnectionError> {
        let retry_strategy = ExponentialBackoff::from_millis(10)
            .map(jitter) // add jitter to delays
            .take(3); // limit to 3 retries

        match Retry::spawn(retry_strategy, || _connect(addr)).await {
            Ok(grpc_client) => Ok(Client { grpc_client }),
            Err(_) => {
                warn!("Connection to {} completely failed after 3 retries", addr);
                Err(ConnectionError { addr: addr.clone() })
            }
        }
    }

    #[instrument]
    pub async fn find_successor(addr: &SocketAddr, key: Key) -> Result<SocketAddr, ClientError> {
        let mut client = Self::connect(addr)
            .await
            .map_err(ClientError::ConnectionFailed)?;

        let mut request = Request::new(FindSuccessorRequest {
            key: key.as_hex_string(),
        });

        inject_span!(&mut request);

        let resp = client
            .grpc_client
            .find_successor(request)
            .await
            .map_err(ClientError::GRPCStatus)?;

        let FindSuccessorReply { addr } = resp.into_inner();

        let parsed_addr = addr.parse().map_err(|_| {
            ClientError::MalformedResponse(format!("could not parse address {}", addr))
        })?;

        Ok(parsed_addr)
    }

    #[instrument]
    pub async fn get_predecessor(addr: &SocketAddr) -> Result<Option<SocketAddr>, ClientError> {
        let mut client = Self::connect(addr)
            .await
            .map_err(ClientError::ConnectionFailed)?;

        let mut request = Request::new(GetPredecessorRequest {});

        inject_span!(&mut request);

        let resp = client
            .grpc_client
            .get_predecessor(request)
            .await
            .map_err(ClientError::GRPCStatus)?;

        let GetPredecessorReply { addr: pred_addr } = resp.into_inner();

        match pred_addr {
            Some(a) => {
                a.parse::<SocketAddr>()
                    .map_err(|_| {
                        ClientError::MalformedResponse(format!("could not parse address {}", a))
                    })
                    .map(Some)

                // Ok(Some(x))
            }
            None => Ok(None),
        }
    }

    #[instrument]
    pub async fn notify(
        addr: &SocketAddr,
        my_addr: &SocketAddr,
    ) -> Result<Vec<String>, ClientError> {
        let mut client = Self::connect(addr)
            .await
            .map_err(ClientError::ConnectionFailed)?;

        let mut request = Request::new(NotifyRequest {
            addr: my_addr.to_string(),
        });

        inject_span!(&mut request);

        let resp = client
            .grpc_client
            .notify(request)
            .await
            .map_err(ClientError::GRPCStatus)?;

        let NotifyReply { values } = resp.into_inner();

        Ok(values)
    }

    #[instrument]
    pub async fn get_key(addr: &SocketAddr, key: Key) -> Result<String, ClientGetError> {
        let mut client = Self::connect(addr)
            .await
            .map_err(ClientGetError::ConnectionFailed)?;

        let mut request = Request::new(GetRequest {
            key: key.as_hex_string(),
        });

        inject_span!(&mut request);

        let resp = client
            .grpc_client
            .get_key(request)
            .await
            .map_err(|err| match err.code() {
                Code::NotFound => ClientGetError::NotFound,
                Code::FailedPrecondition => ClientGetError::BadLocation,
                _ => ClientGetError::GRPCStatus(err),
            })?;

        let GetReply { value } = resp.into_inner();

        Ok(value)
    }

    #[instrument]
    pub async fn store_value(addr: &SocketAddr, value: String) -> Result<Key, ClientStoreError> {
        let mut client = Self::connect(addr)
            .await
            .map_err(ClientStoreError::ConnectionFailed)?;

        let mut request = Request::new(StoreRequest { value });

        inject_span!(&mut request);

        let resp = client
            .grpc_client
            .store_value(request)
            .await
            .map_err(|err| match err.code() {
                Code::FailedPrecondition => ClientStoreError::BadLocation,
                _ => ClientStoreError::GRPCStatus(err),
            })?;

        let StoreReply { key } = resp.into_inner();

        key.clone().try_into().map_err(|_| {
            ClientStoreError::MalformedResponse(format!("could not parse key {}", key))
        })
    }

    #[instrument]
    pub async fn notify_successor_departure(
        addr: &SocketAddr,
        new_successor: &SocketAddr,
    ) -> Result<(), ClientError> {
        let mut client = Self::connect(addr)
            .await
            .map_err(ClientError::ConnectionFailed)?;

        let mut request = Request::new(SuccDepartureRequest {
            successor: new_successor.to_string(),
        });

        inject_span!(&mut request);

        client
            .grpc_client
            .successor_departure(request)
            .await
            .map_err(ClientError::GRPCStatus)?;

        Ok(())
    }

    #[instrument]
    pub async fn notify_predecessor_departure(
        addr: &SocketAddr,
        predecessor: &Option<SocketAddr>,
        values: Vec<String>,
    ) -> Result<(), ClientError> {
        let mut client = Self::connect(addr)
            .await
            .map_err(ClientError::ConnectionFailed)?;

        let mut request = Request::new(PredDepartureRequest {
            predecessor: predecessor.map(|a| a.to_string()),
            values,
        });

        inject_span!(&mut request);

        client
            .grpc_client
            .predecessor_departure(request)
            .await
            .map_err(ClientError::GRPCStatus)?;

        Ok(())
    }

    #[instrument]
    pub async fn health_check(addr: &SocketAddr) -> Result<(), ClientError> {
        let mut client = Self::connect(addr)
            .await
            .map_err(ClientError::ConnectionFailed)?;

        let mut request = Request::new(HealthCheckRequest {});

        inject_span!(&mut request);

        client
            .grpc_client
            .check_health(request)
            .await
            .map_err(ClientError::GRPCStatus)?;

        Ok(())
    }

    #[instrument]
    pub async fn shut_down(addr: &SocketAddr) -> Result<(), ClientError> {
        let mut client = Self::connect(addr)
            .await
            .map_err(ClientError::ConnectionFailed)?;

        let mut request = Request::new(ShutDownRequest {});

        inject_span!(&mut request);

        client
            .grpc_client
            .shut_down(request)
            .await
            .map_err(ClientError::GRPCStatus)?;

        Ok(())
    }

    // #[instrument]
    pub async fn get_status(
        addr: &SocketAddr,
    ) -> Result<(SocketAddr, Option<SocketAddr>, HashMap<Key, String>), ClientError> {
        let mut client = Self::connect(addr)
            .await
            .map_err(ClientError::ConnectionFailed)?;

        let mut request = Request::new(GetStatusRequest {});

        inject_span!(&mut request);

        let response = client
            .grpc_client
            .get_status(request)
            .await
            .map_err(ClientError::GRPCStatus)?;

        let GetStatusReply {
            successor,
            predecessor,
            store,
        } = response.into_inner();

        let successor = successor.parse().map_err(|_| {
            ClientError::MalformedResponse(format!("could not parse address {}", addr))
        })?;

        let predecessor = match predecessor {
            Some(a) => {
                a.parse::<SocketAddr>()
                    .map_err(|_| {
                        ClientError::MalformedResponse(format!("could not parse address {}", a))
                    })
                    .map(Some)

                // Ok(Some(x))
            }
            None => Ok(None),
        }?;

        let pairs = store
            .into_iter()
            .map(|KeyValuePair { key, value }| {
                let parsed_key: Key = key.clone().try_into().map_err(|_| {
                    ClientError::MalformedResponse(format!("could not parse key {}", key))
                })?;
                Ok((parsed_key, value))
            })
            .collect::<Result<Vec<(Key, String)>, ClientError>>()?;

        let s: HashMap<Key, String> = HashMap::from_iter(pairs.into_iter());

        Ok((successor, predecessor, s))
    }
}
struct MetadataMap<'a>(&'a mut tonic::metadata::MetadataMap);

impl<'a> Injector for MetadataMap<'a> {
    /// Set a key and value in the MetadataMap.  Does nothing if the key or value are not valid
    /// inputs
    fn set(&mut self, key: &str, value: String) {
        if let Ok(key) = tonic::metadata::MetadataKey::from_bytes(key.as_bytes()) {
            if let Ok(val) = tonic::metadata::MetadataValue::from_str(&value) {
                self.0.insert(key, val);
            }
        }
    }
}
