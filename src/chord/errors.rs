use std::net::SocketAddr;
use tonic::Status;

#[derive(Clone)]
pub struct InternalError {
    pub message: String,
}

pub enum GetError {
    NotFound,
    BadLocation,
    Internal(InternalError),
}

pub enum StoreError {
    BadLocation,
    Internal(InternalError),
}

pub enum ClientError {
    ConnectionFailed(ConnectionError),
    MalformedResponse(String),
    GRPCStatus(Status),
}

pub enum ClientGetError {
    NotFound,
    BadLocation,
    ConnectionFailed(ConnectionError),
    MalformedResponse(String),
    GRPCStatus(Status),
}

pub enum ClientStoreError {
    BadLocation,
    ConnectionFailed(ConnectionError),
    MalformedResponse(String),
    GRPCStatus(Status),
}

pub struct ConnectionError {
    pub addr: SocketAddr,
}
