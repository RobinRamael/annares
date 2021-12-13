use std::net::SocketAddr;
use tonic::Status;

#[derive(Clone, Debug)]
pub struct InternalError {
    pub message: String,
}

#[derive(Debug)]
pub enum GetError {
    NotFound,
    BadLocation,
    Internal(InternalError),
}

#[derive(Debug)]
pub enum StoreError {
    BadLocation,
    Internal(InternalError),
}

#[derive(Debug)]
pub enum ClientError {
    ConnectionFailed(ConnectionError),
    MalformedResponse(String),
    GRPCStatus(Status),
}

#[derive(Debug)]
pub enum ClientGetError {
    NotFound,
    BadLocation,
    ConnectionFailed(ConnectionError),
    MalformedResponse(String),
    GRPCStatus(Status),
}

#[derive(Debug)]
pub enum ClientStoreError {
    BadLocation,
    ConnectionFailed(ConnectionError),
    MalformedResponse(String),
    GRPCStatus(Status),
}

#[derive(Debug)]
pub struct ConnectionError {
    pub addr: SocketAddr,
}
