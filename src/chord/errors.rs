use crate::timed_lock::LockError;
use std::net::SocketAddr;
use tonic::Status;

#[derive(Clone, Debug)]
pub struct InternalError {
    pub message: String,
}

impl LockError for InternalError {
    fn new(message: String) -> Self {
        InternalError { message }
    }
}

#[derive(Debug)]
pub enum GetError {
    NotFound,
    BadLocation,
    Internal(InternalError),
}

#[derive(Debug)]
pub enum StoreError {
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
