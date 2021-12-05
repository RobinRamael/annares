use crate::peering::hash::Key;
use std::net::SocketAddr;
use tonic::Status;

pub struct InternalError {
    pub message: String,
}

pub enum CommonError {
    Internal(InternalError),
    Status(StatusError),
    Unavailable,
}

pub enum StoreError {
    Common(CommonError),
}

pub struct NotFoundError {
    pub key: Key,
    pub originating_node: SocketAddr,
}

pub enum GetError {
    Common(CommonError),
    NotFound(NotFoundError),
}

pub enum MoveValuesError {
    Common(CommonError),
}

pub enum IntroductionError {
    Common(CommonError),
}

pub struct ConnectionFailedError {
    pub addr: SocketAddr,
    pub cause: tonic::transport::Error,
}

pub struct StatusError {
    pub addr: SocketAddr,
    pub cause: Status,
}

pub enum ClientError {
    ConnectionFailed(ConnectionFailedError),
    Status(StatusError),
    MalformedResponse,
}
