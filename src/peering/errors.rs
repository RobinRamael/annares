use crate::peering::hash::Key;
use std::net::SocketAddr;
use tonic::Status;

pub struct TransportError {
    pub cause: tonic::transport::Error,
    pub message: String,
}

pub struct InternalError {
    pub message: String,
}

pub enum CommonError {
    Transport(TransportError),
    Internal(InternalError),
    Status(StatusError),
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

pub struct MalformedResponseError {}

pub enum ClientError {
    ConnectionFailed(ConnectionFailedError),
    Status(StatusError),
    MalformedResponse(MalformedResponseError),
}

