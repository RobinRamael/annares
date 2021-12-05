use crate::peering::hash::Key;
use std::net::SocketAddr;
use tonic::Status;

#[derive(Debug)]
pub struct InternalError {
    pub message: String,
}

#[derive(Debug)]
pub enum CommonError {
    Internal(InternalError),
    Status(StatusError),
    Unavailable,
}

#[derive(Debug)]
pub enum StoreError {
    Common(CommonError),
}

#[derive(Debug)]
pub struct NotFoundError {
    pub key: Key,
    pub originating_node: SocketAddr,
}

#[derive(Debug)]
pub enum GetError {
    Common(CommonError),
    NotFound(NotFoundError),
}

#[derive(Debug)]
pub enum MoveValuesError {
    Common(CommonError),
}

#[derive(Debug)]
pub enum IntroductionError {
    Common(CommonError),
}

#[derive(Debug)]
pub struct ConnectionFailedError {
    pub addr: SocketAddr,
    pub cause: tonic::transport::Error,
}

#[derive(Debug)]
pub struct StatusError {
    pub addr: SocketAddr,
    pub cause: Status,
}

#[derive(Debug)]
pub enum ClientError {
    ConnectionFailed(ConnectionFailedError),
    Status(StatusError),
    MalformedResponse,
}
