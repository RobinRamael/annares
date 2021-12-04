use crate::peering::hash::Key;
use std::net::SocketAddr;

pub struct TransportError {
    pub tonic_error: tonic::transport::Error,
    pub message: String,
}

pub enum CommonError {
    Internal(TransportError),
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
