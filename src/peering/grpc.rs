pub mod peering_proto {
    tonic::include_proto!("peering");
}

pub use peering_proto::node_service_client::*;
pub use peering_proto::node_service_server::*;
pub use peering_proto::*;
