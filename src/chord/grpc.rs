pub mod chord_proto {
    tonic::include_proto!("chord");
}

pub use chord_proto::chord_service_client::*;
pub use chord_proto::chord_service_server::*;
pub use chord_proto::*;
