use std::net::{IpAddr, Ipv6Addr, SocketAddr};

pub fn build_grpc_url(addr: &SocketAddr) -> String {
    format!("http://{}", addr)
}

pub fn ipv6_loopback_socketaddr(port: u16) -> SocketAddr {
    SocketAddr::new(IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 0x1)), port)
}
