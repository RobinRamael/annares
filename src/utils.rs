#![allow(dead_code)] // for some reason rustc decides
                     // print_peers isn't used while it clearly is...

use std::collections::HashSet;
use std::net::SocketAddr;

pub fn print_peers(peers: &HashSet<SocketAddr>) {
    if peers.is_empty() {
        println!("Known peers: None")
    } else {
        println!("KNOWN PEERS: ");
        for p in peers {
            println!("   - {:?}", p);
        }
    }
}

pub fn build_grpc_url(addr: &SocketAddr) -> String {
    format!("http://{}", addr)
}

pub fn ipv6_loopback_socketaddr(port: u16) -> SocketAddr {
    SocketAddr::new(IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 0x1)), port)
}
