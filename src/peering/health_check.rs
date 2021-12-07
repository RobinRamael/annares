use crate::peering::client::Client;
use crate::peering::this_node::ThisNode;
use std::sync::Arc;
use tokio::time::Duration;
use tracing::{debug, instrument, warn, info};

pub fn spawn_health_check(node: &Arc<ThisNode>, interval: Duration) {
    let node = Arc::clone(&node);
    tokio::task::spawn(async move { run_loop(&node, interval).await });
}

#[instrument(skip(node), fields(port=?node.addr))]
pub async fn run_loop(node: &Arc<ThisNode>, interval: Duration) -> tokio::task::JoinHandle<()> {
    let node_clone = Arc::clone(node);

    let forever = tokio::task::spawn(async move {
        let mut interval = tokio::time::interval(interval);
        loop {
            interval.tick().await;
            check_single_peer(&node_clone).await;
        }
    });
    forever
}

pub async fn check_single_peer(node: &Arc<ThisNode>) {
    let peer = match node.peers.stalest().await {
        Some(peer) => peer,
        None => {
            return;
        }
    };

    match Client::health_check(&peer.addr).await {
        Ok(()) => {
            node.mark_alive(&peer).await
        }
        Err(_) => {
            node.mark_dead(&peer).await
        }
    }
}
