use crate::peering::client::Client;
use crate::peering::this_node::{OtherNode, ThisNode};
use std::sync::Arc;
use strum::IntoEnumIterator;
use strum_macros::EnumIter;
use tokio::time::Duration;
use tokio::time::Instant;
use tracing::*;

pub fn spawn_health_check(node: &Arc<ThisNode>, interval: Duration) {
    let node = Arc::clone(&node);
    tokio::task::spawn(async move { run_loop(&node, interval).await });
}

#[derive(EnumIter)]
enum PeerRoles {
    PrimaryHolder, // we hold secondary data for them
    Secondant,     //  they hold secondary for use
    Peer,          // we just friends aight
}

#[instrument(skip(node), fields(port=?node.addr))]
pub async fn run_loop(node: &Arc<ThisNode>, interval: Duration) -> tokio::task::JoinHandle<()> {
    let node = Arc::clone(node);

    let forever = tokio::task::spawn(async move {
        let mut interval = tokio::time::interval(interval);

        let mut enum_iter = PeerRoles::iter().cycle();

        loop {
            interval.tick().await;

            if let Some(role) = enum_iter.next() {
                if let Some(next) = pick_next(&node, role).await {
                    match Client::health_check(&next.addr).await {
                        Ok(()) => node.mark_alive(&next).await,
                        Err(_) => node.mark_dead(&next).await,
                    }
                }
            }
        }
    });
    forever
}

async fn pick_next(node: &Arc<ThisNode>, role: PeerRoles) -> Option<OtherNode> {
    let known_peers = node.peers.known_peers.read().await;
    match role {
        PeerRoles::PrimaryHolder => {
            let secondants = node.secondants.read().await;
            //
            let mut ps = vec![];

            for peer in secondants.values() {
                if let Some(p) = known_peers.get(&peer.addr) {
                    ps.push(p.clone());
                }
            }
            ps.iter()
                .max_by_key(|p| Instant::now() - p.last_seen)
                .cloned()
        }
        PeerRoles::Secondant => {
            let secondary_data = node.secondary_store.read().await;
            //
            let mut ps = vec![];

            for addr in secondary_data.keys() {
                if let Some(p) = known_peers.get(&addr) {
                    ps.push(p.clone());
                }
            }
            ps.iter()
                .max_by_key(|p| Instant::now() - p.last_seen)
                .cloned()
        }
        PeerRoles::Peer => {
            let known_peers = node.peers.known_peers.read().await;

            known_peers
                .values()
                .max_by_key(|p| Instant::now() - p.last_seen)
                .cloned()
        }
    }
}
