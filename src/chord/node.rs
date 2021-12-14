use std::net::SocketAddr;

use std::collections::HashMap;
use std::sync::Arc;
use tokio::time::Duration;
use tracing::*;

use crate::timed_lock::TimedRwLock;

#[mockall_double::double]
use crate::chord::client::Client;

use crate::chord::errors::*;
use crate::keys::Key;

#[derive(Debug)]
pub struct ChordNode {
    addr: SocketAddr,
    key: Key,
    successor: Arc<TimedRwLock<SocketAddr>>,
    predecessor: Arc<TimedRwLock<Option<SocketAddr>>>,
    finger_map: Arc<TimedRwLock<[Option<SocketAddr>; 256]>>,
    store: Arc<TimedRwLock<HashMap<Key, String>>>,
}

impl ChordNode {
    pub fn new(addr: SocketAddr, successor: SocketAddr) -> Self {
        let lock_timeout = Duration::from_secs(3);
        ChordNode {
            addr,
            key: Key::from_addr(addr),
            successor: Arc::new(TimedRwLock::new(successor, lock_timeout)),
            predecessor: Arc::new(TimedRwLock::new(None, lock_timeout)),
            finger_map: Arc::new(TimedRwLock::new([None; 256], lock_timeout)),
            store: Arc::new(TimedRwLock::new(HashMap::new(), lock_timeout)),
        }
    }

    #[instrument]
    pub async fn join(addr: SocketAddr, peer: SocketAddr) -> Result<Self, InternalError> {
        let successor = Client::find_successor(&peer, Key::from_addr(addr))
            .await
            .map_err(|err| InternalError {
                message: format!("Failed to contact successor: {:?}", err),
            })?;

        info!(successor=?successor, "Found successor, joining network");

        Ok(ChordNode::new(addr, successor))
    }

    #[instrument]
    pub async fn find_successor(self: Arc<Self>, key: Key) -> Result<SocketAddr, InternalError> {
        let successor = self.successor.read().await?;

        if key.is_between(&self, &successor.clone()) {
            Ok(successor.clone())
        } else {
            let closest_preceding_node = Arc::clone(&self)
                .closest_preceding_node(key)
                .await?
                .unwrap_or(successor.clone());

            info!(closest=?closest_preceding_node, "Chose closest preceding node");

            Client::find_successor(&closest_preceding_node, key)
                .await
                .map_err(|err| InternalError {
                    message: format!("Failed to contact successor: {:?}", err),
                }) // TODO: retry using successor list
        }
    }

    async fn closest_preceding_node(
        self: Arc<Self>,
        key: Key,
    ) -> Result<Option<SocketAddr>, InternalError> {
        let finger_map = self.finger_map.read().await?;
        let finger = finger_map
            .into_iter()
            .rev()
            .filter_map(|addr| addr)
            .find(|addr| addr.is_between(&self, &key))
            .clone();

        Ok(finger)
    }

    #[instrument]
    async fn stabilize(self: Arc<Self>) -> Result<(), InternalError> {
        let mut successor = self.successor.write::<InternalError>().await?;

        if let Some(succ_pred) = Client::get_predecessor(&successor).await.expect("TODO") {
            info!("successors predecessor is {}", succ_pred);
            if succ_pred != self.addr && succ_pred.is_between(&self, &successor.clone()) {
                info!("{} is between {} and {}", succ_pred, self.addr, successor);
                info!(successor=?succ_pred, old_successor=?successor, "setting new successor");
                *successor = succ_pred;
            }
        } else {
            info!("Successor has no predecessor");
        }
        info!("Notifying {} of our existence", successor);
        let values_to_store = Client::notify(&successor, &self.addr).await.expect("TODO");
        for value in values_to_store {
            if let Err(err) = Arc::clone(&self).store_value(value.clone()).await {
                error!("Failed to move value {}: {:?}", &value, err);
            }
        }

        Ok(())
    }

    async fn fix_finger(self: Arc<Self>, idx: usize) -> Result<(), InternalError> {
        // n + 2^(idx)
        let finger_key = Arc::clone(&self)
            .key
            .cyclic_add(Key::two_to_the_power_of(idx));

        let finger = Arc::clone(&self).find_successor(finger_key).await?;

        let mut finger_map = self.finger_map.write::<InternalError>().await?;

        if finger_map[idx] != Some(finger) {
            info!(new_finger=%finger, idx=idx, "Setting new finger for index")
        }

        finger_map[idx] = Some(finger);

        Ok(())
    }

    #[instrument]
    pub async fn stabilization_loop(self: Arc<Self>, interval: Duration) {
        let forever = tokio::task::spawn(async move {
            let mut interval = tokio::time::interval(interval);
            let mut idxs = (0..255).cycle();

            loop {
                interval.tick().await;

                Arc::clone(&self).stabilize().await.unwrap_or_else(|err| {
                    error!("Error when stabilizing: {}", err.message);
                });

                Arc::clone(&self)
                    .fix_finger(idxs.next().unwrap())
                    .await
                    .unwrap_or_else(|err| error!("Error when fixing finger: {}", err.message));
            }
        })
        .in_current_span();
        forever.await.unwrap();
    }

    #[instrument]
    pub async fn get_predecessor(self: Arc<Self>) -> Result<Option<SocketAddr>, InternalError> {
        self.predecessor.read().await.map(|p| p.clone())
    }

    #[instrument]
    pub async fn process_notification(
        self: Arc<Self>,
        new_peer: SocketAddr,
    ) -> Result<Vec<String>, InternalError> {
        let mut predecessor = self.predecessor.write().await?;

        if predecessor.is_none() || new_peer.is_between(&predecessor.unwrap(), &self) {
            info!(predecessor=?new_peer, "setting new predecessor");
            *predecessor = Some(new_peer);
        }
        let mut data = self.store.write().await?;

        let values_to_move = data
            .clone()
            .iter()
            .filter(|(key, _)| !key.is_between(&predecessor.unwrap(), &self))
            .filter_map(|(key, _)| data.remove(key)) // this should never be None
            .collect();

        Ok(values_to_move)
    }

    #[instrument]
    pub async fn process_predecessor_departure(
        self: Arc<Self>,
        new_predecessor: Option<SocketAddr>,
        values: Vec<String>,
    ) -> Result<(), InternalError> {
        let mut predecessor = self.predecessor.write().await?;
        *predecessor = new_predecessor;

        for value in values {
            Arc::clone(&self).store_value(value).await?;
        }

        Ok(())
    }

    #[instrument]
    pub async fn process_successor_departure(
        self: Arc<Self>,
        new_successor: SocketAddr,
    ) -> Result<(), InternalError> {
        let mut successor = self.successor.write().await?;

        info!(new_successor=?new_successor, old_successor=?successor, "Setting successor");
        *successor = new_successor;

        Ok(())
    }

    #[instrument]
    pub async fn get_key(self: Arc<Self>, key: Key) -> Result<String, GetError> {
        let store = self
            .store
            .read()
            .await
            .map_err(|err| GetError::Internal(err))?;

        store.get(&key).ok_or(GetError::NotFound).map(String::clone)
    }

    #[instrument]
    pub async fn store_value(self: Arc<Self>, value: String) -> Result<Key, InternalError> {
        let mut store = self.store.write().await?;
        let key = Key::hash(&value);

        store.insert(key, value);
        Ok(key)
    }

    #[instrument]
    pub async fn shut_down(self: Arc<Self>) -> Result<(), InternalError> {
        let successor = self.successor.read().await?;
        let predecessor = self.predecessor.read().await?;
        let data = self
            .store
            .read()
            .await?
            .iter()
            // .cloned()
            .map(|(_, v)| v.clone())
            .collect::<Vec<_>>();

        Client::notify_predecessor_departure(&successor, &predecessor, data)
            .await
            .map_err(|_| InternalError {
                message: "Notifying predecessor failed".to_string(),
            })?;

        if let Some(predecessor) = predecessor.to_owned() {
            Client::notify_successor_departure(&predecessor, &successor)
                .await
                .map_err(|_| InternalError {
                    message: "notifying successor failed".to_string(),
                })?;
        }

        tokio::spawn(async {
            tokio::time::sleep(Duration::from_millis(50)).await;
            std::process::exit(0)
        });
        Ok(())
    }

    #[instrument(level = "debug")]
    pub async fn get_status(
        self: Arc<Self>,
    ) -> Result<
        (
            SocketAddr,
            Option<SocketAddr>,
            HashMap<Key, String>,
            [Option<SocketAddr>; 256],
        ),
        InternalError,
    > {
        let succ = self.successor.read().await?;
        let pred = self.predecessor.read().await?;
        let store = self.store.read().await?;
        let finger_map = self.finger_map.read().await?;

        Ok((
            succ.clone(),
            pred.clone(),
            store.clone(),
            finger_map.clone(),
        ))
    }
}

trait Locatable {
    fn key(&self) -> Key;
    fn is_between(&self, bound_1: &impl Locatable, bound_2: &impl Locatable) -> bool {
        self.key().in_cyclic_range(&bound_1.key(), &bound_2.key())
    }
}

impl Locatable for Key {
    fn key(&self) -> Key {
        self.clone()
    }
}

impl Locatable for ChordNode {
    fn key(&self) -> Key {
        self.key.clone()
    }
}

impl Locatable for Arc<ChordNode> {
    fn key(&self) -> Key {
        self.key.clone()
    }
}

impl Locatable for String {
    fn key(&self) -> Key {
        Key::hash(self)
    }
}

impl Locatable for SocketAddr {
    fn key(&self) -> Key {
        Key::from_addr(self.clone())
    }
}

mod tests {
    use super::{ChordNode, Client, InternalError, Key};
    use crate::utils::ipv6_loopback_socketaddr as a;
    use lazy_static::lazy_static;
    use std::sync::{Arc, Mutex};
    use tokio::test;

    fn h(s: &str) -> Key {
        h_s(s.to_string())
    }

    fn h_s(s: String) -> Key {
        Key::hash(&s)
    }

    fn s(s: &str) -> String {
        s.to_string()
    }

    lazy_static! {
        static ref CLIENT_MTX: Mutex<()> = Mutex::new(());
    }

    #[test]
    async fn test_find_succession_on_single_node() {
        let _m = CLIENT_MTX.lock().unwrap();

        let ctx = Client::find_successor_context();
        ctx.expect().never();

        let node = Arc::new(ChordNode::new(a(1111), a(1111)));

        let key = h("any value");

        assert_eq!(node.find_successor(key).await.unwrap(), a(1111));
    }

    #[test]
    async fn test_join() {
        let _m = CLIENT_MTX.lock().unwrap();

        let ctx = Client::find_successor_context();
        ctx.expect().return_once(|addr, key| {
            assert_eq!(addr, &a(1111));
            Ok(a(1111))
        });

        let node = Arc::new(ChordNode::new(a(1111), a(1111)));
        let other_node = ChordNode::join(a(2222), node.addr).await.unwrap();

        dbg!(Key::from_addr(a(2222)));
        dbg!(Key::from_addr(a(1111)));

        assert_eq!(
            other_node
                .successor
                .read::<InternalError>()
                .await
                .unwrap()
                .clone(),
            node.addr
        );
    }
}
