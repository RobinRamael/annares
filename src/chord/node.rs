use std::net::SocketAddr;
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use std::collections::HashMap;
use std::sync::Arc;
use tokio::time::{timeout, Duration};
use tracing::*;

#[mockall_double::double]
use crate::chord::client::Client;

use crate::chord::errors::*;
use crate::keys::Key;

#[derive(Debug)]
pub struct ChordNode {
    addr: SocketAddr,
    key: Key,
    successor: Arc<RwLock<SocketAddr>>,
    predecessor: Arc<RwLock<Option<SocketAddr>>>,
    store: Arc<RwLock<HashMap<Key, String>>>,
}

impl ChordNode {
    pub fn new(addr: SocketAddr, successor: SocketAddr) -> Self {
        ChordNode {
            addr,
            key: Key::from_addr(addr),
            successor: Arc::new(RwLock::new(successor)),
            predecessor: Arc::new(RwLock::new(None)),
            store: Arc::new(RwLock::new(HashMap::new())),
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
        let successor = self.acquire_successor_read_lock().await?;

        if key.is_between(&self, &successor.clone()) {
            Ok(successor.clone())
        } else {
            Client::find_successor(&successor, key)
                .await
                .map_err(|err| InternalError {
                    message: format!("Failed to contact successor: {:?}", err),
                }) // TODO: retry using successor list
        }
    }

    #[instrument]
    async fn stabilize(self: Arc<Self>) {
        let mut successor = self
            .acquire_successor_write_lock()
            .await
            .expect("Failed to get lock, panic!");

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
                error!("Failed to move value {}: {:?}", &value, err)
            }
        }
    }

    #[instrument]
    pub async fn stabilization_loop(self: Arc<Self>, interval: Duration) {
        let forever = tokio::task::spawn(async move {
            let mut interval = tokio::time::interval(interval);

            loop {
                interval.tick().await;
                self.clone().stabilize().await;
            }
        })
        .in_current_span();
        forever.await.unwrap();
    }

    #[instrument]
    pub async fn get_predecessor(self: Arc<Self>) -> Result<Option<SocketAddr>, InternalError> {
        self.acquire_predecessor_read_lock()
            .await
            .map(|p| p.clone())
    }

    #[instrument]
    pub async fn process_notification(
        self: Arc<Self>,
        new_peer: SocketAddr,
    ) -> Result<Vec<String>, InternalError> {
        let mut predecessor = self.acquire_predecessor_write_lock().await?;

        if predecessor.is_none() || new_peer.is_between(&predecessor.unwrap(), &self) {
            info!(predecessor=?new_peer, "setting new predecessor");
            *predecessor = Some(new_peer);
        }
        let mut data = self.acquire_store_write_lock().await?;

        let values_to_move = data
            .clone()
            .iter()
            .filter(|(key, value)| !key.is_between(&predecessor.unwrap(), &self))
            .filter_map(|(key, value)| data.remove(key)) // this should never be None
            .collect();

        Ok(values_to_move)
    }

    #[instrument]
    pub async fn process_predecessor_departure(
        self: Arc<Self>,
        new_predecessor: Option<SocketAddr>,
        values: Vec<String>,
    ) -> Result<(), InternalError> {
        let mut predecessor = self.acquire_predecessor_write_lock().await?;
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
        let mut successor = self.acquire_successor_write_lock().await?;

        info!(new_successor=?new_successor, old_successor=?successor, "Setting successor");
        *successor = new_successor;

        Ok(())
    }

    async fn is_not_ours(&self, key: Key) -> Result<bool, InternalError> {
        Ok(!self
            .acquire_predecessor_read_lock()
            .await?
            .map_or_else(|| false, |predecessor| key.is_between(&predecessor, self)))
    }

    #[instrument]
    pub async fn get_key(self: Arc<Self>, key: Key) -> Result<String, GetError> {
        let store = self
            .acquire_store_read_lock()
            .await
            .map_err(|err| GetError::Internal(err))?;

        store.get(&key).ok_or(GetError::NotFound).map(String::clone)
    }

    #[instrument]
    pub async fn store_value(self: Arc<Self>, value: String) -> Result<Key, InternalError> {
        let mut store = self.acquire_store_write_lock().await?;
        let key = Key::hash(&value);

        store.insert(key, value);
        Ok(key)
    }

    #[instrument]
    pub async fn shut_down(self: Arc<Self>) -> Result<(), InternalError> {
        let successor = self.acquire_successor_read_lock().await?;
        let predecessor = self.acquire_predecessor_read_lock().await?;
        let data = self
            .acquire_store_read_lock()
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
    ) -> Result<(SocketAddr, Option<SocketAddr>, HashMap<Key, String>), InternalError> {
        let succ = self.acquire_successor_read_lock().await?;
        let pred = self.acquire_predecessor_read_lock().await?;
        let store = self.acquire_store_read_lock().await?;

        Ok((succ.clone(), pred.clone(), store.clone()))
    }

    async fn acquire_predecessor_read_lock(
        &self,
    ) -> Result<RwLockReadGuard<'_, Option<SocketAddr>>, InternalError> {
        match timeout(Duration::from_secs(3), self.predecessor.read()).await {
            Ok(guard) => Ok(guard),
            Err(_) => {
                error!("Possible deadlock, timed out trying to acquire predecessor read lock");
                Err(InternalError {
                    message: String::from("Possible deadlock, timed out after 3 seconds"),
                })
            }
        }
    }

    async fn acquire_predecessor_write_lock(
        &self,
    ) -> Result<RwLockWriteGuard<'_, Option<SocketAddr>>, InternalError> {
        match timeout(Duration::from_secs(3), self.predecessor.write()).await {
            Ok(guard) => Ok(guard),
            Err(_) => {
                error!("Possible deadlock, timed out trying to acquire predecessor write lock.");
                Err(InternalError {
                    message: String::from("Possible deadlock, timed out after 3 seconds"),
                })
            }
        }
    }

    async fn acquire_successor_read_lock(
        &self,
    ) -> Result<RwLockReadGuard<'_, SocketAddr>, InternalError> {
        match timeout(Duration::from_secs(3), self.successor.read()).await {
            Ok(guard) => Ok(guard),
            Err(_) => {
                error!("Possible deadlock, timed out trying to acquire successor read lock");
                Err(InternalError {
                    message: String::from("Possible deadlock, timed out after 3 seconds"),
                })
            }
        }
    }

    async fn acquire_successor_write_lock(
        &self,
    ) -> Result<RwLockWriteGuard<'_, SocketAddr>, InternalError> {
        match timeout(Duration::from_secs(3), self.successor.write()).await {
            Ok(guard) => Ok(guard),
            Err(_) => {
                error!("Possible deadlock, timed out trying to acquire successor write lock.");
                Err(InternalError {
                    message: String::from("Possible deadlock, timed out after 3 seconds"),
                })
            }
        }
    }

    async fn acquire_store_read_lock(
        &self,
    ) -> Result<RwLockReadGuard<'_, HashMap<Key, String>>, InternalError> {
        match timeout(Duration::from_secs(3), self.store.read()).await {
            Ok(guard) => Ok(guard),
            Err(_) => {
                error!("Possible deadlock, timed out trying to acquire store read lock");
                Err(InternalError {
                    message: String::from("Possible deadlock, timed out after 3 seconds"),
                })
            }
        }
    }

    async fn acquire_store_write_lock(
        &self,
    ) -> Result<RwLockWriteGuard<'_, HashMap<Key, String>>, InternalError> {
        match timeout(Duration::from_secs(3), self.store.write()).await {
            Ok(guard) => Ok(guard),
            Err(_) => {
                error!("Possible deadlock, timed out trying to acquire store write lock.");
                Err(InternalError {
                    message: String::from("Possible deadlock, timed out after 3 seconds"),
                })
            }
        }
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
    use super::{ChordNode, Client, Key};
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
            dbg!(key);
            Ok(a(1111))
        });

        let node = Arc::new(ChordNode::new(a(1111), a(1111)));
        let other_node = ChordNode::join(a(2222), node.addr).await.unwrap();

        dbg!(Key::from_addr(a(2222)));
        dbg!(Key::from_addr(a(1111)));

        assert_eq!(
            other_node
                .acquire_successor_read_lock()
                .await
                .unwrap()
                .clone(),
            node.addr
        );
    }
}
