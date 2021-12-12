use std::net::SocketAddr;
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::keys::Key;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::time::{timeout, Duration};
use tracing::*;

use crate::utils;

use crate::chord::errors::*;

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

    pub async fn join(addr: SocketAddr, peer: SocketAddr) -> Result<Self, InternalError> {
        todo!();
    }

    pub async fn find_successor(self: Arc<Self>, key: Key) -> Result<SocketAddr, InternalError> {
        todo!();
    }

    pub async fn get_predecessor(self: Arc<Self>) -> Result<Option<SocketAddr>, InternalError> {
        todo!()
    }

    pub async fn notify(self: Arc<Self>, addr: SocketAddr) -> Result<(), InternalError> {
        todo!();
    }

    pub async fn get_key(self: Arc<Self>, key: Key) -> Result<String, GetError> {
        {
            if let Some(predecessor) = self
                .acquire_predecessor_read_lock()
                .await
                .map_err(|err| GetError::Internal)
            {
                let pred_key = Key::from_addr(predecessor);
                // if pred_key.dis
            }
        }

        let store = self
            .acquire_store_read_lock()
            .await
            .map_err(|err| GetError::Internal(err))?;

        store.get(&key).ok_or(GetError::NotFound).map(String::clone)
    }

    pub async fn store_value(self: Arc<Self>, value: String) -> Result<Key, StoreError> {
        let mut store = self
            .acquire_store_write_lock()
            .await
            .map_err(|err| GetError::Internal(err))?;
    }

    fn distance_to(&self, key: &Key) -> Key {
        self.key.cyclic_distance(&key)
    }

    pub async fn shut_down(self: Arc<Self>) {
        todo!();
    }

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
            Err(elapsed) => {
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
            Err(elapsed) => {
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
            Err(elapsed) => {
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
            Err(elapsed) => {
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
            Err(elapsed) => {
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
            Err(elapsed) => {
                error!("Possible deadlock, timed out trying to acquire store write lock.");
                Err(InternalError {
                    message: String::from("Possible deadlock, timed out after 3 seconds"),
                })
            }
        }
    }
}
