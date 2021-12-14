use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use tokio::time::{timeout, Duration};
use tracing::error;

#[derive(Debug)]
pub struct TimedRwLock<T> {
    lock: RwLock<T>,
    timeout: Duration,
}

pub trait LockError {
    fn new(msg: String) -> Self;
}

impl<T> TimedRwLock<T> {
    pub fn new(t: T, timeout: Duration) -> Self {
        TimedRwLock {
            lock: RwLock::new(t),
            timeout,
        }
    }

    pub async fn read<E: LockError>(&self) -> Result<RwLockReadGuard<'_, T>, E> {
        match timeout(self.timeout, self.lock.read()).await {
            Ok(guard) => Ok(guard),
            Err(_) => {
                error!("Possible deadlock, timed out trying to acquire write lock.");
                Err(E::new(
                    "Possible deadlock, timed out after 3 seconds".to_string(),
                ))
            }
        }
    }

    pub async fn write<E: LockError>(&self) -> Result<RwLockWriteGuard<'_, T>, E> {
        match timeout(Duration::from_secs(3), self.lock.write()).await {
            Ok(guard) => Ok(guard),
            Err(_) => {
                error!("Possible deadlock, timed out trying to acquire write lock.");
                Err(E::new(
                    "Possible deadlock, timed out after 3 seconds".to_string(),
                ))
            }
        }
    }
}
