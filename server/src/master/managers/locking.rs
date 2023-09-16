use crate::core::errors::MetalFsError;
use async_trait::async_trait;
use std::{collections::HashMap, path::Path, sync::Arc};
use tokio::sync::RwLock;

#[async_trait]
pub trait LockManager {
    /// Check the existence of a lock given a filename.
    async fn exist(&self, name: &String) -> bool;

    /// Create a lock for a given path, return error if the lock already exists.
    async fn create_lock(&self, name: &String) -> Result<Arc<RwLock<()>>, MetalFsError>;

    /// Retrieve a lock for a given path, return error if the lock does not exist.
    async fn fetch_lock(&self, name: &String) -> Result<Arc<RwLock<()>>, MetalFsError>;

    /// Retrieve locks for all the parent directories of a given path name.
    async fn fetch_parent_locks(&self, name: &String)
        -> Result<Vec<Arc<RwLock<()>>>, MetalFsError>;
}

pub struct InMemoryLockManager {
    locks: Arc<RwLock<HashMap<String, Arc<RwLock<()>>>>>,
}

impl InMemoryLockManager {
    pub fn new() -> Self {
        Self {
            locks: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

#[async_trait]
impl LockManager for InMemoryLockManager {
    async fn exist(&self, name: &String) -> bool {
        self.locks.read().await.contains_key(name)
    }

    async fn create_lock(&self, name: &String) -> Result<Arc<RwLock<()>>, MetalFsError> {
        let mut locks = self.locks.write().await;

        if locks.contains_key(name) {
            return Err(MetalFsError::LockAlreadyExists(name.into()));
        }

        let lock = Arc::new(RwLock::new(()));
        locks.insert(name.clone(), lock.clone());
        Ok(lock)
    }

    async fn fetch_lock(&self, name: &String) -> Result<Arc<RwLock<()>>, MetalFsError> {
        match self.locks.read().await.get(name) {
            Some(l) => Ok(l.to_owned()),
            None => Err(MetalFsError::LockNotFound(name.into())),
        }
    }

    async fn fetch_parent_locks(
        &self,
        name: &String,
    ) -> Result<Vec<Arc<RwLock<()>>>, MetalFsError> {
        let mut parent_locks = Vec::new();
        let parent_paths = Path::new(name).ancestors();

        for (i, dir) in parent_paths.enumerate() {
            if i == 0 || i == (parent_paths.count() - 1) {
                continue;
            }

            let path = String::from(dir.to_string_lossy());
            let lock = self.fetch_lock(&path).await?;
            parent_locks.push(lock);
        }

        Ok(parent_locks)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use tokio::task::JoinSet;

    #[tokio::test]
    async fn create_lock_works() {
        let manager = InMemoryLockManager::new();

        let name = String::from("/foo");
        let result = manager.create_lock(&name).await;
        assert!(result.is_ok());
        assert!(result.ok().is_some());

        let name = String::from("/foo/bar");
        let result = manager.create_lock(&name).await;
        assert!(result.is_ok());
        assert!(result.ok().is_some());

        let name = String::from("/foo/bar/baz.o");
        let result = manager.create_lock(&name).await;
        assert!(result.is_ok());
        assert!(result.ok().is_some());
    }

    #[tokio::test]
    async fn exist_works() {
        let manager = InMemoryLockManager::new();

        let name = String::from("/foo");
        _ = manager.create_lock(&name);
        let exist = manager.exist(&name).await;
        assert!(exist);

        let name = String::from("/foo/bar");
        _ = manager.create_lock(&name).await;
        let exist = manager.exist(&name).await;
        assert!(exist);
    }

    #[tokio::test]
    async fn create_multiple_locks_concurrently_works() {
        let num_threads = 100;
        let mut set = JoinSet::new();
        let manager = Arc::new(InMemoryLockManager::new());

        for i in 0..num_threads {
            let clone = manager.clone();
            set.spawn(async move {
                _ = clone.create_lock(&format!("/{i}")).await;
            });
        }

        for _ in set.join_next().await {}

        for i in 0..num_threads {
            assert!(manager.exist(&format!("/{i}")).await)
        }
    }

    #[tokio::test]
    async fn create_single_lock_concurrently_works() {
        let num_threads = 100;
        let mut set = JoinSet::new();
        let manager = Arc::new(InMemoryLockManager::new());
        let count = Arc::new(AtomicU32::new(0));

        for _ in 0..num_threads {
            let mgr_clone = manager.clone();
            let cnt_clone = count.clone();

            set.spawn(async move {
                let result = mgr_clone.create_lock(&format!("/key")).await;
                if result.is_ok() {
                    cnt_clone.fetch_add(1, Ordering::SeqCst);
                }
            });
        }

        for _ in set.join_next().await {}

        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn fetch_lock_works() {
        let manager = InMemoryLockManager::new();

        let name = String::from("/foo");
        _ = manager.create_lock(&name);
        let result = manager.fetch_lock(&name).await;
        assert!(result.is_ok());

        let name = String::from("/foo/bar");
        _ = manager.create_lock(&name);
        let result = manager.fetch_lock(&name).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn fetch_parent_locks_with_varying_path_components_works() {
        let manager = Arc::new(InMemoryLockManager::new());
        _ = manager.create_lock(&String::from("/foo"));
        _ = manager.create_lock(&String::from("/foo/bar"));
        _ = manager.create_lock(&String::from("/foo/bar/baz.o"));

        let result = manager
            .fetch_parent_locks(&String::from("/foo/bar/baz.o"))
            .await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 2);

        let result = manager
            .fetch_parent_locks(&String::from("/foo/bar/gob.o"))
            .await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 2);

        let result = manager.fetch_parent_locks(&String::from("/foo/bar")).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 1);

        let result = manager.fetch_parent_locks(&String::from("/foo")).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 0);

        let result = manager.fetch_parent_locks(&String::from("/fizz/b.o")).await;
        assert!(!result.is_ok());
    }
}
