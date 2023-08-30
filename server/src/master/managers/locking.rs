use crate::core::errors::MetalFsError;
use std::{
    collections::HashMap,
    path::Path,
    sync::{Arc, RwLock},
};

pub trait LockManager {
    /// Check the existence of a lock given a filename.
    fn exist(&self, name: &String) -> bool;

    /// Create a lock for a given path, return error if the lock already exists.
    fn create_lock(&self, name: &String) -> Result<Arc<RwLock<()>>, MetalFsError>;

    /// Retrieve a lock for a given path, return error if the lock does not exist.
    fn fetch_lock(&self, name: &String) -> Result<Arc<RwLock<()>>, MetalFsError>;

    /// Retrieve locks for all the parent directories of a given path name.
    fn fetch_parent_locks(&self, name: &String) -> Result<Vec<Arc<RwLock<()>>>, MetalFsError>;
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

impl LockManager for InMemoryLockManager {
    fn exist(&self, name: &String) -> bool {
        self.locks.read().unwrap().contains_key(name)
    }

    fn create_lock(&self, name: &String) -> Result<Arc<RwLock<()>>, MetalFsError> {
        let mut locks = self.locks.write().unwrap();

        if locks.contains_key(name) {
            return Err(MetalFsError::LockAlreadyExists(name.into()));
        }

        let lock = Arc::new(RwLock::new(()));
        locks.insert(name.clone(), lock.clone());
        Ok(lock)
    }

    fn fetch_lock(&self, name: &String) -> Result<Arc<RwLock<()>>, MetalFsError> {
        match self.locks.read().unwrap().get(name) {
            Some(l) => Ok(l.to_owned()),
            None => Err(MetalFsError::LockNotFound(name.into())),
        }
    }

    fn fetch_parent_locks(&self, name: &String) -> Result<Vec<Arc<RwLock<()>>>, MetalFsError> {
        let mut parent_locks = Vec::new();
        let parent_paths = Path::new(name).ancestors();

        for (i, dir) in parent_paths.enumerate() {
            if i == 0 || i == (parent_paths.count() - 1) {
                continue;
            }

            let path = String::from(dir.to_string_lossy());
            let lock = self.fetch_lock(&path)?;
            parent_locks.push(lock);
        }

        Ok(parent_locks)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        sync::atomic::{AtomicU32, Ordering},
        thread,
    };

    #[test]
    fn create_lock_works() {
        let manager = InMemoryLockManager::new();

        let name = String::from("/foo");
        let result = manager.create_lock(&name);
        assert!(result.is_ok());
        assert!(result.ok().is_some());

        let name = String::from("/foo/bar");
        let result = manager.create_lock(&name);
        assert!(result.is_ok());
        assert!(result.ok().is_some());

        let name = String::from("/foo/bar/baz.o");
        let result = manager.create_lock(&name);
        assert!(result.is_ok());
        assert!(result.ok().is_some());
    }

    #[test]
    fn exist_works() {
        let manager = InMemoryLockManager::new();

        let name = String::from("/foo");
        _ = manager.create_lock(&name);
        let exist = manager.exist(&name);
        assert!(exist);

        let name = String::from("/foo/bar");
        _ = manager.create_lock(&name);
        let exist = manager.exist(&name);
        assert!(exist);
    }

    #[test]
    fn create_multiple_locks_concurrently_works() {
        let num_threads = 10;
        let mut threads = Vec::with_capacity(num_threads);
        let manager = Arc::new(InMemoryLockManager::new());

        for i in 0..num_threads {
            let clone = manager.clone();
            threads.push(thread::spawn(move || {
                _ = clone.create_lock(&format!("/{i}"));
            }));
        }

        for thread in threads {
            _ = thread.join();
        }

        for i in 0..num_threads {
            assert!(manager.exist(&format!("/{i}")))
        }
    }

    #[test]
    fn create_single_lock_concurrently_works() {
        let num_threads = 10;
        let mut threads = Vec::with_capacity(num_threads);
        let manager = Arc::new(InMemoryLockManager::new());
        let count = Arc::new(AtomicU32::new(0));

        for _ in 0..num_threads {
            let mgr_clone = manager.clone();
            let cnt_clone = count.clone();

            threads.push(thread::spawn(move || {
                let result = mgr_clone.create_lock(&format!("/key"));
                if result.is_ok() {
                    cnt_clone.fetch_add(1, Ordering::SeqCst);
                }
            }));
        }

        for thread in threads {
            _ = thread.join();
        }

        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn fetch_lock_works() {
        let manager = InMemoryLockManager::new();

        let name = String::from("/foo");
        _ = manager.create_lock(&name);
        let result = manager.fetch_lock(&name);
        assert!(result.is_ok());

        let name = String::from("/foo/bar");
        _ = manager.create_lock(&name);
        let result = manager.fetch_lock(&name);
        assert!(result.is_ok());
    }

    #[test]
    fn fetch_parent_locks_with_varying_path_components_works() {
        let manager = Arc::new(InMemoryLockManager::new());
        _ = manager.create_lock(&String::from("/foo"));
        _ = manager.create_lock(&String::from("/foo/bar"));
        _ = manager.create_lock(&String::from("/foo/bar/baz.o"));

        let result = manager.fetch_parent_locks(&String::from("/foo/bar/baz.o"));
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 2);

        let result = manager.fetch_parent_locks(&String::from("/foo/bar/gob.o"));
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 2);

        let result = manager.fetch_parent_locks(&String::from("/foo/bar"));
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 1);

        let result = manager.fetch_parent_locks(&String::from("/foo"));
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 0);

        let result = manager.fetch_parent_locks(&String::from("/fizz/b.o"));
        assert!(!result.is_ok());
    }
}
