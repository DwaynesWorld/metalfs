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

        for dir in Path::new(name).ancestors().skip(1) {
            let path = String::from(dir.to_string_lossy());
            let lock = self.fetch_lock(&path)?;
            parent_locks.push(lock);
        }

        Ok(parent_locks)
    }
}
