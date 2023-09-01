#![allow(unused)]
use tokio::runtime::Handle;

use crate::{
    core::errors::MetalFsError,
    metalfs::{ChunkMetadata, FileMetadata, StorageServerLocation as Location},
};
use std::{
    collections::{HashMap, HashSet},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, RwLock,
    },
    vec,
};

use super::locking::{InMemoryLockManager, LockManager};

pub trait MetadataManager {
    /// Create the file metadata (and a lock associated with this file) for a
    /// given file path. This function returns error if the file path already
    /// exists or if any of the intermediate parent directory not found.
    fn create_file_metadata(&self, name: &String) -> Result<(), MetalFsError>;

    /// Check if metadata file exists.
    fn file_metadata_exists(&self, name: &String) -> bool;

    /// Delete a file metadata, and delete all chunk handles associated with
    /// this file.
    fn delete_file_and_chunk_metadata(&self, name: &String);

    /// Access the file metadata for a given file path. The caller of this
    /// function needs to ensure the lock for this file is properly used.
    /// return error if fileMetadata not found.
    fn get_file_metadata(&self, name: &String) -> Result<Arc<RwLock<FileMetadata>>, MetalFsError>;

    /// Create a file chunk for a given filename and a chunk index.
    fn create_chunk_handle(&self, name: &String, index: u32) -> Result<String, MetalFsError>;

    /// Retrieve a chunk handle for a given filename and chunk index. Return
    /// error if filename or chunk not found.
    fn get_chunk_handle(&self, name: &String, index: u32) -> Result<String, MetalFsError>;

    /// Advance the chunk version number for a chunk handle, return error if
    /// chunk handle not found.
    fn advance_chunk_version(&self, handle: &String) -> Result<(), MetalFsError>;

    /// Check whether chunk metadata exists.
    fn chunk_metadata_exists(&self, handle: &String) -> bool;

    /// Get the chunk metadata for a given chunk handle, return error if
    /// chunk handle not found.
    fn get_chunk_metadata(
        &self,
        handle: &String,
    ) -> Result<Arc<RwLock<ChunkMetadata>>, MetalFsError>;

    // Set the chunk metadata for a given chunk handle.
    fn set_chunk_metadata(&self, data: Arc<RwLock<ChunkMetadata>>);

    // Delete the chunk metadata for a given chunk handle.
    fn delete_chunk_metadata(&self, handle: &String);

    /// Set the primary chunk location that holds the lease for a given chunk
    /// handle, and its lease expiration time.
    fn set_primary_lease_metadata(&self, handle: &String, location: Location, expiration: u64);

    /// Unset the primary chunk location that holds the lease for a given chunk
    /// handle; this happens when a lease expires / gets revoked.
    fn remove_primary_lease_metadata(&self, handle: &String);

    /// Return the server location that last held the lease for the handle,
    /// which may or may not be expired; it's up to caller to check the expiration.
    fn get_primary_lease_metadata(&self, handle: &String) -> Option<(Location, u64)>;

    /// Assign a new chunk handle. This function returns a unique chunk handle
    /// every time when it gets called.
    fn allocate_new_chunk_handle(&self) -> String;
}

pub(crate) struct DefaultMetadataManager {
    global_chunk_id: AtomicU64,
    deleted_chunk_handles: HashSet<String>,
    file_metadatas: Arc<RwLock<HashMap<String, Arc<RwLock<FileMetadata>>>>>,
    chunk_metadatas: RwLock<HashMap<String, Arc<RwLock<ChunkMetadata>>>>,
    lease_holders: RwLock<HashMap<String, (Location, u64)>>,
    lock_manager: Arc<dyn LockManager + Sync + Send>,
}

impl DefaultMetadataManager {
    pub fn new() -> Self {
        Self {
            global_chunk_id: AtomicU64::new(0),
            deleted_chunk_handles: HashSet::new(),
            file_metadatas: Arc::new(RwLock::new(HashMap::new())),
            chunk_metadatas: RwLock::new(HashMap::new()),
            lease_holders: RwLock::new(HashMap::new()),
            lock_manager: Arc::new(InMemoryLockManager::new()),
        }
    }
}

impl MetadataManager for DefaultMetadataManager {
    fn create_file_metadata(&self, name: &String) -> Result<(), MetalFsError> {
        // Lock all parent directories
        let parent_locks = self.lock_manager.fetch_parent_locks(name)?;
        let mut parent_lock_guards = Vec::new();

        parent_locks.iter().for_each(|pl| {
            parent_lock_guards.push(pl.read().unwrap());
        });

        // Create lock for file metadata
        let file_lock;
        let result = self.lock_manager.create_lock(name);

        if result.is_ok() {
            file_lock = result.unwrap();
        } else {
            let err = result.unwrap_err();

            if !err.is_lock_already_exists() {
                return Err(err);
            }

            file_lock = self.lock_manager.fetch_lock(name)?;
        }

        let file_lock = file_lock.write().unwrap();

        // Create metadata object in memory
        let meta = Arc::new(RwLock::new(FileMetadata {
            filename: name.to_string(),
            chunks: HashMap::new(),
        }));

        let mut file_metadatas = self.file_metadatas.write().unwrap();
        file_metadatas.insert(name.to_owned(), meta);

        Ok(())
    }

    fn file_metadata_exists(&self, name: &String) -> bool {
        let file_metadatas = self.file_metadatas.read().unwrap();
        file_metadatas.contains_key(name)
    }

    fn delete_file_and_chunk_metadata(&self, name: &String) {
        let Ok(parent_locks) = self.lock_manager.fetch_parent_locks(name) else { return; };
        let mut parent_lock_guards = Vec::new();

        parent_locks.iter().for_each(|pl| {
            parent_lock_guards.push(pl.read().unwrap());
        });

        let Ok(file_lock) = self.lock_manager.fetch_lock(name) else { return; };
        let file_lock = file_lock.write().unwrap();

        let Ok(file_metadata) = self.get_file_metadata(name) else { return; };
        let mut file_metadata = file_metadata.write().unwrap();

        let mut file_metadatas = self.file_metadatas.write().unwrap();
        file_metadatas.remove(name);

        for (_, handle) in file_metadata.chunks.iter() {
            self.delete_chunk_metadata(handle);
        }
    }

    fn get_file_metadata(&self, name: &String) -> Result<Arc<RwLock<FileMetadata>>, MetalFsError> {
        let file_metadatas = self.file_metadatas.read().unwrap();
        match file_metadatas.get(name) {
            Some(m) => Ok(m.clone()),
            None => Err(MetalFsError::FileMetadataNotFound(name.to_owned())),
        }
    }

    fn create_chunk_handle(&self, name: &String, index: u32) -> Result<String, MetalFsError> {
        let handle = self.allocate_new_chunk_handle();

        {
            let parent_locks = self.lock_manager.fetch_parent_locks(name)?;
            let mut parent_lock_guards = Vec::new();

            parent_locks.iter().for_each(|pl| {
                parent_lock_guards.push(pl.read().unwrap());
            });

            let file_lock = self.lock_manager.fetch_lock(name)?;
            let file_lock = file_lock.write().unwrap();

            let file_metadata = self.get_file_metadata(name)?;
            let mut file_metadata = file_metadata.write().unwrap();

            file_metadata.filename = name.to_owned();
            if file_metadata.chunks.contains_key(&index) {
                return Err(MetalFsError::ChunkAlreadyExists(index, name.to_owned()));
            }

            file_metadata.chunks.insert(index, handle.clone());
        }

        let chunk_metadata = ChunkMetadata {
            handle: handle.clone(),
            version: 0,
            primary_location: None,
            locations: vec![],
        };

        self.set_chunk_metadata(Arc::new(RwLock::new(chunk_metadata)));

        Ok(handle)
    }

    fn get_chunk_handle(&self, name: &String, index: u32) -> Result<String, MetalFsError> {
        let parent_locks = self.lock_manager.fetch_parent_locks(name)?;
        let mut parent_lock_guards = Vec::new();

        parent_locks.iter().for_each(|pl| {
            parent_lock_guards.push(pl.read().unwrap());
        });

        let file_lock = self.lock_manager.fetch_lock(name)?;
        let file_lock = file_lock.write().unwrap();

        let file_metadata = self.get_file_metadata(name)?;
        let file_metadata = file_metadata.read().unwrap();
        match file_metadata.chunks.get(&index) {
            Some(m) => Ok(m.clone()),
            None => Err(MetalFsError::ChunkNotFound(index, name.clone())),
        }
    }

    fn advance_chunk_version(&self, handle: &String) -> Result<(), MetalFsError> {
        let chunk_metadata = self.get_chunk_metadata(handle)?;

        let mut chunk_metadata_ = chunk_metadata.write().unwrap();
        chunk_metadata_.version += 1;
        drop(chunk_metadata_);

        self.set_chunk_metadata(chunk_metadata.clone());
        Ok(())
    }

    fn chunk_metadata_exists(&self, handle: &String) -> bool {
        let chunk_metadatas = self.chunk_metadatas.read().unwrap();
        chunk_metadatas.contains_key(handle)
    }

    fn get_chunk_metadata(
        &self,
        handle: &String,
    ) -> Result<Arc<RwLock<ChunkMetadata>>, MetalFsError> {
        let chunk_metadatas = self.chunk_metadatas.read().unwrap();
        match chunk_metadatas.get(handle) {
            Some(m) => Ok(m.clone()),
            None => Err(MetalFsError::ChunkMetadataNotFound(handle.to_owned())),
        }
    }

    fn set_chunk_metadata(&self, data: Arc<RwLock<ChunkMetadata>>) {
        let handle = data.read().unwrap().handle.clone();
        let mut chunk_metadatas = self.chunk_metadatas.write().unwrap();
        chunk_metadatas.insert(handle, data);
    }

    fn delete_chunk_metadata(&self, handle: &String) {
        let mut chunk_metadatas = self.chunk_metadatas.write().unwrap();
        chunk_metadatas.remove(handle);
    }

    fn set_primary_lease_metadata(&self, handle: &String, location: Location, expiration: u64) {
        let mut lease_holders = self.lease_holders.write().unwrap();
        lease_holders.insert(handle.clone(), (location, expiration));
    }

    fn remove_primary_lease_metadata(&self, handle: &String) {
        let mut lease_holders = self.lease_holders.write().unwrap();
        lease_holders.remove(handle);
    }

    fn get_primary_lease_metadata(&self, handle: &String) -> Option<(Location, u64)> {
        let lease_holders = self.lease_holders.read().unwrap();
        match lease_holders.get(handle) {
            Some(m) => Some(m.clone()),
            None => None,
        }
    }

    fn allocate_new_chunk_handle(&self) -> String {
        self.global_chunk_id
            .fetch_add(1u64, Ordering::SeqCst)
            .to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn create_single_file_metadata_works() {
        let manager = DefaultMetadataManager::new();

        let name = String::from("/foo");
        let result = manager.create_file_metadata(&name);
        assert!(result.is_ok());
        assert!(manager.file_metadata_exists(&name));

        let result = manager.get_file_metadata(&name);
        assert!(result.is_ok());

        let file_metadata = result.unwrap();
        assert_eq!(file_metadata.read().unwrap().filename, name);

        let result = manager.create_chunk_handle(&name, 0);
        assert!(result.is_ok());

        let handle = result.unwrap();
        assert_eq!(handle, String::from("0"));
        assert_eq!(file_metadata.read().unwrap().chunks.len(), 1);
    }

    #[test]
    fn create_multiple_file_metadatas_concurrently_works() {
        let num_threads = 100;
        let mut threads = Vec::with_capacity(num_threads);
        let manager = Arc::new(DefaultMetadataManager::new());

        for i in 0..num_threads {
            let clone = manager.clone();
            let filename = format!("/{i}");

            threads.push(thread::spawn(move || {
                _ = clone.create_file_metadata(&filename);
                _ = clone.create_chunk_handle(&filename, 0);
            }));
        }

        for thread in threads {
            _ = thread.join();
        }

        let mut unique_handles = HashSet::new();

        for i in 0..num_threads {
            let filename = format!("/{i}");
            assert!(manager.file_metadata_exists(&filename));

            let result = manager.get_file_metadata(&filename);
            assert!(result.is_ok());

            let file_metadata = result.unwrap();
            assert_eq!(file_metadata.read().unwrap().filename, filename);
            assert_eq!(file_metadata.read().unwrap().chunks.len(), 1);

            unique_handles.insert(
                file_metadata
                    .read()
                    .unwrap()
                    .chunks
                    .get(&0)
                    .unwrap()
                    .clone(),
            );
        }

        assert_eq!(unique_handles.len(), num_threads);
    }

    #[test]
    fn create_single_file_metadata_concurrently_works() {}
}
