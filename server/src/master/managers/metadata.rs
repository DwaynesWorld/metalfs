use super::locking::{InMemoryLockManager, LockManager};
use crate::core::errors::MetalFsError;
use crate::metalfs::{ChunkMetadata, FileMetadata, StorageServerLocation as Location};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::vec;

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
    file_metadatas: Arc<RwLock<HashMap<String, Arc<RwLock<FileMetadata>>>>>,
    chunk_metadatas: RwLock<HashMap<String, Arc<RwLock<ChunkMetadata>>>>,
    lease_holders: RwLock<HashMap<String, (Location, u64)>>,
    lock_manager: Arc<dyn LockManager + Sync + Send>,
}

impl DefaultMetadataManager {
    #![allow(unused)]
    pub fn new() -> Self {
        Self {
            global_chunk_id: AtomicU64::new(0),
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

        let _file_lock = file_lock.write().unwrap();

        // Create metadata object in memory
        let meta = Arc::new(RwLock::new(FileMetadata {
            filename: name.to_string(),
            chunks: HashMap::new(),
        }));

        let mut file_metadatas = self.file_metadatas.write().unwrap();
        if file_metadatas.contains_key(name) {
            return Err(MetalFsError::FileMetadataAlreadyExists(name.to_owned()));
        }

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
        let _file_lock = file_lock.write().unwrap();

        let Ok(file_metadata) = self.get_file_metadata(name) else { return; };
        let file_metadata = file_metadata.read().unwrap();

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
            let _file_lock = file_lock.write().unwrap();

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
        let _file_lock = file_lock.write().unwrap();

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
    use std::collections::HashSet;
    use std::{sync::atomic::AtomicU32, thread};

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
    fn create_duplicate_file_metadata_fails() {
        let manager = DefaultMetadataManager::new();

        let name = String::from("/foo");
        let result = manager.create_file_metadata(&name);
        assert!(result.is_ok());

        let result = manager.create_file_metadata(&name);
        assert!(result.is_err());
        assert!(result.unwrap_err().is_file_already_exists());
    }

    #[test]
    fn get_nonexisting_file_metadata_fails() {
        let manager = DefaultMetadataManager::new();

        let name = String::from("/foo");
        let result = manager.create_file_metadata(&name);
        assert!(result.is_ok());

        let result = manager.get_file_metadata(&format!("/bar"));
        assert!(result.is_err());
        assert!(result.unwrap_err().is_file_not_found());

        let result = manager.create_chunk_handle(&format!("/bar"), 0);
        assert!(result.is_err());
        assert!(result.unwrap_err().is_lock_not_found());
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
    fn create_same_file_metadata_concurrently_works() {
        let num_threads = 100;
        let mut threads = Vec::with_capacity(num_threads);
        let manager = Arc::new(DefaultMetadataManager::new());
        let count = Arc::new(AtomicU32::new(0));
        let filename = format!("/key");

        for _ in 0..num_threads {
            let mgr_clone = manager.clone();
            let cnt_clone = count.clone();
            let fname_clone = filename.clone();

            threads.push(thread::spawn(move || {
                let result = mgr_clone.create_file_metadata(&fname_clone);
                if result.is_ok() {
                    cnt_clone.fetch_add(1, Ordering::SeqCst);
                }
            }));
        }

        for thread in threads {
            _ = thread.join();
        }

        assert_eq!(count.load(Ordering::SeqCst), 1);

        let mut threads = Vec::with_capacity(num_threads);

        for i in 0..num_threads {
            let mgr_clone = manager.clone();
            let fname_clone = filename.clone();

            threads.push(thread::spawn(move || {
                let _ = mgr_clone.create_chunk_handle(&fname_clone, i as u32);
            }));
        }

        for thread in threads {
            _ = thread.join();
        }

        let result = manager.get_file_metadata(&filename);
        assert!(result.is_ok());

        let file_metadata = result.unwrap();
        assert_eq!(file_metadata.read().unwrap().filename, filename);
        assert_eq!(file_metadata.read().unwrap().chunks.len(), num_threads);

        let mut unique_handles = HashSet::new();

        for i in 0..num_threads {
            unique_handles.insert(
                file_metadata
                    .read()
                    .unwrap()
                    .chunks
                    .get(&(i as u32))
                    .unwrap()
                    .clone(),
            );
        }

        assert_eq!(unique_handles.len(), num_threads);
    }

    #[test]
    fn create_chunks_concurrently_works() {
        let manager = Arc::new(DefaultMetadataManager::new());

        let filename = format!("/key");
        let result = manager.create_file_metadata(&filename);
        assert!(result.is_ok());

        let num_threads = 100;
        let num_chunks_per_file = 77;
        let err_count = Arc::new(AtomicU32::new(0));
        let mut threads = Vec::with_capacity(num_threads);

        for _ in 0..num_threads {
            let mgr = manager.clone();
            let count = err_count.clone();
            let fname = filename.clone();

            threads.push(thread::spawn(move || {
                for i in 0..num_chunks_per_file {
                    let result = mgr.create_chunk_handle(&fname, i as u32);
                    if result.is_err() {
                        count.fetch_add(1, Ordering::SeqCst);
                    }
                }
            }));
        }

        for thread in threads {
            _ = thread.join();
        }

        assert_eq!(
            err_count.load(Ordering::SeqCst),
            ((num_threads - 1) * num_chunks_per_file) as u32
        );

        let result = manager.get_file_metadata(&filename);
        assert!(result.is_ok());

        let file_metadata = result.unwrap();
        let file_metadata = file_metadata.read().unwrap();
        assert_eq!(file_metadata.filename, filename);
        assert_eq!(file_metadata.chunks.len(), num_chunks_per_file);
    }

    #[test]
    fn create_multiple_file_metadatas_concurrently_same_parent_folder_works() {
        let num_threads = 100;
        let mut threads = Vec::with_capacity(num_threads);
        let manager = Arc::new(DefaultMetadataManager::new());

        let result = manager.create_file_metadata(&format!("/foo"));
        assert!(result.is_ok());

        for i in 0..num_threads {
            let clone = manager.clone();
            let filename = format!("/foo/{i}");

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
            let filename = format!("/foo/{i}");
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
    fn set_and_get_chunk_metadata_works() {
        let manager = Arc::new(DefaultMetadataManager::new());
        let filename = format!("/key");
        let result = manager.create_file_metadata(&filename);
        assert!(result.is_ok());

        let result = manager.create_chunk_handle(&filename, 0);
        assert!(result.is_ok());

        let handle = result.unwrap();

        let metadata = ChunkMetadata {
            handle: handle.clone(),
            version: 0,
            primary_location: Some(Location {
                hostname: "localhost".to_string(),
                port: 5000,
            }),
            locations: vec![
                Location {
                    hostname: "localhost".to_string(),
                    port: 5000,
                },
                Location {
                    hostname: "localhost".to_string(),
                    port: 5001,
                },
                Location {
                    hostname: "localhost".to_string(),
                    port: 5002,
                },
            ],
        };

        let metadata = Arc::new(RwLock::new(metadata));
        manager.set_chunk_metadata(metadata.clone());

        let result = manager.get_chunk_metadata(&handle);
        assert!(result.is_ok());

        let binding = result.unwrap();
        let metadata_ = binding.read().unwrap();
        let metadata = metadata.read().unwrap();

        assert_eq!(metadata.handle, metadata_.handle);
        assert_eq!(metadata.version, metadata_.version);

        assert_eq!(
            metadata.clone().primary_location.unwrap().hostname,
            metadata_.clone().primary_location.unwrap().hostname
        );
        assert_eq!(
            metadata.clone().primary_location.unwrap().port,
            metadata_.clone().primary_location.unwrap().port
        );
        assert_eq!(
            metadata.clone().locations[0].hostname,
            metadata_.clone().locations[0].hostname
        );
        assert_eq!(
            metadata.clone().locations[0].port,
            metadata_.clone().locations[0].port
        );
        assert_eq!(
            metadata.clone().locations[1].hostname,
            metadata_.clone().locations[1].hostname
        );
        assert_eq!(
            metadata.clone().locations[1].port,
            metadata_.clone().locations[1].port
        );
        assert_eq!(
            metadata.clone().locations[2].hostname,
            metadata_.clone().locations[2].hostname
        );
        assert_eq!(
            metadata.clone().locations[2].port,
            metadata_.clone().locations[2].port
        );
    }

    #[test]
    fn file_deletion_works() {
        let manager = Arc::new(DefaultMetadataManager::new());
        let num_threads = 100;
        let num_chunks_per_file = 77;
        let mut threads = Vec::with_capacity(num_threads);

        for i in 0..num_threads {
            let mgr = manager.clone();
            let filename = format!("/{i}");

            threads.push(thread::spawn(move || {
                for j in 0..num_chunks_per_file {
                    _ = mgr.create_file_metadata(&filename);
                    _ = mgr.create_chunk_handle(&filename, j as u32);
                }
            }));
        }

        for thread in threads {
            _ = thread.join();
        }

        let mut threads = Vec::with_capacity(num_threads);

        for i in 0..num_threads {
            let mgr = manager.clone();
            let filename = format!("/{i}");

            threads.push(thread::spawn(move || {
                _ = mgr.delete_file_and_chunk_metadata(&filename)
            }));
        }

        for thread in threads {
            _ = thread.join();
        }

        for i in 0..num_threads {
            let mgr = manager.clone();
            let filename = format!("/{i}");
            assert!(!mgr.file_metadata_exists(&filename))
        }
    }
}
