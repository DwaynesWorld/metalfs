use crate::{
    core::errors::MetalFsError,
    metalfs::{ChunkMetadata, FileMetadata, StorageServerLocation as Location},
};
use std::{
    collections::{HashMap, HashSet},
    sync::{atomic::AtomicU64, Arc},
};

#[derive(Debug)]
pub(crate) struct MetadataManagerImpl {
    global_chunk_id: AtomicU64,
    deleted_chunk_handles: HashSet<String>,
    file_metadata: HashMap<String, Arc<FileMetadata>>,
    chunk_metadata: HashMap<String, Arc<ChunkMetadata>>,
    lease_holders: HashMap<String, (Location, u64)>,
}

impl MetadataManagerImpl {
    pub fn new() -> Self {
        Self {
            global_chunk_id: AtomicU64::new(0),
            deleted_chunk_handles: HashSet::new(),
            file_metadata: HashMap::new(),
            chunk_metadata: HashMap::new(),
            lease_holders: HashMap::new(),
        }
    }
}

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
    fn get_file_metadata(&self, name: &String) -> Result<Arc<FileMetadata>, MetalFsError>;

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
    fn get_chunk_metadata(&self, handle: &String) -> Result<ChunkMetadata, MetalFsError>;

    // Set the chunk metadata for a given chunk handle.
    fn set_chunk_metadata(&self, data: ChunkMetadata);

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
    fn get_primary_lease_metadata(&self, handle: &String) -> ((Location, u64), bool);

    /// Assign a new chunk handle. This function returns a unique chunk handle
    /// every time when it gets called.
    fn allocate_new_chunk_handle(&self) -> String;
}

impl MetadataManager for MetadataManagerImpl {
    fn create_file_metadata(&self, name: &String) -> Result<(), MetalFsError> {
        todo!()
    }

    fn file_metadata_exists(&self, name: &String) -> bool {
        todo!()
    }

    fn delete_file_and_chunk_metadata(&self, name: &String) {
        todo!()
    }

    fn get_file_metadata(&self, name: &String) -> Result<Arc<FileMetadata>, MetalFsError> {
        todo!()
    }

    fn create_chunk_handle(&self, name: &String, index: u32) -> Result<String, MetalFsError> {
        todo!()
    }

    fn get_chunk_handle(&self, name: &String, index: u32) -> Result<String, MetalFsError> {
        todo!()
    }

    fn advance_chunk_version(&self, handle: &String) -> Result<(), MetalFsError> {
        todo!()
    }

    fn chunk_metadata_exists(&self, handle: &String) -> bool {
        todo!()
    }

    fn get_chunk_metadata(&self, handle: &String) -> Result<ChunkMetadata, MetalFsError> {
        todo!()
    }

    fn set_chunk_metadata(&self, data: ChunkMetadata) {
        todo!()
    }

    fn delete_chunk_metadata(&self, handle: &String) {
        todo!()
    }

    fn set_primary_lease_metadata(&self, handle: &String, location: Location, expiration: u64) {
        todo!()
    }

    fn remove_primary_lease_metadata(&self, handle: &String) {
        todo!()
    }

    fn get_primary_lease_metadata(&self, handle: &String) -> ((Location, u64), bool) {
        todo!()
    }

    fn allocate_new_chunk_handle(&self) -> String {
        todo!()
    }
}
