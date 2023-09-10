#![allow(dead_code, unused_variables)]

use crate::metalfs::{StorageServer, StorageServerLocation as Location};
use async_trait::async_trait;
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::hash::Hash;
use std::sync::Arc;
use tokio::sync::RwLock;

const CHUNK_SIZE: u32 = 64;

pub type ThreadSafeStorageManager = dyn StorageManager + Sync + Send;

impl Hash for Location {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.hostname.hash(state);
        self.port.hash(state);
    }
}

#[derive(Debug)]
struct MostAvailableDisk(Arc<RwLock<StorageServer>>);

impl PartialEq for MostAvailableDisk {
    fn eq(&self, other: &Self) -> bool {
        self.0.blocking_read().available_disk_mb == other.0.blocking_read().available_disk_mb
    }
}

impl Ord for MostAvailableDisk {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0
            .blocking_read()
            .available_disk_mb
            .cmp(&other.0.blocking_read().available_disk_mb)
    }
}

impl PartialOrd for MostAvailableDisk {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.0
            .blocking_read()
            .available_disk_mb
            .partial_cmp(&other.0.blocking_read().available_disk_mb)
    }
}

impl Eq for MostAvailableDisk {}
impl Eq for StorageServer {}
impl Eq for Location {}

#[async_trait]
pub trait StorageManager {
    /// This will allocate the specified number of storage servers for storing the
    /// specified chunk. And will return the locations of the allocated servers.
    /// This is where we do some load balancing to make sure that the chunks are
    /// evenly distributed across storage servers. We prioritize storage servers with
    /// the most available disk space.
    async fn allocate_servers(
        &self,
        handle: &String,
        replication_cnt: u8,
    ) -> Arc<RwLock<HashSet<Location>>>;

    /// Register the StorageServer with the manager.
    /// Manager can now decide to select it for chunk storage.
    async fn register_server(&self, server: Arc<StorageServer>) -> bool;

    /// Unregister the StorageServer with the manager.
    /// Manager no longer knows about this server and won't be selected for chunk
    /// storage. This also removes the chunk server from the locations for
    /// previously allocated chunk handles.
    async fn unregister_server(&self, location: Location) -> bool;

    /// Returns the StorageServer for the specified location.
    async fn get_server(&self, location: &Location) -> Option<Arc<StorageServer>>;

    /// Update information about a registered storage server. We only allow updating
    /// the available disk and chunks. Location can't be updated since it uniquely
    /// identifies this storage server. To change the storage server location, it needs
    /// to unregistered and re-registered with a new location.
    async fn update_server(
        &self,
        location: &Location,
        available_disk_mb: Option<u32>,
        chunks_to_add: &HashSet<String>,
        chunks_to_remove: &HashSet<String>,
    );

    fn get_server_location_map(&self) -> Arc<RwLock<HashMap<Location, Arc<StorageServer>>>>;
}

#[derive(Debug)]
pub(crate) struct DefaultStorageManager {
    server_locations: Arc<RwLock<HashMap<Location, Arc<StorageServer>>>>,
    chunk_locations: Arc<RwLock<HashMap<String, Arc<RwLock<HashSet<Location>>>>>>,
    server_queue: Arc<RwLock<BinaryHeap<MostAvailableDisk>>>,
}

impl DefaultStorageManager {
    pub fn new() -> Self {
        Self {
            server_locations: Arc::new(RwLock::new(HashMap::new())),
            chunk_locations: Arc::new(RwLock::new(HashMap::new())),
            server_queue: Arc::new(RwLock::new(BinaryHeap::new())),
        }
    }
}

#[async_trait]
impl StorageManager for DefaultStorageManager {
    async fn allocate_servers(
        &self,
        handle: &String,
        replication_cnt: u8,
    ) -> Arc<RwLock<HashSet<Location>>> {
        let existing = self.chunk_locations.read().await;
        if existing.contains_key(handle) {
            info!("Storage servers have been previously allocated for chunk: {handle}",);
            return existing.get(handle).unwrap().clone();
        }
        drop(existing);

        let mut allocated_locations = HashSet::new();
        let mut server_queue = self.server_queue.write().await;

        for i in 0..replication_cnt {
            if server_queue.is_empty() {
                warn!("No storage servers to allocate chunks");
                break;
            }

            let item = server_queue.peek_mut().unwrap();
            let mut server = item.0.write().await;

            if server.location.is_none() {
                warn!("Found storage server with missing location");
                continue;
            }

            let new_available_disk = server.available_disk_mb - CHUNK_SIZE;
            if new_available_disk <= 0 {
                warn!(
                    "Unable to allocate all storage servers for chunk {}",
                    handle
                );
                break;
            }

            let location = server.location.as_ref().unwrap().clone();
            allocated_locations.insert(location.clone());
            server.available_disk_mb = new_available_disk;
            server.stored_chunk_handles.push(handle.clone());

            info!(
                "Allocated storage server {}:{} (new available disk={}mb) for storing chunk {}",
                location.hostname, location.port, new_available_disk, handle
            );
        }

        if allocated_locations.len() > 0 {
            let mut chunk_locations = self.chunk_locations.write().await;
            let allocated_locations = Arc::new(RwLock::new(allocated_locations));
            chunk_locations.insert(handle.to_string(), allocated_locations.clone());
            allocated_locations
        } else {
            Arc::new(RwLock::new(HashSet::new()))
        }
    }

    async fn register_server(&self, server: Arc<StorageServer>) -> bool {
        if server.location.is_none() {
            info!("Storage server is missing location");
            return false;
        }

        let location = server.location.as_ref().unwrap().clone();
        self.server_locations
            .write()
            .await
            .insert(location.clone(), server.clone());

        info!(
            "Registered storage server {}:{}",
            location.hostname, location.port
        );

        for handle in server.stored_chunk_handles.as_slice() {
            match self.chunk_locations.write().await.get(handle) {
                Some(s) => s.write().await.insert(location.clone()),
                None => false,
            };
        }

        true
    }

    async fn unregister_server(&self, location: Location) -> bool {
        todo!()
    }

    async fn get_server(&self, location: &Location) -> Option<Arc<StorageServer>> {
        match self.server_locations.read().await.get(location) {
            Some(s) => Some(s.clone()),
            None => None,
        }
    }

    async fn update_server(
        &self,
        location: &Location,
        available_disk_mb: Option<u32>,
        chunks_to_add: &HashSet<String>,
        chunks_to_remove: &HashSet<String>,
    ) {
        todo!()
    }

    fn get_server_location_map(&self) -> Arc<RwLock<HashMap<Location, Arc<StorageServer>>>> {
        self.server_locations.clone()
    }
}
