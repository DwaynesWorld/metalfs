use tokio::sync::RwLock;

use crate::metalfs::{StorageServer, StorageServerLocation};
use std::{collections::HashMap, sync::Arc};

#[derive(Debug)]
pub(crate) struct StorageManager {
    pub server_map: Arc<RwLock<HashMap<StorageServerLocation, StorageServer>>>,
}

impl StorageManager {
    pub fn new() -> Self {
        Self {
            server_map: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn unregister_server(&self, _: &StorageServerLocation) {
        todo!()
    }
}
