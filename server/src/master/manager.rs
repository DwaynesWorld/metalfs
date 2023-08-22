use tokio::sync::RwLock;

use crate::metalfs::{FilerLocation, FilerServer};
use std::{collections::HashMap, sync::Arc};

#[derive(Debug)]
pub(crate) struct FilerManager {
    pub server_map: Arc<RwLock<HashMap<FilerLocation, FilerServer>>>,
}

impl FilerManager {
    pub fn new() -> Self {
        Self {
            server_map: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn unregister_server(&self, _: &FilerLocation) {
        todo!()
    }
}
