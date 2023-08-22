use super::manager::FilerManager;
use crate::{
    core::shutdown::Shutdown,
    metalfs::{filer_control_service_client::FilerControlServiceClient, HeartBeatRequest},
};
use std::{collections::HashMap, sync::Arc};
use tokio::{
    sync::Mutex,
    time::{interval, Duration},
};
use tonic::transport::Channel;

// Timeout for heartbeats
pub const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(30);

// Interval for heartbeat checks
pub const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);

pub(crate) fn build_and_run_monitoring_service(
    filer_mgr: Arc<FilerManager>,
) -> Arc<MonitorService> {
    let service = Arc::new(MonitorService::new(filer_mgr));
    service.clone().start();
    service
}

#[derive(Debug)]
pub(crate) struct MonitorService {
    filer_mgr: Arc<FilerManager>,
    control_clients: Mutex<HashMap<String, Arc<Mutex<FilerControlServiceClient<Channel>>>>>,
    // Synchronizer for controlled shutdown of service
    shutdown: Shutdown,
}

impl MonitorService {
    pub fn new(filer_mgr: Arc<FilerManager>) -> Self {
        MonitorService {
            filer_mgr,
            control_clients: Mutex::new(HashMap::new()),
            shutdown: Shutdown::new(),
        }
    }

    pub fn start(self: Arc<Self>) {
        debug!("Starting monitoring service...");
        tokio::spawn(async move { self.monitor().await });
    }

    pub async fn stop(self: Arc<Self>) {
        debug!("Stopping monitoring service");

        self.shutdown.begin();
        self.shutdown.wait_complete().await;

        debug!("Monitoring service shutdown complete");
    }

    pub async fn monitor(self: Arc<Self>) {
        info!("File server monitor task is now running in the background...");

        // TODO - Make these configurable, with the config manager.
        let max_attempts: u8 = 3;
        let mut interval = interval(HEARTBEAT_INTERVAL);

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let map = self.filer_mgr.server_map.read().await;

                    for (location, _) in map.iter() {
                        // Resolve address
                        let hostname = location.hostname.clone();
                        let port = location.port.clone();
                        let addr = format!("{hostname}:{port}");

                        // Get or create client
                        let client = self
                            .get_or_create_control_client(addr.clone())
                            .await
                            .unwrap();
                        let mut rpc = client.lock().await;

                        // Make rpc heartbeat request w/retry
                        info!("Sending heartbeat message to chunk server: {addr}");

                        let mut success = false;
                        for attempts in 1..max_attempts {
                            let response = rpc.heart_beat(HeartBeatRequest {}).await;

                            if response.is_ok() {
                                info!("Received heartbeat from filer server: {addr}");
                                success = true;
                                break;
                            }

                            error!(
                                "Failed to receive heartbeat from filer server: {} after {} attempt(s). Status: {}",
                                addr,
                                attempts,
                                response.err().unwrap().code());
                        }

                        // If reply isn't ok after all the attempts. We declare it as unavailable.
                        // Lets unregister this server.
                        if !success {
                            self.filer_mgr.unregister_server(location);
                        }
                    }

                    // Sleep until next cycle
                    info!("File server heartbeat monitor task is going to sleep for {} secs", HEARTBEAT_INTERVAL.as_secs());
                }

                _ = self.shutdown.wait_begin() => {
                    break;
                }
            }
        }

        self.shutdown.complete();
    }

    /// Return the protocol client for talking to the filer server at
    /// |server_address|. If the connection is already established, reuse the
    /// connection. Otherwise, initialize and return a new protocol client
    /// connecting to |server_address|.
    async fn get_or_create_control_client(
        &self,
        addr: String,
    ) -> Result<Arc<Mutex<FilerControlServiceClient<Channel>>>, Box<dyn std::error::Error>> {
        let mut map = self.control_clients.lock().await;
        if map.contains_key(&addr) {
            return Ok(map.get(&addr).unwrap().clone());
        }

        let client = Arc::new(Mutex::new(
            FilerControlServiceClient::connect(addr.clone()).await?,
        ));

        map.insert(addr, client.clone());
        Ok(client)
    }
}
