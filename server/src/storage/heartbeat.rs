pub struct HeartbeatService;

impl HeartbeatService {
    pub fn new() -> Self {
        Service {}
    }

    pub async fn start(self: Arc<Self>) -> Result<(), AnyError> {
        debug!("Starting monitoring service");

        // TODO: Perform necessary startup

        debug!("monitoring service startup complete");
        Ok(())
    }

    pub async fn stop(self: Arc<Self>) {
        debug!("Stopping monitoring service");

        // TODO: Perform necessary shutdown

        debug!("monitoring service shutdown complete");
    }
}
