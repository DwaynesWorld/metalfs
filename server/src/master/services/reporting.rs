use crate::master::managers::storage::ThreadSafeStorageManager;
use crate::metalfs::master_reporting_service_server as mrss;
use crate::metalfs::{ReportStorageServerRequest, ReportStorageServerResponse};
use mrss::{MasterReportingService, MasterReportingServiceServer};
use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::{Request, Response, Status};

pub(crate) fn make_reporting_server(
    storage_mgr: Arc<ThreadSafeStorageManager>,
) -> MasterReportingServiceServer<Service> {
    MasterReportingServiceServer::new(Service::new(storage_mgr))
}

pub(crate) struct Service {
    // Storage manager
    storage_mgr: Arc<ThreadSafeStorageManager>,
}

impl Service {
    fn new(storage_mgr: Arc<ThreadSafeStorageManager>) -> Self {
        Service { storage_mgr }
    }
}

#[tonic::async_trait]
impl MasterReportingService for Service {
    async fn report_storage_server(
        &self,
        req: Request<ReportStorageServerRequest>,
    ) -> Result<Response<ReportStorageServerResponse>, Status> {
        let req = req.into_inner();

        let Some(server) = req.storage_server.clone() else {
            return Err(Status::invalid_argument("storage_server argument is required"));
        };

        let Some(location) = server.location.clone() else {
            return Err(Status::invalid_argument("server location argument is required"));
        };

        let existing = self.storage_mgr.get_server(&location).await;

        if existing.is_none() {
            self.storage_mgr
                .register_server(Arc::new(RwLock::new(server)))
                .await;

            return Ok(Response::new(ReportStorageServerResponse {
                request: Some(req),
                stale_chunk_handles: vec![],
            }));
        }

        todo!("update existing server");
    }
}
