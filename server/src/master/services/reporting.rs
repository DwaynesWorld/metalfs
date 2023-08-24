use crate::master::managers::storage::StorageManager;
use crate::metalfs::master_reporting_service_server as mrss;
use crate::metalfs::{ReportChunkRequest, ReportChunkResponse};
use mrss::{MasterReportingService, MasterReportingServiceServer};
use std::sync::Arc;
use tonic::{Request, Response, Status};

pub(crate) fn make_reporting_server(
    _: Arc<StorageManager>,
) -> MasterReportingServiceServer<Service> {
    MasterReportingServiceServer::new(Service::new())
}

#[derive(Default)]
pub(crate) struct Service {}

impl Service {
    fn new() -> Self {
        Service {}
    }
}

#[tonic::async_trait]
impl MasterReportingService for Service {
    async fn report_chunk(
        &self,
        _: Request<ReportChunkRequest>,
    ) -> Result<Response<ReportChunkResponse>, Status> {
        todo!()
    }
}
