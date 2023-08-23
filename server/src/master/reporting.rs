use super::manager::StorageManager;
use crate::metalfs::{
    master_reporting_service_server::{MasterReportingService, MasterReportingServiceServer},
    ReportChunkRequest, ReportChunkResponse,
};
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
