use super::manager::StorageManager;
use crate::metalfs::{
    master_metadata_service_server::{MasterMetadataService, MasterMetadataServiceServer},
    DeleteFileRequest, OpenFileRequest, OpenFileResponse,
};
use std::sync::Arc;
use tonic::{Request, Response, Status};

pub(crate) fn make_metadata_server(_: Arc<StorageManager>) -> MasterMetadataServiceServer<Service> {
    MasterMetadataServiceServer::new(Service::new())
}

pub(crate) struct Service;

impl Service {
    fn new() -> Self {
        Service {}
    }
}

#[tonic::async_trait]
impl MasterMetadataService for Service {
    async fn open_file(
        &self,
        _: Request<OpenFileRequest>,
    ) -> Result<Response<OpenFileResponse>, Status> {
        todo!()
    }

    async fn delete_file(&self, _: Request<DeleteFileRequest>) -> Result<Response<()>, Status> {
        todo!()
    }
}
