use crate::master::managers::storage::StorageManager;
use crate::metalfs::master_metadata_service_server::MasterMetadataService;
use crate::metalfs::master_metadata_service_server::MasterMetadataServiceServer;
use crate::metalfs::open_file_request::OpenMode;
use crate::metalfs::{DeleteFileRequest, OpenFileRequest, OpenFileResponse};
use std::sync::Arc;
use tonic::{Request, Response, Status};

pub(crate) fn make_metadata_server(
    storage_manager: Arc<StorageManager>,
) -> MasterMetadataServiceServer<Service> {
    MasterMetadataServiceServer::new(Service::new(storage_manager))
}

pub(crate) struct Service {
    #[allow(dead_code)]
    storage_manager: Arc<StorageManager>,
}

impl Service {
    fn new(storage_manager: Arc<StorageManager>) -> Self {
        Service { storage_manager }
    }

    fn handle_file_creation(
        &self,
        r: OpenFileRequest,
    ) -> Result<Response<OpenFileResponse>, Status> {
        // Create file metadata
        let filename = r.filename;
        info!("MasterMetadataService handling file creation: {filename}");

        todo!()
    }

    fn handle_file_chunk_read(
        &self,
        _: OpenFileRequest,
    ) -> Result<Response<OpenFileResponse>, Status> {
        todo!()
    }

    fn handle_file_chunk_write(
        &self,
        _: OpenFileRequest,
    ) -> Result<Response<OpenFileResponse>, Status> {
        todo!()
    }
}

#[tonic::async_trait]
impl MasterMetadataService for Service {
    async fn open_file(
        &self,
        r: Request<OpenFileRequest>,
    ) -> Result<Response<OpenFileResponse>, Status> {
        let r = r.into_inner();

        match r.mode() {
            OpenMode::Create => self.handle_file_creation(r),
            OpenMode::Read => self.handle_file_chunk_read(r),
            OpenMode::Write => self.handle_file_chunk_write(r),
            _ => Err(Status::invalid_argument("invalid mode")),
        }
    }

    async fn delete_file(&self, _: Request<DeleteFileRequest>) -> Result<Response<()>, Status> {
        todo!()
    }
}
