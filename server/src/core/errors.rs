use thiserror::Error;

error_chain! {
    errors {}
}

pub type AnyError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Error, Debug)]
pub enum MetalFsError {
    #[error("lock already exists for filename `{0}`")]
    LockAlreadyExists(String),

    #[error("lock not found for filename `{0}`")]
    LockNotFound(String),

    #[error("file metadata already exists for filename `{0}`")]
    FileMetadataAlreadyExists(String),

    #[error("file metadata not found for filename `{0}`")]
    FileMetadataNotFound(String),

    #[error("chunk {0} already exists for filename `{1}`")]
    ChunkAlreadyExists(u32, String),

    #[error("chunk {0} not found for filename `{1}`")]
    ChunkNotFound(u32, String),

    #[error("chunk metadata not found for handle `{0}`")]
    ChunkMetadataNotFound(String),
}

impl MetalFsError {
    pub fn is_lock_already_exists(&self) -> bool {
        if let MetalFsError::LockAlreadyExists(_) = self {
            true
        } else {
            false
        }
    }

    pub fn is_lock_not_found(&self) -> bool {
        if let MetalFsError::LockNotFound(_) = self {
            true
        } else {
            false
        }
    }

    pub fn is_file_already_exists(&self) -> bool {
        if let MetalFsError::FileMetadataAlreadyExists(_) = self {
            true
        } else {
            false
        }
    }

    pub fn is_file_not_found(&self) -> bool {
        if let MetalFsError::FileMetadataNotFound(_) = self {
            true
        } else {
            false
        }
    }
}
