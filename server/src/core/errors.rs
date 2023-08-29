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
}

impl MetalFsError {
    pub fn is_lock_already_exists(&self) -> bool {
        if let MetalFsError::LockAlreadyExists(_) = self {
            true
        } else {
            false
        }
    }
}
