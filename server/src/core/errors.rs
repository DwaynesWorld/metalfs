error_chain! {
    errors {}
}

pub type AnyError = Box<dyn std::error::Error + Send + Sync>;

pub enum MetalFsError {}
