#[macro_use]
extern crate error_chain;

#[macro_use]
pub mod macros;

pub mod errors;
pub mod id;
pub mod logger;
pub mod version;

pub const BANNER: &str = "metal_fs";

// The name and version of this build
pub const PKG_NAME: &str = env!("CARGO_PKG_NAME");
pub const PKG_VERS: &str = env!("CARGO_PKG_VERSION");
pub const RUST_VERS: &str = env!("RUST_VERSION");
pub const GIT_VERS: &str = env!("GIT_VERSION");
pub const GIT_BRANCH: &str = env!("GIT_BRANCH");
pub const GIT_SHA: &str = env!("GIT_SHA");
