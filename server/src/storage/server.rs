use super::config::StorageServerConfig;
use crate::core::logger;
// use actix_web::{middleware, web, App, HttpServer};

pub async fn run(config: StorageServerConfig) -> std::io::Result<()> {
    logger::init(&config.log);
    info!("Starting storage server...");
    Ok(())
}
