use super::config::FilerServerConfig;
use crate::core::logger;
// use actix_web::{middleware, web, App, HttpServer};

pub async fn run(config: FilerServerConfig) -> std::io::Result<()> {
    logger::init(&config.log);
    info!("starting filer server...");
    Ok(())
}
