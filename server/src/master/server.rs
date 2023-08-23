use super::config::MasterServerConfig;
use super::metadata::make_metadata_server;
use super::reporting::make_reporting_server;
use crate::{
    core::logger,
    master::{manager::StorageManager, monitoring::build_and_run_monitoring_service},
};
use actix_web::{middleware, web, App, HttpServer};
use std::{net::SocketAddr, sync::Arc};
use tonic::transport::Server;

pub async fn run(config: MasterServerConfig) -> std::io::Result<()> {
    // Set the default log level
    logger::init(&config.log);

    info!("Starting master server...");

    let storage_mgr = Arc::new(StorageManager::new());

    // Assemble and start the rpc server.
    let rpc = build_and_serve_rpc(&config, storage_mgr.clone());

    // Assemble and start the http server.
    let http = build_and_serve_http(&config, storage_mgr.clone());

    // Start the file server heartbeat monitor task after services have been started.
    let monitoring = build_and_run_monitoring_service(storage_mgr.clone());

    // Listen for ctrl-c
    let signal = tokio::spawn(async move {
        tokio::signal::ctrl_c().await.unwrap();
        info!("Global shutdown has been initiated...");

        // Start shutdown of tasks
        monitoring.stop().await;
        debug!("Monitoring service shutdown completed...");
    });

    let _ = tokio::try_join!(signal, rpc, http).expect("Unable to join concurrent tasks");

    Ok(())
}

fn build_and_serve_rpc(
    config: &MasterServerConfig,
    storage_mgr: Arc<StorageManager>,
) -> tokio::task::JoinHandle<Result<(), tonic::transport::Error>> {
    let host = config.host.clone();
    let port = config.rpc_port.clone();

    let addr: SocketAddr = format!("{host}:{port}")
        .parse()
        .expect("Error parsing RPC host and port");

    let server = Server::builder()
        .add_service(make_reporting_server(storage_mgr.clone()))
        .add_service(make_metadata_server(storage_mgr.clone()))
        .serve(addr);

    let task = tokio::spawn(async move {
        info!("Master RPC server running at http://{addr}");
        server.await
    });

    task
}

fn build_and_serve_http(
    config: &MasterServerConfig,
    _: Arc<StorageManager>,
) -> tokio::task::JoinHandle<Result<(), std::io::Error>> {
    let host = config.host.clone();
    let port = config.http_port.clone();

    let addr: SocketAddr = format!("{host}:{port}")
        .parse()
        .expect("Error parsing HTTP host and port");

    let server = HttpServer::new(move || {
        App::new()
            .wrap(middleware::Logger::default())
            .wrap(middleware::Compress::default())
            .configure(routes)
    })
    .bind(addr)
    .expect("Unable to resolves socket address and bind server to listener")
    .disable_signals()
    .run();

    let task = tokio::spawn(async move {
        info!("Master HTTP server running at http://{addr}");
        server.await
    });

    task
}

fn routes(_: &mut web::ServiceConfig) {}
