use actix_web::{middleware, web, App, HttpServer};
use common::logger;

use crate::config::ServerConfig;

pub async fn run(config: ServerConfig) -> std::io::Result<()> {
    // Set the default log level
    logger::init(&config.log);

    info!("Starting server...");

    // Start Http server
    let server = HttpServer::new(move || {
        App::new()
            .wrap(middleware::Logger::default())
            .wrap(middleware::Compress::default())
            .configure(routes)
    })
    .bind((config.host.clone(), config.port))
    .expect("Unable to resolves socket address and bind server to listener.")
    .disable_signals()
    .run();

    let server_handle = server.handle();
    let server_task = tokio::spawn(async move {
        info!("Server running at http://{}:{}", config.host, config.port);
        server.await
    });

    let shutdown_task = tokio::spawn(async move {
        // Listen for ctrl-c
        tokio::signal::ctrl_c().await.unwrap();
        info!("Global shutdown has been initiated...");

        server_handle.stop(true).await;
        debug!("HTTP server shutdown completed...");
    });

    let _ = tokio::try_join!(server_task, shutdown_task).expect("unable to join tasks");

    Ok(())
}

fn routes(_: &mut web::ServiceConfig) {}
