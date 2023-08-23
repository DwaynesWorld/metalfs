extern crate dotenv;

use clap::Parser;
use clap::Subcommand;
use dotenv::dotenv;
use log::error;
use mfs::core::version;
use mfs::storage::config::StorageServerConfig;
use mfs::storage::server;
use mfs::BANNER;

pub const LOG: &str = "metalfs::storage";

const INFO: &str = "Web server that manages file storage.";

#[derive(Debug, Parser)]
#[clap(name = "Storage service command-line interface")]
#[clap(about = INFO, before_help = BANNER, disable_version_flag = true, arg_required_else_help = true)]
struct AppOptions {
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    Server(StorageServerConfig),
    Version,
}

#[actix_web::main]
async fn main() {
    #[cfg(debug_assertions)]
    dotenv().ok();

    let app = AppOptions::parse();

    let output = match app.command {
        Commands::Server(c) => server::run(c).await,
        Commands::Version => version::init(),
    };

    if let Err(e) = output {
        error!(target: LOG, "{}", e);
    }
}
