extern crate dotenv;

use clap::Parser;
use clap::Subcommand;
use dotenv::dotenv;
use log::error;

use common::{version, BANNER};
use controller::config::ServerConfig;
use controller::server;

pub const LOG: &str = "metalfs::controller";

const INFO: &str = "Web server that orchestrates file-system operations and metadata.";

#[derive(Debug, Parser)]
#[clap(name = "Controller service command-line interface")]
#[clap(about = INFO, before_help = BANNER, disable_version_flag = true, arg_required_else_help = true)]
struct AppOptions {
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    Server(ServerConfig),
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
