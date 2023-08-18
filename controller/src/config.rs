use clap::Args;

use common::logger::Level;

#[derive(Args, Debug)]
pub struct ServerConfig {
    #[clap(
        short,
        long,
        env = "METALFS_LOG",
        default_value = "info",
        forbid_empty_values = true,
        help = "The logging level",
        value_enum
    )]
    /// The logging level
    pub log: Level,

    #[clap(
        long = "host",
        env = "METALFS_CONTROLLER_HOST",
        default_value = "localhost",
        forbid_empty_values = true,
        help = "Host the server will bind to"
    )]
    /// Host where server will bind to
    pub host: String,

    #[clap(
        long = "port",
        env = "METALFS_CONTROLLER_PORT",
        default_value = "5000",
        forbid_empty_values = true,
        help = "Port the server will bind to"
    )]
    /// Port where server will bind to
    pub port: u16,
}
