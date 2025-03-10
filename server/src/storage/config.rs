use crate::core::logger::Level;
use clap::Args;

#[derive(Args, Debug)]
pub struct StorageServerConfig {
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
        env = "METALFS_STORAGE_HOST",
        default_value = "localhost",
        forbid_empty_values = true,
        help = "Host the server will bind to"
    )]
    /// Host where server will bind to
    pub host: String,

    #[clap(
        long = "port",
        env = "METALFS_STORAGE_PORT",
        default_value = "5000",
        forbid_empty_values = true,
        help = "Port the server will bind to"
    )]
    /// Port where server will bind to
    pub port: u16,
}
