use crate::core::logger::Level;
use clap::Args;

#[derive(Args, Debug)]
pub struct MasterServerConfig {
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
        env = "METALFS_MASTER_HOST",
        default_value = "localhost",
        forbid_empty_values = true,
        help = "Host the server will bind to"
    )]
    /// Host where server will bind to
    pub host: String,

    #[clap(
        long = "rpc_port",
        env = "METALFS_MASTER_RPC_PORT",
        default_value = "5500",
        forbid_empty_values = true,
        help = "RPC Port the server will bind to"
    )]
    /// RPC Port where server will bind to
    pub rpc_port: u16,

    #[clap(
        long = "http_port",
        env = "METALFS_MASTER_HTTP_PORT",
        default_value = "5500",
        forbid_empty_values = true,
        help = "HTTP Port the server will bind to"
    )]
    /// HTTP Port where server will bind to
    pub http_port: u16,
}
