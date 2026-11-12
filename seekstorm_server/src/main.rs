#![recursion_limit = "256"]
#![crate_name = "seekstorm_server"]
#![doc(html_logo_url = "http://seekstorm.com/assets/logo.svg")]
#![allow(clippy::approx_constant)]
#![doc = include_str!("../README.md")]

use std::collections::HashMap;
use std::env;
use std::error::Error;
use std::str;
use std::sync::LazyLock;

use crate::server::initialize;

/// &#x26A0; **WARNING**: make sure to set the MASTER_KEY_SECRET environment variable to a secret, otherwise your generated API keys will be compromised.
#[doc(hidden)]
pub static MASTER_KEY_SECRET: LazyLock<String> =
    LazyLock::new(|| env::var("MASTER_KEY_SECRET").unwrap_or("1234".to_string()));

#[doc(hidden)]
mod api_endpoints;
#[doc(hidden)]
mod http_server;
#[doc(hidden)]
mod multi_tenancy;
#[doc(hidden)]
mod server;
#[doc(hidden)]
pub(crate) const VERSION: &str = env!("CARGO_PKG_VERSION");
#[doc(hidden)]
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let args: Vec<String> = env::args().collect();
    let mut params = HashMap::new();
    if args.len() > 1 {
        for s in args {
            let split: Vec<&str> = s.split('=').collect();
            if split.len() == 2 {
                params.insert(split[0].trim().to_owned(), split[1].trim().to_owned());
            }
        }
    }

    initialize(params).await;

    Ok(())
}
