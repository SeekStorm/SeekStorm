#![crate_name = "seekstorm_server"]
#![doc(html_logo_url = "http://seekstorm.com/assets/logo.svg")]

//! # `seekstorm_server`
//! **SeekStorm** is an open-source, sub-millisecond full-text search library & multi-tenancy server implemented in Rust.<br>
//! **SeekStorm server** is a standalone search server to be accessed via HTTP.
//! * it is both accessible via RESTful API endpoints and via embedded web UI
//! * supports multi-tenancy: multiple users, each with multiple indices
//! * API-key management
//! * rate-limiting
//! ### Command line parameters
//! ```rust
//! * index_path   (default = "/seekstorm_index" in current directory)
//! * local_ip     (default = 0.0.0.0)
//! * local_port   (default = 80)
//! seekstorm_server.exe local_ip="127.0.0.1" local_port=80 index_path="c:/seekstorm_index"
//! ```
//! ### Console commands
//! ```
//! quit to exit
//! ```
//! ### REST API endpoints
//! use with VSC extension "Rest client" to execute API calls and inspect responses:  
//! [interactive API endpoint examples](https://github.com/SeekStorm/SeekStorm/blob/master/src/seekstorm_server/test_api.rest)
//! ## Open embedded Web UI in browser
//! <a href="http://127.0.0.1">http://127.0.0.1</a>
//! To use the embedded Web UI for a selected index you need to change the API_KEY and index_id (in QUERY_URL) in master.js
//! **before** building the seekstorm_server (html/css/js are embedded ressources).

use std::collections::HashMap;
use std::env;
use std::error::Error;
use std::str;

use crate::server::initialize;

/// &#x26A0; **WARNING**: make sure to change the SECRET_MASTER_KEY in src\seekstorm_server\main.rs to a secret, otherwise your generated API keys will be compromised.
const SECRET_MASTER_KEY: &str = "1234";

#[doc(hidden)]
mod api_endpoints;
#[doc(hidden)]
mod http_server;
#[doc(hidden)]
mod ingest;
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

    println!("SeekStorm server v{} starting ...", VERSION,);
    println!("Hit CTRL-C or enter 'quit' to shutdown server");

    initialize(params).await;

    Ok(())
}
