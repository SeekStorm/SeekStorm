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
//! ./seekstorm_server.exe local_ip="127.0.0.1" local_port=80 index_path="c:/seekstorm_index"
//! ```
//! &#x26A0; **WARNING**: make sure to set the MASTER_KEY_SECRET environment variable to a secret,
//! otherwise your generated API keys will be compromised.
//! ### Console commands
//! ```
//! quit to exit
//! ```
//! ### REST API endpoints
//! Use VSC extension "Rest client" to execute API calls, inspect responses and generate code snippets in your language:  
//! [**interactive API endpoint examples**](https://github.com/SeekStorm/SeekStorm/blob/master/src/seekstorm_server/test_api.rest)
//! ### create api key
//! ```
//! curl --request POST --url http://127.0.0.1:80/api/v1/apikey --header 'apikey: A6xnQhbz4Vx2HuGl4lXwZ5U2I8iziLRFnhP5eNfIRvQ=' --header 'content-type: application/json' --data '{"indices_max": 10,"indices_size_max":100000,"documents_max":10000000,"operations_max":10000000,"rate_limit": 100000}'
//! ```
//! ### delete api key
//! ```
//! curl --request DELETE --url http://127.0.0.1/api/v1/apikey --header 'apikey: AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=' --header 'content-type: application/json' --header 'user-agent: vscode-restclient'
//! ```
//! ---
//! ### create index
//! ```
//! curl --request POST --url http://127.0.0.1:80/api/v1/index --header 'apikey: AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=' --header 'content-type: application/json' --data '{"schema":[{"field_type": "Text","field_stored": true,"field_name": "title","field_indexed": true,"field_boost":10.0},{"field_type": "Text","field_stored": true,"field_name": "body","field_indexed": true},{"field_type": "String","field_stored": true,"field_name": "url","field_indexed": false}],"index_name": "test_index","similarity": "Bm25fProximity","tokenizer": "UnicodeAlphanumeric"}'
//! ```
//! ### get index
//! ```
//! curl --request GET --url http://127.0.0.1/api/v1/index/0 --header 'apikey: AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=' --header 'content-type: application/json'
//! ```
//! ### delete index
//! ```
//! curl --request DELETE --url http://127.0.0.1/api/v1/index/0 --header 'apikey: AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=' --header 'content-type: application/json'
//! ```
//! ### commit index
//! ```
//! curl --request PATCH --url http://127.0.0.1/api/v1/index/0 --header 'apikey: AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=' --header 'content-type: application/json'
//! ```
//! ---
//! ### index document(s)
//! single document
//! ```
//! curl --request POST --url http://127.0.0.1:80/api/v1/index/0/doc --header 'apikey: AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=' --header 'content-type: application/json' --data '{"title":"title1 test","body":"body1","url":"url1"}'
//! ```
//! multiple documents
//! ```
//! curl --request POST --url http://127.0.0.1:80/api/v1/index/0/doc --header 'apikey: AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=' --header 'content-type: application/json' --header 'user-agent: vscode-restclient' --data '[{"title":"title2","body":"body2 test","url":"url2"},{"title":"title3 test","body":"body3 test","url":"url3"}]'
//! ```
//! ### get document
//! without highlight
//! ```
//! curl --request GET --url http://127.0.0.1/api/v1/index/0/doc/0 --header 'apikey: AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=' --header 'content-type: application/json'
//! ```
//! with highlight
//! ```
//! curl --request GET --url http://127.0.0.1/api/v1/index/0/doc/0 --header 'apikey: AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=' --header 'content-type: application/json' --data '{"query_terms": ["test"],"fields": ["title", "body"],"highlights": [{ "field": "title", "fragment_number": 0, "fragment_size": 1000, "highlight_markup": true},{ "field": "body", "fragment_number": 2, "fragment_size": 160, "highlight_markup": true},{ "field": "body", "name": "body2", "fragment_number": 0, "fragment_size": 4000, "highlight_markup": true}]}'
//! ```
//! ### update document(s)
//! not yet implemented
//! ### delete document(s)
//! not yet implemented
//! ---
//! ### query index (GET)
//!  with URL parameter
//! ```
//! curl --request GET --url 'http://127.0.0.1/api/v1/index/0/query?query=test&offset=0&length=10' --header 'apikey: AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=' --header 'content-type: application/json'
//! ```
//! with JSON parameter
//! ```
//! curl --request GET --url http://127.0.0.1/api/v1/index/0/query --header 'apikey: AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=' --header 'content-type: application/json' --data '{"query":"test","offset":0,"length":10,"realtime": true,"field_filter": ["title", "body"]}'
//! ```
//! ### query index (POST)
//! ```
//! curl --request POST --url http://127.0.0.1/api/v1/index/0/query --header 'apikey: AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=' --header 'content-type: application/json' --data '{"query":"test","offset":0,"length":10,"realtime": true,"field_filter": ["title", "body"]}'
//! ```
//! ---
//! ## Open embedded Web UI in browser
//! <a href="http://127.0.0.1">http://127.0.0.1</a>
//! To use the embedded Web UI for a selected index you need to change the API_KEY and index_id (in QUERY_URL) in master.js
//! **before** building the seekstorm_server (html/css/js are embedded ressources).

use lazy_static::lazy_static;
use std::collections::HashMap;
use std::env;
use std::error::Error;
use std::str;

use crate::server::initialize;

lazy_static! {
    #[doc(hidden)]
    pub(crate) static ref MASTER_KEY_SECRET: String = env::var("MASTER_KEY_SECRET").unwrap_or("1234".to_string());
}

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
