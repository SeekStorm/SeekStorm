# SeekStorm server 

* The SeekStorm server is a standalone search server to be accessed via HTTP, while the SeekStorm crate/library can be embedded into your program.
* it is both accessible via RESTful API endpoints and via embedded web UI
* supports multi-tenancy: multiple users, each with multiple indices
* API-key management
* rate-limiting

## Command line parameters

* index_path   (default = "/seekstorm_index" in current directory)
* local_ip     (default = 0.0.0.0)
* local_port   (default = 80)

```
seekstorm_server.exe local_ip="127.0.0.1" local_port=80 index_path="c:/seekstorm_index"
```

## Console commands

* quit to exit

## Open embedded Web UI in browser
[http://127.0.0.1](http://127.0.0.1)

To use the embedded Web UI for a selected index you need to change the API_KEY and index_id (in QUERY_URL) in master.js
**before** building the seekstorm_server (html/css/js are embedded ressources). 

## REST API endpoints

[interactive API endpoint examples](https://github.com/SeekStorm/SeekStorm/blob/master/src/seekstorm_server/test_api.rest)

## Building

```
cargo build --release
```

&#x26A0; **WARNING**: make sure to set the MASTER_KEY_SECRET environment variable to a secret, otherwise your generated API keys will be compromised.