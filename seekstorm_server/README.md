# SeekStorm server 

SeekStorm open-source, multi-tenancy, vector & lexical search server

<img src="assets/logo.png" width="450" alt="Logo"><br>
[![Crates.io](https://img.shields.io/crates/v/seekstorm_server.svg)](https://crates.io/crates/seekstorm_server)
[![Downloads](https://img.shields.io/crates/d/seekstorm_server.svg?style=flat-square)](https://crates.io/crates/seekstorm_server)
[![Docker](https://img.shields.io/docker/pulls/wolfgarbe/seekstorm_server)](https://hub.docker.com/r/wolfgarbe/seekstorm_server)
[![REST API Documentation](https://docs.rs/seekstorm/badge.svg)](https://seekstorm.com/documentation/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/SeekStorm/SeekStorm?tab=Apache-2.0-1-ov-file#readme)
[![Roadmap](https://img.shields.io/badge/Roadmap-2026-DA7F07.svg)](#roadmap)

* The SeekStorm server is a standalone search server to be accessed via HTTP, while the SeekStorm crate/library can be embedded into your program.
* Index and search via [RESTful API](#rest-api-endpoints) endpoints and via [Embedded  web UI](#open-embedded-web-ui-in-browser).
* Ingest local data files in [JSON](https://en.wikipedia.org/wiki/JSON), [Newline-delimited JSON](https://github.com/ndjson/ndjson-spec) (ndjson), and [Concatenated JSON](https://en.wikipedia.org/wiki/JSON_streaming) formats via console command.  
* Multi-tenancy index management: multiple users, each with multiple indices
* API-key management
* Rate-limiting
* Cross-platform: runs on Linux, Windows, and macOS (other OS untested)

## Command line parameters

* ingest_path   (default = path of the current running executable) : The default path for data files to ingest with the console command `ingest`, if entered without absolute path/filename.
* index_path   (default = "/seekstorm_index" in the path of the current running executable)
* local_ip     (default = 0.0.0.0)
* local_port   (default = 80)
* force_shard_number : Set number of shards manually.
    - not set: number of shards is set automatically = number of physical processor cores (default)
    - small: slower indexing, higher latency, slightly higher throughput, faster realtime search, lower RAM consumption, more frequent auto-commit (docs round-robin distributed across shards, commit after 64k docs per shard)
    - large: faster indexing, lower latency, slightly lower throughput, slower realtime search, higher RAM consumption, less frequent auto-commit (docs round-robin distributed across shards, commit after 64k docs per shard)

```
./seekstorm_server.exe local_ip="127.0.0.1" local_port=80 index_path="c:/seekstorm_index"
```

## Console commands

Index local files in [PDF](https://en.wikipedia.org/wiki/PDF), [CSV](https://en.wikipedia.org/wiki/Comma-separated_values), [SSV](https://en.wikipedia.org/wiki/Comma-separated_values), [TSV](https://en.wikipedia.org/wiki/Comma-separated_values), [PSV](https://en.wikipedia.org/wiki/Comma-separated_values), [JSON](https://en.wikipedia.org/wiki/JSON), [Newline-delimited JSON](https://github.com/ndjson/ndjson-spec) (ndjson), or [Concatenated JSON](https://en.wikipedia.org/wiki/JSON_streaming) formats via console command. 

```
ingest
```

```
ingest [file_path]
```

```
ingest [file_path] -t [type] -k [api_key] -i [index_id] -d [delimiter] -h [header] -q [quoting] -s [skip] -n [num]
```

-t \[type\]:      file type, default=derived from file extension  
-k \[api_key\]:   default=Demo API Key  
-i \[index_id\]:  default=0  
-h \[header\]:    CSV header, treat first line of file as header, default=false  
-d \[delimiter\]: CSV delimiter, default=derived from file type or file extension  
-q \[quoting\]:   CSV quoting, default=true  
-s \[skip\]:      number of records to skip from start, default=None=0  
-n \[num\]:       number of records to read, default=None=usize::MAX   

The path to the PDF, CSV or JSON files is specified by the `[data_filename]` parameter, the API key is specified by the `[api_key]` parameter, the index ID by specified in the `[index_id]` parameter.  
If no absolute path is specified then the path specified with the command line parameter `ingest_path` or the path of the current running seekstorm_server.exe is used.
The document file ingestion is streamed without loading the whole document vector into memory to allow for bulk import with unlimited file size and document number while keeping RAM consumption low.

If the file extension is `PDF` then [PDF](https://en.wikipedia.org/wiki/PDF) format is assumed, if the file extension is `CSV` then [CSV](https://en.wikipedia.org/wiki/Comma-separated_values) format is assumed, if the file extension is `JSON` then it is automatically detected whether it is in [JSON](https://en.wikipedia.org/wiki/JSON), [Newline-delimited JSON](https://github.com/ndjson/ndjson-spec) (ndjson), or [Concatenated JSON](https://en.wikipedia.org/wiki/JSON_streaming) format.  
If the specified `[file_path]` is a `directory` instead of a file, then all `PDF` files within that directory are indexed, including all subdirectories.

If no `[file_path]` parameter is specified then **wiki-articles.json** is indexed, if present in same directory like seekstorm_server.exe or the directory specified by the command line parameter `ingest_path`.  
If no `[api_key]` and `[index_id]` parameter are specified then the default demo API key and index_id=0 are used.

&#x26A0; **CAUTION**: When ingesting a `CSV` file, the **columns in the CSV file** are assigned **sequentially** to the defined **fields in the schema** of the index.  
  Any columns in the CSV file exceeding the number of defined fields are ignored.

&#x26A0; **CAUTION**: The **array of documents** is expected to be in the **root element** of a `JSON` file.

If API key or index specified by `[api_key]` and `[index_id]` parameter do not yet exist they have to be created prior to ingest via REST API endpoints:

create api key: Use master API key displayed in the server console at startup.
```bash
curl -X POST http://127.0.0.1/api/v1/apikey \
  -H 'apikey: <MASTER_API_KEY>' -H 'content-type: application/json' \
  -d '{"indices_max":10,"indices_size_max":100000,"documents_max":10000000,"operations_max":10000000,"rate_limit":100000}'
```

create index: Use individual API key (use create api key above to generate)
```bash
curl -X POST http://127.0.0.1/api/v1/index \
  -H 'apikey: <API_KEY>' -H 'content-type: application/json' \
  -d '{"index_config":{"name":"test_index","lexical_similarity":"Bm25fProximity","tokenizer":"UnicodeAlphanumeric"},"schema":[{"field":"title","field_type":"Text","store":true,"index_lexical":true,"boost":10.0},{"field":"body","field_type":"Text","store":true,"index_lexical":true,"longest":true},{"field":"url","field_type":"String32","store":true,"index_lexical":false}],"synonyms":[]}'
```

&#x26A0; **CAUTION**: If sending CURL commands from MS Windows Powershell use 'curl.exe' instead of 'curl' AND escape (\") all double quotes within the JSON request object!


After ingest you can search the index via [REST API endpoints](https://github.com/SeekStorm/SeekStorm/blob/main/seekstorm_server/README.md#rest-api-endpoints) or via [browser in the embedded web UI](https://github.com/SeekStorm/SeekStorm/blob/main/seekstorm_server/README.md#open-embedded-web-ui-in-browser).  
The field names to display in the web UI can be automatically detected or pre-defined. 

<br><br>

```
delete
```
Delete the demo API key and all its indices.

```
quit
```
Exit server.

```
help
```
Show help.

## Manual file manipulation

While the server is not running, you may manually delete or backup API key and index directories or modify schema files in a text editor.  
In the index.json file you also may change access_type":"Mmap" to access_type":"Ram" and vice versa, as the index file format is identical.

## Open embedded Web UI in browser

The embedded web UI allows to search and display results (document fields) from an index:  
[http://127.0.0.1](http://127.0.0.1)

Per default the web UI is set to the [Wikipedia demo](https://github.com/SeekStorm/SeekStorm/?tab=readme-ov-file#build-a-wikipedia-search-engine-with-the-seekstorm-server) **API key**, **index_id** and **field names** ([use console command `ingest` first](#console-commands)).  

<img src="../../assets/wikipedia_demo.png" width="800">
<br><br>

To overwrite the default **API key** and **index_id** use url parameters: [http://127.0.0.1/?api_key=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=&index_id=0](http://127.0.0.1/?api_key=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=&index_id=0).  
This allows to open an web UI to multiple indices in different web browser tabs simultaneously.

If the default names defined in [master.js](https://github.com/SeekStorm/SeekStorm/blob/main/seekstorm_server/web/js/master.js) for `TITLE_FIELD`, `TEXT_FIELD` and `URL_FIELD` do not exist in the schema of the selected index, then the first three fields (stored=true) from the index schema are automatically used for display in the web UI instead.

To permanently change the default **API key** or **index_id** of the embedded Web UI modify `API_KEY` and `INDEX_ID` in [/src/seekstorm_server/web/js/master.js](https://github.com/SeekStorm/SeekStorm/blob/main/seekstorm_server/web/js/master.js) **before** building the seekstorm_server (html/css/js are embedded ressources).  
To permanently change the default **field names** from the index schema that are used in the web UI modify `TITLE_FIELD`, `TEXT_FIELD` and `URL_FIELD` [/src/seekstorm_server/web/js/master.js](https://github.com/SeekStorm/SeekStorm/blob/main/seekstorm_server/web/js/master.js) **before** building the seekstorm_server.

The embedded Web UI is intended for demonstration, test and debugging rather than for end customer use.

## REST API

The server exposes a REST API for API keys, indices, documents and search (lexical, vector and hybrid).

- 📖 **[Full API reference](https://seekstorm.com/documentation/)** (OpenAPI, generated from the server code)
- ▶️ **[Interactive examples](https://github.com/SeekStorm/SeekStorm/blob/master/src/seekstorm_server/test_api.rest)**: open `test_api.rest` in VS Code with the [REST Client](https://marketplace.visualstudio.com/items?itemName=humao.rest-client) extension, or import the OpenAPI spec into [Bruno](https://www.usebruno.com/) / Postman
- 🧰 **[Client SDKs](https://github.com/SeekStorm/SeekStorm/tree/main/seekstorm_client)** for Rust, Python, TypeScript, C# and Java, if you prefer idiomatic code over raw HTTP

&#x26A0; **WARNING**: Set the `MASTER_KEY_SECRET` environment variable to a secret before generating API keys, otherwise your generated API keys will be compromised.

### Quick start with curl

The examples assume the server runs on `http://127.0.0.1` (port 80). If you use the Docker command above, use `http://127.0.0.1:8000`.

**1. Create an API key** with the master API key displayed in the server console at startup:

```bash
curl -X POST http://127.0.0.1/api/v1/apikey \
  -H 'apikey: <MASTER_API_KEY>' -H 'content-type: application/json' \
  -d '{"indices_max":10,"indices_size_max":100000,"documents_max":10000000,"operations_max":10000000,"rate_limit":100000}'
```

The response is your API key. Use it as the `apikey` header in all following calls.

**2. Create an index** (returns the `index_id`, here `0`):

```bash
curl -X POST http://127.0.0.1/api/v1/index \
  -H 'apikey: <API_KEY>' -H 'content-type: application/json' \
  -d '{"index_config":{"name":"test_index","lexical_similarity":"Bm25fProximity","tokenizer":"UnicodeAlphanumeric"},"schema":[{"field":"title","field_type":"Text","store":true,"index_lexical":true,"boost":10.0},{"field":"body","field_type":"Text","store":true,"index_lexical":true,"longest":true},{"field":"url","field_type":"String32","store":true,"index_lexical":false}],"synonyms":[]}'
```

**3. Index documents** (a single document or an array):

```bash
curl -X POST http://127.0.0.1/api/v1/index/0/doc \
  -H 'apikey: <API_KEY>' -H 'content-type: application/json' \
  -d '[{"title":"title2","body":"body2 test","url":"url2"},{"title":"title3 test","body":"body3 test","url":"url3"}]'
```

**4. Search:**

```bash
curl -X POST http://127.0.0.1/api/v1/index/0/query \
  -H 'apikey: <API_KEY>' -H 'content-type: application/json' \
  -d '{"query_string":"test","offset":0,"length":10,"include_uncommitted":true}'
```

New documents become searchable immediately with `include_uncommitted: true`. Call `PATCH /api/v1/index/0` to commit them to disk.

&#x26A0; **Windows PowerShell**: use `curl.exe` instead of `curl`, and escape all double quotes inside the JSON body (`\"`).

### Other endpoints

Document update/delete/get (with highlighting), PDF upload and download, the document iterator, vector and hybrid search, faceting, sorting, and index and API-key management are covered in the [API reference](https://seekstorm.com/documentation/) and in `test_api.rest`.

## Building

```
cargo build --release
```

&#x26A0; **WARNING**: make sure to set the MASTER_KEY_SECRET environment variable to a secret, otherwise your generated API keys will be compromised.