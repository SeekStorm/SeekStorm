# SeekStorm server 

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
ingest [file_path] -t [type] -k [api_key] -i [index_id] -d [delimiter]  -h [header] -q [quoting] -s [skip] -n [num]
```

-t [type]:      file type, default=derived from file extension  
-k [api_key]:   default=Demo API Key  
-i [index_id]:  default=0  
-h [header]:    CSV header, treat first line of file as header, default=false  
-d [delimiter]: CSV delimiter, default=derived from file type or file extension  
-q [quoting]:   CSV quoting, default=true  
-s [skip]:      number of records to skip from start, default=None=0  
-n [num]:       number of records to read, default=None=usize::MAX   

The path to the PDF, CSV or JSON files is specified by the `[data_filename]` parameter, the API key is specified by the `[api_key]` parameter, the index ID by specified in the `[index_id]` parameter.  
If no absolute path is specified then the path specified with the command line parameter `ingest_path` or the path of the current running seekstorm_server.exe is used.
The document file ingestion is streamed without loading the whole document vector into memory to allow for bulk import with unlimited file size and document number while keeping RAM consumption low.

If the file extension is `PDF` then [PDF](https://en.wikipedia.org/wiki/PDF) format is assumed, if the file extension is `CSV` then [CSV](https://en.wikipedia.org/wiki/Comma-separated_values) format is assumed, if the file extension is `JSON` then it is automatically detected whether it is in [JSON](https://en.wikipedia.org/wiki/JSON), [Newline-delimited JSON](https://github.com/ndjson/ndjson-spec) (ndjson), or [Concatenated JSON](https://en.wikipedia.org/wiki/JSON_streaming) format.  
If the specified `[file_path]` is a `directory` instead of a file, then all `PDF` files within that directory are indexed, including all subdirectories.

If no `[file_path]` parameter is specified then **wiki-articles.json** is indexed, if present in same directory like seekstorm_server.exe or the directory specified by the command line parameter `ingest_path`.  
If no `[api_key]` and `[index_id]` parameter are specified then the default demo API key and index_id=0 are used.

&#x26A0; **CAUTION**: The **array of documents** is expected to be in the **root element** of a `JSON` file.

If API key or index specified by `[api_key]` and `[index_id]` parameter do not yet exist they have to be created prior to ingest via REST API endpoints:

create api key: Use master API key displayed in the server console at startup.
```
curl --request POST --url http://127.0.0.1:80/api/v1/apikey --header 'apikey: BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB=' --header 'content-type: application/json' --data '{"indices_max": 10,"indices_size_max":100000,"documents_max":10000000,"operations_max":10000000,"rate_limit": 100000}'
```

create index: Use individual API key (use create api key above to generate)
```
curl --request POST --url http://127.0.0.1:80/api/v1/index --header 'apikey: AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=' --header 'content-type: application/json' --data '{"schema":[{"field_type": "Text","stored": true,"field": "title","indexed": true,"boost":10.0},{"field_type": "Text","stored": true,"field": "body","indexed": true},{"field_type": "String","stored": true,"field": "url","indexed": false}],"index_name": "test_index","similarity": "Bm25fProximity","tokenizer": "UnicodeAlphanumeric"}'
```

&#x26A0; **CAUTION**: If sending CURL commands from MS Windows Powershell use 'curl.exe' instead of 'curl' AND escape (\") all double quotes within the JSON request object!


After ingest you can search the index via [REST API endpoints](https://github.com/SeekStorm/SeekStorm/blob/main/src/seekstorm_server#rest-api-endpoints) or via [browser in the embedded web UI](https://github.com/SeekStorm/SeekStorm/blob/main/src/seekstorm_server#open-embedded-web-ui-in-browser).  
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

If the default names defined in [master.js](https://github.com/SeekStorm/SeekStorm/blob/main/src/seekstorm_server/web/js/master.js) for `TITLE_FIELD`, `TEXT_FIELD` and `URL_FIELD` do not exist in the schema of the selected index, then the first three fields (stored=true) from the index schema are automatically used for display in the web UI instead.

To permanently change the default **API key** or **index_id** of the embedded Web UI modify `API_KEY` and `INDEX_ID` in [/src/seekstorm_server/web/js/master.js](https://github.com/SeekStorm/SeekStorm/blob/main/src/seekstorm_server/web/js/master.js) **before** building the seekstorm_server (html/css/js are embedded ressources).  
To permanently change the default **field names** from the index schema that are used in the web UI modify `TITLE_FIELD`, `TEXT_FIELD` and `URL_FIELD` [/src/seekstorm_server/web/js/master.js](https://github.com/SeekStorm/SeekStorm/blob/main/src/seekstorm_server/web/js/master.js) **before** building the seekstorm_server.

The embedded Web UI is intended for demonstration, test and debugging rather than for end customer use.

## REST API endpoints

Use VSC extension "Rest client" to execute API calls, inspect responses and generate code snippets in your language:  
[**interactive API endpoint examples**](https://github.com/SeekStorm/SeekStorm/blob/master/src/seekstorm_server/test_api.rest)

&#x26A0; **CAUTION**: If sending CURL commands from MS Windows Powershell use 'curl.exe' instead of 'curl' AND escape (\") all double quotes within the JSON request object!


### create api key
Use master API key displayed in the server console at startup.
WARNING: make sure to set the MASTER_KEY_SECRET environment variable to a secret, otherwise your generated API keys will be compromised.
```
curl --request POST --url http://127.0.0.1:80/api/v1/apikey --header 'apikey: BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB=' --header 'content-type: application/json' --data '{"indices_max": 10,"indices_size_max":100000,"documents_max":10000000,"operations_max":10000000,"rate_limit": 100000}'
```
### delete api key
```
curl --request DELETE --url http://127.0.0.1/api/v1/apikey --header 'apikey: AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=' --header 'content-type: application/json' --header 'user-agent: vscode-restclient'
```

---

### create index
```
curl --request POST --url http://127.0.0.1:80/api/v1/index --header 'apikey: AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=' --header 'content-type: application/json' --data '{"schema":[{"field_type": "Text","stored": true,"field": "title","indexed": true,"boost":10.0},{"field_type": "Text","stored": true,"field": "body","indexed": true},{"field_type": "String","stored": true,"field": "url","indexed": false}],"index_name": "test_index","similarity": "Bm25fProximity","tokenizer": "UnicodeAlphanumeric"}'
```
### get index
```
curl --request GET --url http://127.0.0.1/api/v1/index/0 --header 'apikey: AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=' --header 'content-type: application/json'
```

### delete index
```
curl --request DELETE --url http://127.0.0.1/api/v1/index/0 --header 'apikey: AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=' --header 'content-type: application/json'
```

### commit index
```
curl --request PATCH --url http://127.0.0.1/api/v1/index/0 --header 'apikey: AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=' --header 'content-type: application/json'
```
---

### index document(s)

single document
```
curl --request POST --url http://127.0.0.1:80/api/v1/index/0/doc --header 'apikey: AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=' --header 'content-type: application/json' --data '{"title":"title1 test","body":"body1","url":"url1"}'
```

multiple documents
```
curl --request POST --url http://127.0.0.1:80/api/v1/index/0/doc --header 'apikey: AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=' --header 'content-type: application/json' --header 'user-agent: vscode-restclient' --data '[{"title":"title2","body":"body2 test","url":"url2"},{"title":"title3 test","body":"body3 test","url":"url3"}]'
```

### index PDF file 

- converts pdf to text and indexes it
- extracts title from metatag, or first line of text, or from filename
- extracts creation date from metatag, or from file creation date (Unix timestamp: the number of seconds since 1 January 1970)
- copies all ingested pdf files to "files" subdirectory in index
- the following index schema is required (and automatically created by the console `ingest` command):
```json
 [
   {
     "field": "title",
     "stored": true,
     "indexed": true,
     "field_type": "Text",
     "boost": 10
   },
   {
     "field": "body",
     "stored": true,
     "indexed": true,
     "field_type": "Text"
   },
   {
     "field": "url",
     "stored": true,
     "indexed": false,
     "field_type": "Text"
   },
   {
     "field": "date",
     "stored": true,
     "indexed": false,
     "field_type": "Timestamp",
     "facet": true
   }
 ]
```

create index
```
curl.exe --request POST --url 'http://127.0.0.1/api/v1/index' --header 'apikey: AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=' --header 'content-type: application/json' --data '{\"schema\":[{\"field\": \"title\", \"field_type\": \"Text\",\"stored\": true, \"indexed\": true,\"boost\":10.0},{\"field\": \"body\", \"field_type\": \"Text\",\"stored\": true, \"indexed\": true},{\"field\": \"url\", \"field_type\": \"Text\",\"stored\": true, \"indexed\": false},{\"field\": \"date\", \"field_type\": \"Timestamp\",\"stored\": true, \"indexed\": true, \"facet\": true}],\"index_name\": \"pdf_index\",\"similarity\": \"Bm25fProximity\",\"tokenizer\": \"UnicodeAlphanumeric\"}'
```

index PDF
```
curl.exe --request POST --url 'http://127.0.0.1/api/v1/index/0/file' --header 'apikey: AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=' --header 'content-type: application/pdf' --header 'file: C:\Users\johndoe\Downloads\odsceast2018.pdf' --data-binary '@C:\Users\johndoe\Downloads\odsceast2018.pdf'
```

### get PDF file 

get PDF file bytes from file folder in index
```
curl --request GET --url http://127.0.0.1/api/v1/index/0/file/0 --header 'apikey: AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=' --header 'content-type: application/json'
```

### get document 

without highlight
```
curl --request GET --url http://127.0.0.1/api/v1/index/0/doc/0 --header 'apikey: AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=' --header 'content-type: application/json'
```

with highlight
```
curl --request GET --url http://127.0.0.1/api/v1/index/0/doc/0 --header 'apikey: AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=' --header 'content-type: application/json' --data '{"query_terms": ["test"],"fields": ["title", "body"],"highlights": [{ "field": "title", "fragment_number": 0, "fragment_size": 1000, "highlight_markup": true},{ "field": "body", "fragment_number": 2, "fragment_size": 160, "highlight_markup": true},{ "field": "body", "name": "body2", "fragment_number": 0, "fragment_size": 4000, "highlight_markup": true}]}'
```
### update document(s) 

update document
```
curl --request PATCH --url http://127.0.0.1/api/v1/index/0/doc --header 'apikey: AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=' --header 'content-type: application/json' --data '[0,{"title":"title1 test","body":"body1","url":"url1"}]'
```

update documents
```
curl --request PATCH --url http://127.0.0.1/api/v1/index/0/doc --header 'apikey: AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=' --header 'content-type: application/json' --data '[[1,{"title":"title1 test","body":"body1","url":"url1"}],[2,{"title":"title3 test","body":"body3 test","url":"url3"}]]'
```

### delete document(s) 

delete document, by single document ID in URL parameter
```
curl --request DELETE --url http://127.0.0.1/api/v1/index/0/doc/0 --header 'apikey: AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=' --header 'content-type: application/json'
```

delete document, by single document ID in JSON request object
```
curl --request DELETE --url http://127.0.0.1/api/v1/index/0/doc --header 'apikey: AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=' --header 'content-type: application/json' --data 0
```

delete documents, by vector of document IDs in JSON request object
```
curl --request DELETE --url http://127.0.0.1/api/v1/index/0/doc --header 'apikey: AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=' --header 'content-type: application/json' --data '[0,1]'
```

delete documents, by query in JSON request object
```
curl --request DELETE --url http://127.0.0.1/api/v1/index/0/doc --header 'apikey: AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=' --header 'content-type: application/json' --data '{"query":"test","offset":0,"length":10,"realtime": true,"field_filter": ["title", "body"]}'
```

--- 

### query index (GET)

wit URL parameter
```
curl --request GET --url 'http://127.0.0.1/api/v1/index/0/query?query=test&offset=0&length=10' --header 'apikey: AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=' --header 'content-type: application/json'
```

with JSON parameter
```
curl --request GET --url http://127.0.0.1/api/v1/index/0/query --header 'apikey: AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=' --header 'content-type: application/json' --data '{"query":"test","offset":0,"length":10,"realtime": true,"field_filter": ["title", "body"]}'
```

### query index (POST)

```
curl --request POST --url http://127.0.0.1/api/v1/index/0/query --header 'apikey: AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=' --header 'content-type: application/json' --data '{"query":"test","offset":0,"length":10,"realtime": true,"field_filter": ["title", "body"]}'
```

## Building

```
cargo build --release
```

&#x26A0; **WARNING**: make sure to set the MASTER_KEY_SECRET environment variable to a secret, otherwise your generated API keys will be compromised.