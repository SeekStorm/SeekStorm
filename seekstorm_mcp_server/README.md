# SeekStorm MCP search server

<img src="assets/logo.png" width="450" alt="Logo"><br>
[![Crates.io](https://img.shields.io/crates/v/seekstorm_mcp_server.svg)](https://crates.io/crates/seekstorm_mcp_server)
[![Downloads](https://img.shields.io/crates/d/seekstorm_mcp_server.svg?style=flat-square)](https://crates.io/crates/seekstorm_mcp_server)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/SeekStorm/SeekStorm?tab=Apache-2.0-1-ov-file#readme)
[![Roadmap](https://img.shields.io/badge/Roadmap-2026-DA7F07.svg)](#roadmap)

The **SeekStorm MCP Server** exposes the [SeekStorm](https://github.com/SeekStorm/SeekStorm) search library as a local [Model Context Protocol](https://modelcontextprotocol.io/) server.  
It runs as a child process over MCP stdio, embeds SeekStorm directly, and owns its local index. 
No `seekstorm_server` process or network connection is required.  
The server supports lexical, vector, and hybrid retrieval, plus document indexing and index management. 

  - Supports configurable schemas and index settings, document indexing/updating/deletion, retrieval and iteration, JSON/NDJSON ingestion, and PDF indexing.
  - Supports lexical, vector, and hybrid search, including filters, facets, sorting, and highlighting.
  - Added unit, stdio integration, and MCP contract tests.
  - The MCP server does currently implement embedded mode only; it does not connect to a remote SeekStorm server.

## Quickstart

Build from the workspace root:

```powershell
cargo build --release -p seekstorm_mcp_server
```

Add the server to an MCP client configuration. Replace the executable path with the location of the built binary:

```json
{
  "mcpServers": {
    "seekstorm": {
      "command": "C:/path/to/seekstorm_mcp_server.exe",
      "env": {
        "SEEKSTORM_INDEX_PATH": "C:/Users/me/seekstorm-index"
      }
    }
  }
}
```

The server opens the index at `SEEKSTORM_INDEX_PATH` if it exists. Otherwise, it creates an index there with a small default schema containing `title`, `body`, and `path` text fields. The index is committed when the MCP process exits.

You can also launch it directly from the workspace during development:

```powershell
$env:SEEKSTORM_INDEX_PATH = "$PWD/seekstorm_index"
cargo run -p seekstorm_mcp_server
```

MCP clients speak the stdio protocol; do not send regular output to the server's stdout.

## Index Configuration

To create an index with a different schema or vector settings at startup, set `SEEKSTORM_INDEX_CONFIG` to a JSON file containing a SeekStorm `CreateIndexRequest`. The index path still comes from `SEEKSTORM_INDEX_PATH`.

```json
{
  "index_name": "project_docs",
  "schema": [
    { "field": "title", "field_type": "Text", "store": true, "index_lexical": true, "boost": 5 },
    { "field": "body", "field_type": "Text", "store": true, "index_lexical": true, "longest": true },
    { "field": "url", "field_type": "Text", "store": true, "index_lexical": false }
  ],
  "similarity": "Bm25f",
  "tokenizer": "UnicodeAlphanumericFolded"
}
```

The request uses the same schema and index-setting types as the SeekStorm library/REST client, including synonyms, spelling correction, query completion, vector inference, and clustering. For an already-running process, use the `create_index` MCP tool with an `index_path` and a `config` object, or use `open_index` to switch to an existing local index. Switching indexes commits the currently active index first.

## Tools

- `create_index`, `open_index`, `get_index_info`, `clear_index`, `commit`
- `index_document`, `index_documents`, `update_document`, `update_documents`
- `search`, `get_document`, `document_iterator`
- `delete_document`, `delete_documents`, `delete_documents_by_query`
- `ingest_json`, `index_pdf_file`, `get_file`

Documents are arbitrary JSON objects whose fields must match the active index schema. `ingest_json` streams JSON arrays, NDJSON, and concatenated JSON documents. `index_pdf_file` extracts text from a local PDF and requires an available Pdfium library. `get_file` returns the associated file bytes as base64 text.

`delete_documents_by_query` uses `length` and `offset` to bound and page through matching documents before deletion. Search first to verify the intended matches.

## Search

`search` accepts a query, `length`, `offset`, and `mode` (`Lexical`, `Vector`, or `Hybrid`). It also supports SeekStorm query types and rewriting, real-time search, field selection, facet filters/facets, sorting, highlighting, ANN settings, and similarity thresholds.

For vector or hybrid search, provide `query_vector` (float values) or `query_vector_i8` (signed 8-bit values), unless the active index is configured with an internal inference model. Explicit vectors must match the index dimensions. Vector search needs vector-indexed fields in the schema; hybrid search can combine lexical fields and vectors. `ann_mode` accepts SeekStorm's serialized `AnnMode`, for example `"All"` or `{ "Nprobe": 8 }`.

Example lexical search tool arguments:

```json
{
  "query": "indexing pipeline",
  "mode": "Lexical",
  "length": 5,
  "offset": 0,
  "field_filter": ["title", "body"]
}
```

Example vector search arguments for a 3-dimensional vector index:

```json
{
  "query": "related documents",
  "mode": "Vector",
  "query_vector": [0.12, -0.03, 0.88],
  "length": 5
}
```

Facet definitions, facet filters, sort definitions, highlights, query rewriting, and enum settings are passed in their SeekStorm serialized JSON forms. The schema and index settings are fixed when an index is created; changing them requires creating a new index and reindexing documents.

## Tests

Run all server unit, stdio integration, and MCP contract tests from the workspace root:

```powershell
cargo test -p seekstorm_mcp_server
```
