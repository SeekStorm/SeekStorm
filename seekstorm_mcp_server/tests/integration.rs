mod common;

use serde_json::{Value, json};
use tempfile::tempdir;
use tokio::fs;

use common::{McpProcess, content_text};

fn test_index_config() -> Value {
    json!({
        "index_name": "mcp_integration",
        "schema": [
            {"field": "title", "store": true, "index_lexical": true, "field_type": "Text", "boost": 5},
            {"field": "body", "store": true, "index_lexical": true, "field_type": "Text", "longest": true},
            {"field": "source", "store": true, "index_lexical": false, "field_type": "Text"}
        ],
        "similarity": "Bm25f",
        "tokenizer": "UnicodeAlphanumericFolded"
    })
}

#[tokio::test]
async fn stdio_server_creates_indexes_streams_documents_searches_and_persists() {
    let directory = tempdir().expect("temporary index directory");
    let startup_path = directory.path().join("startup-index");
    let active_path = directory.path().join("custom-index");
    let ndjson_path = directory.path().join("documents.ndjson");
    let array_path = directory.path().join("documents.json");
    fs::write(
        &ndjson_path,
        "{\"title\":\"Nebula guide\",\"body\":\"Nebula search material\",\"source\":\"ndjson\"}\n",
    )
    .await
    .expect("write NDJSON fixture");
    fs::write(
        &array_path,
        "[{\"title\":\"Quasar guide\",\"body\":\"Quasar search material\",\"source\":\"array\"}]",
    )
    .await
    .expect("write JSON fixture");

    let mut client = McpProcess::launch(&startup_path).await;
    let created = client
        .call_tool(
            "create_index",
            json!({"index_path": active_path, "config": test_index_config()}),
        )
        .await;
    assert_eq!(created["isError"], false);

    let ingested_ndjson = client
        .call_tool("ingest_json", json!({"path": ndjson_path}))
        .await;
    assert_eq!(ingested_ndjson["isError"], false);
    let ingested_array = client
        .call_tool("ingest_json", json!({"path": array_path}))
        .await;
    assert_eq!(ingested_array["isError"], false);

    let committed = client.call_tool("commit", json!({})).await;
    assert_eq!(committed["isError"], false);

    let nebula = client
        .call_tool("search", json!({"query": "nebula", "mode": "Lexical"}))
        .await;
    let nebula_response: Value = serde_json::from_str(content_text(&nebula)).unwrap();
    assert_eq!(nebula_response["result_count_total"], 1);
    assert_eq!(nebula_response["results"][0]["title"], "Nebula guide");
    let document_id = nebula_response["results"][0]["_id"]
        .as_u64()
        .expect("document id in search result");

    let updated = client
        .call_tool(
            "update_document",
            json!({
                "doc_id": document_id,
                "document": {
                    "title": "Nebula reference",
                    "body": "Updated keyword pulsar",
                    "source": "updated"
                }
            }),
        )
        .await;
    assert_eq!(updated["isError"], false);
    client.call_tool("commit", json!({})).await;

    let updated_search = client
        .call_tool("search", json!({"query": "pulsar", "mode": "Lexical"}))
        .await;
    let updated_response: Value = serde_json::from_str(content_text(&updated_search)).unwrap();
    assert_eq!(updated_response["result_count_total"], 1);
    assert_eq!(updated_response["results"][0]["source"], "updated");

    let iterator = client
        .call_tool(
            "document_iterator",
            json!({"take": 10, "include_document": true}),
        )
        .await;
    let iterator_result: Value = serde_json::from_str(content_text(&iterator)).unwrap();
    assert_eq!(iterator_result["results"].as_array().unwrap().len(), 2);

    let info = client.call_tool("get_index_info", json!({})).await;
    let info_result: Value = serde_json::from_str(content_text(&info)).unwrap();
    assert_eq!(info_result["name"], "mcp_integration");
    assert_eq!(info_result["schema"]["source"]["field_type"], "Text");

    client.shutdown().await;

    let mut reopened = McpProcess::launch(&active_path).await;
    let persisted = reopened
        .call_tool("search", json!({"query": "pulsar", "mode": "Lexical"}))
        .await;
    let persisted_response: Value = serde_json::from_str(content_text(&persisted)).unwrap();
    assert_eq!(persisted_response["result_count_total"], 1);
    reopened.shutdown().await;
}
