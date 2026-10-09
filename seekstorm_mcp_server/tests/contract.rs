mod common;

use serde_json::json;
use tempfile::tempdir;

use common::McpProcess;

#[tokio::test]
async fn advertises_the_embedded_seekstorm_tool_contract() {
    let directory = tempdir().expect("temporary index directory");
    let mut client = McpProcess::launch(&directory.path().join("startup-index")).await;
    let response = client.request("tools/list", json!({})).await;
    let tools = response["result"]["tools"]
        .as_array()
        .expect("tools array in MCP response");
    let names: Vec<_> = tools
        .iter()
        .map(|tool| tool["name"].as_str().expect("tool name"))
        .collect();

    for name in [
        "create_index",
        "open_index",
        "index_document",
        "index_documents",
        "update_document",
        "update_documents",
        "search",
        "get_document",
        "get_file",
        "document_iterator",
        "delete_document",
        "delete_documents",
        "delete_documents_by_query",
        "clear_index",
        "commit",
        "get_index_info",
        "ingest_json",
        "index_pdf_file",
    ] {
        assert!(names.contains(&name), "missing MCP tool {name}");
    }

    let search = tools
        .iter()
        .find(|tool| tool["name"] == "search")
        .expect("search tool");
    let properties = &search["inputSchema"]["properties"];
    for field in [
        "query",
        "length",
        "mode",
        "query_vector",
        "query_vector_i8",
        "ann_mode",
        "field_filter",
        "query_facets",
        "facet_filter",
        "result_sort",
        "highlights",
    ] {
        assert!(properties[field].is_object(), "search input misses {field}");
    }
    assert!(
        properties["limit"].is_null(),
        "search should use SeekStorm's length name"
    );

    let delete_by_query = tools
        .iter()
        .find(|tool| tool["name"] == "delete_documents_by_query")
        .expect("delete_documents_by_query tool");
    let delete_properties = &delete_by_query["inputSchema"]["properties"];
    assert!(delete_properties["length"].is_object());
    assert!(delete_properties["limit"].is_null());

    let create = tools
        .iter()
        .find(|tool| tool["name"] == "create_index")
        .expect("create_index tool");
    let create_properties = &create["inputSchema"]["properties"];
    assert!(create_properties["index_path"].is_object());
    assert!(create_properties["config"].is_object());

    client.shutdown().await;
}
