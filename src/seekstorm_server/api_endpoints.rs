use std::{
    collections::HashMap,
    env::current_exe,
    fs::{self, File},
    path::{Path, PathBuf},
    sync::Arc,
    time::Instant,
};

use ahash::AHashMap;
use itertools::Itertools;
use std::collections::HashSet;
use utoipa::{OpenApi, ToSchema};

use seekstorm::{
    commit::Commit,
    highlighter::{Highlight, highlighter},
    index::{
        AccessType, DeleteDocument, DeleteDocuments, DeleteDocumentsByQuery, DistanceField,
        Document, Facet, FileType, FrequentwordType, IndexArc, IndexDocument, IndexDocuments,
        IndexMetaObject, MinMaxFieldJson, NgramSet, SchemaField, SimilarityType, StemmerType,
        StopwordType, Synonym, TokenizerType, UpdateDocument, UpdateDocuments, create_index,
        open_index,
    },
    ingest::IndexPdfBytes,
    search::{FacetFilter, QueryFacet, QueryType, ResultSort, ResultType, Search},
};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::{
    VERSION,
    http_server::calculate_hash,
    multi_tenancy::{ApikeyObject, ApikeyQuotaObject},
};

const APIKEY_PATH: &str = "apikey.json";

/// Search request object
#[derive(Deserialize, Serialize, Clone, ToSchema)]
pub struct SearchRequestObject {
    /// Query string, search operators + - "" are recognized.
    #[serde(rename = "query")]
    pub query_string: String,
    #[serde(default)]
    #[schema(required = false, minimum = 0, default = 0, example = 0)]
    /// Offset of search results to return.
    pub offset: usize,
    /// Number of search results to return.
    #[serde(default = "length_api")]
    #[schema(required = false, minimum = 1, default = 10, example = 10)]
    pub length: usize,
    #[serde(default)]
    pub result_type: ResultType,
    /// True realtime search: include indexed, but uncommitted documents into search results.
    #[serde(default)]
    pub realtime: bool,
    #[serde(default)]
    pub highlights: Vec<Highlight>,
    /// Specify field names where to search at querytime, whereas SchemaField.indexed is set at indextime. If empty then all indexed fields are searched.
    #[schema(required = false, example = json!(["title"]))]
    #[serde(default)]
    pub field_filter: Vec<String>,
    #[serde(default)]
    pub fields: Vec<String>,
    #[serde(default)]
    pub distance_fields: Vec<DistanceField>,
    #[serde(default)]
    pub query_facets: Vec<QueryFacet>,
    #[serde(default)]
    pub facet_filter: Vec<FacetFilter>,
    /// Sort field and order:
    /// Search results are sorted by the specified facet field, either in ascending or descending order.
    /// If no sort field is specified, then the search results are sorted by rank in descending order per default.
    /// Multiple sort fields are combined by a "sort by, then sort by"-method ("tie-breaking"-algorithm).
    /// The results are sorted by the first field, and only for those results where the first field value is identical (tie) the results are sub-sorted by the second field,
    /// until the n-th field value is either not equal or the last field is reached.
    /// A special _score field (BM25x), reflecting how relevant the result is for a given search query (phrase match, match in title etc.) can be combined with any of the other sort fields as primary, secondary or n-th search criterium.
    /// Sort is only enabled on facet fields that are defined in schema at create_index!
    /// Examples:
    /// - result_sort = vec![ResultSort {field: "price".into(), order: SortOrder::Descending, base: FacetValue::None},ResultSort {field: "language".into(), order: SortOrder::Ascending, base: FacetValue::None}];
    /// - result_sort = vec![ResultSort {field: "location".into(),order: SortOrder::Ascending, base: FacetValue::Point(vec![38.8951, -77.0364])}];
    #[schema(required = false, example = json!([{"field": "date", "order": "Ascending", "base": "None" }]))]
    #[serde(default)]
    pub result_sort: Vec<ResultSort>,
    #[schema(required = false, example = QueryType::Intersection)]
    #[serde(default = "query_type_api")]
    pub query_type_default: QueryType,
}

fn query_type_api() -> QueryType {
    QueryType::Intersection
}

fn length_api() -> usize {
    10
}

#[derive(Debug, Clone, Deserialize, Serialize, ToSchema)]
pub struct SearchResultObject {
    pub time: u128,
    pub query: String,
    pub offset: usize,
    pub length: usize,
    pub count: usize,
    pub count_total: usize,
    pub query_terms: Vec<String>,
    #[schema(value_type=Vec<HashMap<String, serde_json::Value>>)]
    pub results: Vec<Document>,
    #[schema(value_type=HashMap<String, Vec<(String, usize)>>)]
    pub facets: AHashMap<String, Facet>,
    pub suggestions: Vec<String>,
}

/// Create index request object
#[derive(Debug, Clone, Deserialize, Serialize, ToSchema)]
pub struct CreateIndexRequest {
    #[schema(example = "demo_index")]
    pub index_name: String,
    #[schema(required = true, example = json!([
    {"field":"title","field_type":"Text","stored":true,"indexed":true,"boost":10.0},
    {"field":"body","field_type":"Text","stored":true,"indexed":true},
    {"field":"url","field_type":"Text","stored":true,"indexed":false},
    {"field":"date","field_type":"Timestamp","stored":true,"indexed":false,"facet":true}]))]
    #[serde(default)]
    pub schema: Vec<SchemaField>,
    #[serde(default = "similarity_type_api")]
    pub similarity: SimilarityType,
    #[serde(default = "tokenizer_type_api")]
    pub tokenizer: TokenizerType,
    #[serde(default)]
    pub stemmer: StemmerType,
    #[serde(default)]
    pub stop_words: StopwordType,
    #[serde(default)]
    pub frequent_words: FrequentwordType,
    #[serde(default = "ngram_indexing_api")]
    pub ngram_indexing: u8,
    #[schema(required = true, example = json!([{"terms":["berry","lingonberry","blueberry","gooseberry"],"multiway":false}]))]
    #[serde(default)]
    pub synonyms: Vec<Synonym>,
}

fn similarity_type_api() -> SimilarityType {
    SimilarityType::Bm25fProximity
}

fn tokenizer_type_api() -> TokenizerType {
    TokenizerType::UnicodeAlphanumeric
}

fn ngram_indexing_api() -> u8 {
    NgramSet::NgramFF as u8 | NgramSet::NgramFFF as u8
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DeleteApikeyRequest {
    pub apikey_base64: String,
}

/// Specifies which document and which field to return
#[derive(Debug, Clone, Deserialize, Serialize, ToSchema)]
pub struct GetDocumentRequest {
    /// query terms for highlighting
    #[serde(default)]
    pub query_terms: Vec<String>,
    /// which fields to highlight: create keyword-in-context fragments and highlight terms
    #[serde(default)]
    pub highlights: Vec<Highlight>,
    /// which fields to return
    #[serde(default)]
    pub fields: Vec<String>,
    /// which distance fields to derive and return
    #[serde(default)]
    pub distance_fields: Vec<DistanceField>,
}

#[derive(Debug, Clone, Deserialize, Serialize, ToSchema)]
pub(crate) struct IndexResponseObject {
    /// Index ID
    pub id: u64,
    /// Index name
    #[schema(example = "demo_index")]
    pub name: String,
    #[schema(example = json!({
        "title":{
            "field":"title",
            "stored":true,
            "indexed":true,
            "field_type":"Text",
            "boost":10.0,
            "field_id":0
        },
        "body":{
            "field":"body",
            "stored":true,
            "indexed":true,
            "field_type":"Text",
            "field_id":1
        },
        "url":{
           "field":"url",
           "stored":true,
           "indexed":false,
           "field_type":"Text",
           "field_id":2
        },
        "date":{
           "field":"date",
           "stored":true,
           "indexed":false,
           "field_type":"Timestamp",
           "facet":true,
           "field_id":3
        }
     }))]
    pub schema: HashMap<String, SchemaField>,
    /// Number of indexed documents
    pub indexed_doc_count: usize,
    /// Number of operations: index, update, delete, queries
    pub operations_count: u64,
    /// Number of queries, for quotas and billing
    pub query_count: u64,
    /// SeekStorm version the index was created with
    #[schema(example = "0.11.1")]
    pub version: String,
    /// Minimum and maximum values of numeric facet fields
    #[schema(example = json!({"date":{"min":831306011,"max":1730901447}}))]
    pub facets_minmax: HashMap<String, MinMaxFieldJson>,
}

/// Save file atomically
pub(crate) fn save_file_atomically(path: &PathBuf, content: String) {
    let mut temp_path = path.clone();
    temp_path.set_extension("bak");
    fs::write(&temp_path, content).unwrap();
    match fs::rename(temp_path, path) {
        Ok(_) => {}
        Err(e) => println!("error: {e:?}"),
    }
}

pub(crate) fn save_apikey_data(apikey: &ApikeyObject, index_path: &PathBuf) {
    let apikey_id: u64 = apikey.id;

    let apikey_id_path = Path::new(&index_path).join(apikey_id.to_string());
    let apikey_persistence_json = serde_json::to_string(&apikey).unwrap();
    let apikey_persistence_path = Path::new(&apikey_id_path).join(APIKEY_PATH);
    save_file_atomically(&apikey_persistence_path, apikey_persistence_json);
}

/// Create API Key
/// Creates an API key and returns the Base64 encoded API key.  
/// Expects the Base64 encoded master API key in the header.  
/// Use the master API key displayed in the server console at startup.
///  
/// WARNING: make sure to set the MASTER_KEY_SECRET environment variable to a secret, otherwise your generated API keys will be compromised.  
/// For development purposes you may also use the SeekStorm server console command 'create' to create an demo API key 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA='.
#[utoipa::path(
    tag = "API Key",
    post,
    path = "/api/v1/apikey",
    params(
        ("apikey" = String, Header, description = "YOUR_MASTER_API_KEY",example="BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB="),
    ),
    request_body = inline(ApikeyQuotaObject),
    responses(
        (status = 200, description = "API key created, returns Base64 encoded API key", body = String),
        (status = UNAUTHORIZED, description = "master_apikey invalid"),
        (status = UNAUTHORIZED, description = "master_apikey missing")
    )
)]
pub(crate) fn create_apikey_api<'a>(
    index_path: &'a PathBuf,
    apikey_quota_request_object: ApikeyQuotaObject,
    apikey: &[u8],
    apikey_list: &'a mut HashMap<u128, ApikeyObject>,
) -> &'a mut ApikeyObject {
    let apikey_hash_u128 = calculate_hash(&apikey) as u128;

    let mut apikey_id: u64 = 0;
    let mut apikey_list_vec: Vec<(&u128, &ApikeyObject)> = apikey_list.iter().collect();
    apikey_list_vec.sort_by(|a, b| a.1.id.cmp(&b.1.id));
    for value in apikey_list_vec {
        if value.1.id == apikey_id {
            apikey_id = value.1.id + 1;
        } else {
            break;
        }
    }

    let apikey_object = ApikeyObject {
        id: apikey_id,
        apikey_hash: apikey_hash_u128,
        quota: apikey_quota_request_object,
        index_list: HashMap::new(),
    };

    let apikey_id_path = Path::new(&index_path).join(apikey_id.to_string());
    fs::create_dir_all(apikey_id_path).unwrap();

    save_apikey_data(&apikey_object, index_path);

    apikey_list.insert(apikey_hash_u128, apikey_object);
    apikey_list.get_mut(&apikey_hash_u128).unwrap()
}

/// Delete API Key
/// Deletes an API and returns the number of remaining API keys.
/// Expects the Base64 encoded master API key in the header.
/// WARNING: This will delete all indices and documents associated with the API key.
#[utoipa::path(
    delete,
    tag = "API Key",
    path = "/api/v1/apikey",
    params(
        ("apikey" = String, Header, description = "YOUR_MASTER_API_KEY",example="BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB="),
    ),
    responses(
        (status = 200, description = "API key deleted, returns number of remaining API keys", body = u64),
        (status = UNAUTHORIZED, description = "master_apikey invalid"),
        (status = UNAUTHORIZED, description = "master_apikey missing")
    )
)]
pub(crate) fn delete_apikey_api(
    index_path: &PathBuf,
    apikey_list: &mut HashMap<u128, ApikeyObject>,
    apikey_hash: u128,
) -> Result<u64, String> {
    if let Some(apikey_object) = apikey_list.get(&apikey_hash) {
        let apikey_id_path = Path::new(&index_path).join(apikey_object.id.to_string());
        println!("delete path {}", apikey_id_path.to_string_lossy());
        fs::remove_dir_all(&apikey_id_path).unwrap();

        apikey_list.remove(&apikey_hash);
        Ok(apikey_list.len() as u64)
    } else {
        Err("not found".to_string())
    }
}

/// Open all indices below a single apikey
pub(crate) async fn open_all_indices(
    index_path: &PathBuf,
    index_list: &mut HashMap<u64, IndexArc>,
) {
    if !Path::exists(index_path) {
        fs::create_dir_all(index_path).unwrap();
    }

    for result in fs::read_dir(index_path).unwrap() {
        let path = result.unwrap();
        if path.path().is_dir() {
            let single_index_path = path.path();
            let Ok(index_arc) = open_index(&single_index_path, false).await else {
                continue;
            };

            let index_id = index_arc.read().await.meta.id;
            index_list.insert(index_id, index_arc);
        }
    }
}

/// Open api key
pub(crate) async fn open_apikey(
    index_path: &PathBuf,
    apikey_list: &mut HashMap<u128, ApikeyObject>,
) -> bool {
    let apikey_path = Path::new(&index_path).join(APIKEY_PATH);
    match fs::read_to_string(apikey_path) {
        Ok(apikey_string) => {
            let mut apikey_object: ApikeyObject = serde_json::from_str(&apikey_string).unwrap();

            open_all_indices(index_path, &mut apikey_object.index_list).await;
            apikey_list.insert(apikey_object.apikey_hash, apikey_object);

            true
        }
        Err(_) => false,
    }
}

/// Open all apikeys in the specified path
pub(crate) async fn open_all_apikeys(
    index_path: &PathBuf,
    apikey_list: &mut HashMap<u128, ApikeyObject>,
) -> bool {
    let mut test_index_flag = false;
    if !Path::exists(index_path) {
        println!("index path not found: {} ", index_path.to_string_lossy());
        fs::create_dir_all(index_path).unwrap();
    }

    for result in fs::read_dir(index_path).unwrap() {
        let path = result.unwrap();
        if path.path().is_dir() {
            let single_index_path = path.path();
            test_index_flag |= open_apikey(&single_index_path, apikey_list).await;
        }
    }
    test_index_flag
}

/// Create Index
/// Create an index within the directory associated with the specified API key and return the index_id.
#[utoipa::path(
    post,
    tag = "Index",
    path = "/api/v1/index",
    params(
        ("apikey" = String, Header, description = "YOUR_SECRET_API_KEY",example="AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="),
    ),
    request_body = inline(CreateIndexRequest),
    responses(
        (status = OK, description = "Index created, returns the index_id", body = u64),
        (status = BAD_REQUEST, description = "Request object incorrect"),
        (status = NOT_FOUND, description = "API key does not exists"),
        (status = UNAUTHORIZED, description = "API key is missing"),
        (status = UNAUTHORIZED, description = "API key does not exists")
    )
)]
#[allow(clippy::too_many_arguments)]
pub(crate) fn create_index_api<'a>(
    index_path: &'a PathBuf,
    index_name: String,
    schema: Vec<SchemaField>,
    similarity: SimilarityType,
    tokenizer: TokenizerType,
    stemmer: StemmerType,
    stop_words: StopwordType,
    frequent_words: FrequentwordType,
    ngram_indexing: u8,
    synonyms: Vec<Synonym>,
    apikey_object: &'a mut ApikeyObject,
) -> u64 {
    let mut index_id: u64 = 0;
    for id in apikey_object.index_list.keys().sorted() {
        if *id == index_id {
            index_id = id + 1;
        } else {
            break;
        }
    }

    let index_id_path = Path::new(&index_path)
        .join(apikey_object.id.to_string())
        .join(index_id.to_string());
    fs::create_dir_all(&index_id_path).unwrap();

    let meta = IndexMetaObject {
        id: index_id,
        name: index_name,
        similarity,
        tokenizer,
        stemmer,
        stop_words,
        frequent_words,
        ngram_indexing,
        access_type: AccessType::Mmap,
    };

    let index = create_index(&index_id_path, meta, &schema, true, &synonyms, 11, false).unwrap();

    let index_arc = Arc::new(RwLock::new(index));
    apikey_object.index_list.insert(index_id, index_arc);

    index_id
}

/// Delete Index
/// Delete an index within the directory associated with the specified API key and return the number of remaining indices.
#[utoipa::path(
    delete,
    tag = "Index",
    path = "/api/v1/index/{index_id}",
    params(
        ("apikey" = String, Header, description = "YOUR_SECRET_API_KEY",example="AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="),
        ("index_id" = u64, Path, description = "index id"),
    ),
    responses(
        (status = 200, description = "Index deleted, returns the number of indices", body = u64),
        (status = BAD_REQUEST, description = "index_id invalid or missing"),
        (status = NOT_FOUND, description = "Index_id does not exists"),
        (status = NOT_FOUND, description = "api_key does not exists"),
        (status = UNAUTHORIZED, description = "api_key does not exists"),
        (status = UNAUTHORIZED, description = "api_key missing")
    )
)]
pub(crate) async fn delete_index_api(
    index_id: u64,
    index_list: &mut HashMap<u64, IndexArc>,
) -> Result<u64, String> {
    if let Some(index_arc) = index_list.get(&index_id) {
        let mut index_mut = index_arc.write().await;
        index_mut.delete_index();
        drop(index_mut);
        index_list.remove(&index_id);

        Ok(index_list.len() as u64)
    } else {
        Err("index_id not found".to_string())
    }
}

/// Commit Index
/// Commit moves indexed documents from the intermediate uncompressed data structure (array lists/HashMap, queryable by realtime search) in RAM
/// to the final compressed data structure (roaring bitmap) on Mmap or disk -
/// which is persistent, more compact, with lower query latency and allows search with realtime=false.
/// Commit is invoked automatically each time 64K documents are newly indexed as well as on close_index (e.g. server quit).
/// There is no way to prevent this automatic commit by not manually invoking it.
/// But commit can also be invoked manually at any time at any number of newly indexed documents.
/// commit is a **hard commit** for persistence on disk. A **soft commit** for searchability
/// is invoked implicitly with every index_doc,
/// i.e. the document can immediately searched and included in the search results
/// if it matches the query AND the query paramter realtime=true is enabled.
/// **Use commit with caution, as it is an expensive operation**.
/// **Usually, there is no need to invoke it manually**, as it is invoked automatically every 64k documents and when the index is closed with close_index.
/// Before terminating the program, always call close_index (commit), otherwise all documents indexed since last (manual or automatic) commit are lost.
/// There are only 2 reasons that justify a manual commit:
/// 1. if you want to search newly indexed documents without using realtime=true for search performance reasons or
/// 2. if after indexing new documents there won't be more documents indexed (for some time),
///    so there won't be (soon) a commit invoked automatically at the next 64k threshold or close_index,
///    but you still need immediate persistence guarantees on disk to protect against data loss in the event of a crash.
#[utoipa::path(
    patch,
    tag = "Index",
    path = "/api/v1/index/{index_id}",
    params(
        ("apikey" = String, Header, description = "YOUR_SECRET_API_KEY",example="AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="),
        ("index_id" = u64, Path, description = "index id"),
    ),
    responses(
        (status = 200, description = "Index committed, returns the number of committed documents", body = u64),
        (status = BAD_REQUEST, description = "Index id invalid or missing"),
        (status = NOT_FOUND, description = "Index id does not exist"),
        (status = NOT_FOUND, description = "API key does not exist"),
        (status = UNAUTHORIZED, description = "api_key does not exists"),
        (status = UNAUTHORIZED, description = "api_key missing")
    )
)]
pub(crate) async fn commit_index_api(index_arc: &IndexArc) -> Result<u64, String> {
    let index_arc_clone = index_arc.clone();
    let index_ref = index_arc.read().await;
    let indexed_doc_count = index_ref.indexed_doc_count;

    drop(index_ref);
    index_arc_clone.commit().await;

    Ok(indexed_doc_count as u64)
}

pub(crate) async fn close_index_api(index_arc: &IndexArc) -> Result<u64, String> {
    let mut index_mut = index_arc.write().await;
    let indexed_doc_count = index_mut.indexed_doc_count;
    index_mut.close_index();
    drop(index_mut);

    Ok(indexed_doc_count as u64)
}

pub(crate) async fn set_synonyms_api(
    index_arc: &IndexArc,
    synonyms: Vec<Synonym>,
) -> Result<usize, String> {
    let mut index_mut = index_arc.write().await;
    index_mut.set_synonyms(&synonyms)
}

pub(crate) async fn add_synonyms_api(
    index_arc: &IndexArc,
    synonyms: Vec<Synonym>,
) -> Result<usize, String> {
    let mut index_mut = index_arc.write().await;
    index_mut.add_synonyms(&synonyms)
}

pub(crate) async fn get_synonyms_api(index_arc: &IndexArc) -> Result<Vec<Synonym>, String> {
    let index_ref = index_arc.read().await;
    index_ref.get_synonyms()
}

/// Get Index Info
/// Get index Info from index with index_id
#[utoipa::path(
    get,
    tag = "Index",
    path = "/api/v1/index/{index_id}",
    params(
        ("apikey" = String, Header, description = "YOUR_SECRET_API_KEY",example="AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="),
        ("index_id" = u64, Path, description = "index id"),
    ),
    responses(
        (
            status = 200, description = "Index found, returns the index info", 
            body = IndexResponseObject,
        ),
        (status = BAD_REQUEST, description = "Request object incorrect"),
        (status = NOT_FOUND, description = "Index id does not exist"),
        (status = NOT_FOUND, description = "API key does not exist"),
        (status = UNAUTHORIZED, description = "api_key does not exists"),
        (status = UNAUTHORIZED, description = "api_key missing"),
    )
)]
pub(crate) async fn get_index_info_api(
    index_id: u64,
    index_list: &HashMap<u64, IndexArc>,
) -> Result<IndexResponseObject, String> {
    if let Some(index_arc) = index_list.get(&index_id) {
        let index_ref = index_arc.read().await;

        Ok(IndexResponseObject {
            version: VERSION.to_string(),
            schema: index_ref.schema_map.clone(),
            id: index_ref.meta.id,
            name: index_ref.meta.name.clone(),
            indexed_doc_count: index_ref.indexed_doc_count,
            operations_count: 0,
            query_count: 0,
            facets_minmax: index_ref.get_index_facets_minmax(),
        })
    } else {
        Err("index_id not found".to_string())
    }
}

/// Get API Key Info
/// Get info about all indices associated with the specified API key
#[utoipa::path(
    get,
    tag = "API Key",
    path = "/api/v1/apikey",
    params(
        ("apikey" = String, Header, description = "YOUR_SECRET_API_KEY",example="AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="),
    ),
    responses(
        (
            status = 200, description = "Indices found, returns a list of index info", 
            body = Vec<IndexResponseObject>,
        ),
        (status = BAD_REQUEST, description = "Request object incorrect"),
        (status = NOT_FOUND, description = "Index ID or API key missing"),
        (status = UNAUTHORIZED, description = "API key does not exists"),
    )
)]
pub(crate) async fn get_apikey_indices_info_api(
    index_list: &HashMap<u64, IndexArc>,
) -> Result<Vec<IndexResponseObject>, String> {
    let mut index_response_object_vec: Vec<IndexResponseObject> = Vec::new();
    for index in index_list.iter() {
        let index_ref = index.1.read().await;

        index_response_object_vec.push(IndexResponseObject {
            version: VERSION.to_string(),
            schema: index_ref.schema_map.clone(),
            id: index_ref.meta.id,
            name: index_ref.meta.name.clone(),
            indexed_doc_count: index_ref.indexed_doc_count,
            operations_count: 0,
            query_count: 0,
            facets_minmax: index_ref.get_index_facets_minmax(),
        });
    }

    Ok(index_response_object_vec)
}

/// Index Document(s)
/// Index a JSON document or an array of JSON documents (bulk), each consisting of arbitrary key-value pairs to the index with the specified apikey and index_id, and return the number of indexed docs.
/// Index documents enables true real-time search (as opposed to near realtime.search):
/// When in query_index the parameter `realtime` is set to `true` then indexed, but uncommitted documents are immediately included in the search results, without requiring a commit or refresh.
/// Therefore a explicit commit_index is almost never required, as it is invoked automatically after 64k documents are indexed or on close_index for persistence.
#[utoipa::path(
    post,
    tag = "Document",
    path = "/api/v1/index/{index_id}/doc",
    params(
        ("apikey" = String, Header, description = "YOUR_SECRET_API_KEY",example="AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="),
        ("index_id" = u64, Path, description = "index id"),
    ),

    request_body(content = HashMap<String, Value>, description = "JSON document or array of JSON documents, each consisting of key-value pairs", content_type = "application/json", example=json!({"title":"title1 test","body":"body1","url":"url1"})),
    responses(
        (status = 200, description = "Document indexed, returns the number of indexed documents", body = usize),
        (status = BAD_REQUEST, description = "Document object invalid"),
        (status = NOT_FOUND, description = "Index id does not exist"),
        (status = NOT_FOUND, description = "API key does not exist"),
        (status = UNAUTHORIZED, description = "api_key does not exists"),
        (status = UNAUTHORIZED, description = "api_key missing")
    )
)]
pub(crate) async fn index_document_api(
    index_arc: &IndexArc,
    document: Document,
) -> Result<usize, String> {
    index_arc.index_document(document, FileType::None).await;

    Ok(index_arc.read().await.indexed_doc_count)
}

/// Index PDF file
/// Index PDF file (byte array) to the index with the specified apikey and index_id, and return the number of indexed docs.
/// - Converts PDF to a JSON document with "title", "body", "url" and "date" fields and indexes it.
/// - extracts title from metatag, or first line of text, or from filename
/// - extracts creation date from metatag, or from file creation date (Unix timestamp: the number of seconds since 1 January 1970)
/// - copies all ingested pdf files to "files" subdirectory in index
#[utoipa::path(
    post,
    tag = "PDF File",
    path = "/api/v1/index/{index_id}/file",
    params(
        ("apikey" = String, Header, description = "YOUR_SECRET_API_KEY",example="AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="),
        ("file" = String, Header, description = "filepath from header for JSON 'url' field"),
        ("date" = String, Header, description = "date (timestamp) from header, as fallback for JSON 'date' field, if PDF date meta tag unaivailable"),
        ("index_id" = u64, Path, description = "index id"),
    ),
    request_body = inline(&[u8]),
    responses(
        (status = 200, description = "PDF file indexed, returns the number of indexed documents", body = usize),
        (status = BAD_REQUEST, description = "Document object invalid"),
        (status = NOT_FOUND, description = "Index id does not exist"),
        (status = NOT_FOUND, description = "API key does not exist"),
        (status = UNAUTHORIZED, description = "api_key does not exists"),
        (status = UNAUTHORIZED, description = "api_key missing")
    )
)]
pub(crate) async fn index_file_api(
    index_arc: &IndexArc,
    file_path: &Path,
    file_date: i64,
    document: &[u8],
) -> Result<usize, String> {
    match index_arc
        .index_pdf_bytes(file_path, file_date, document)
        .await
    {
        Ok(_) => Ok(index_arc.read().await.indexed_doc_count),
        Err(e) => Err(e),
    }
}

/// Get PDF file
/// Get PDF file from index with index_id
#[utoipa::path(
    get,
    tag = "PDF File",
    path = "/api/v1/index/{index_id}/file/{document_id}",
    params(
        ("apikey" = String, Header, description = "YOUR_SECRET_API_KEY",example="AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="),
        ("index_id" = u64, Path, description = "index id"),
        ("document_id" = u64, Path, description = "document id"),
    ),
    responses(
        (status = 200, description = "PDF file found, returns the PDF file as byte array", body = [u8]),
        (status = BAD_REQUEST, description = "index_id invalid or missing"),
        (status = BAD_REQUEST, description = "doc_id invalid or missing"),
        (status = BAD_REQUEST, description = "Request object incorrect"),
        (status = NOT_FOUND, description = "Index id does not exist"),
        (status = NOT_FOUND, description = "Document id does not exist"),
        (status = NOT_FOUND, description = "api_key does not exists"),
        (status = UNAUTHORIZED, description = "api_key does not exists"),
        (status = UNAUTHORIZED, description = "api_key missing"),
    )
)]
pub(crate) async fn get_file_api(index_arc: &IndexArc, document_id: usize) -> Option<Vec<u8>> {
    if !index_arc.read().await.stored_field_names.is_empty() {
        index_arc.read().await.get_file(document_id).ok()
    } else {
        None
    }
}

pub(crate) async fn index_documents_api(
    index_arc: &IndexArc,
    document_vec: Vec<Document>,
) -> Result<usize, String> {
    index_arc.index_documents(document_vec).await;
    Ok(index_arc.read().await.indexed_doc_count)
}

/// Get Document
/// Get document from index with index_id
#[utoipa::path(
    get,
    tag = "Document",
    path = "/api/v1/index/{index_id}/doc/{document_id}",
    params(
        ("apikey" = String, Header, description = "YOUR_SECRET_API_KEY",example="AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="),
        ("index_id" = u64, Path, description = "index id"),
        ("document_id" = u64, Path, description = "document id"),
    ),
    request_body(content = GetDocumentRequest, example=json!({
        "query_terms": ["test"],
        "fields": ["title", "body"],
        "highlights": [
        { "field": "title", "fragment_number": 0, "fragment_size": 1000, "highlight_markup": true},
        { "field": "body", "fragment_number": 2, "fragment_size": 160, "highlight_markup": true},
        { "field": "body", "name": "body2", "fragment_number": 0, "fragment_size": 4000, "highlight_markup": true}]
    })),
    responses(
        (status = 200, description = "Document found, returns the JSON document consisting of arbitrary key-value pairs", body = HashMap<String, Value>),
        (status = BAD_REQUEST, description = "index_id invalid or missing"),
        (status = BAD_REQUEST, description = "doc_id invalid or missing"),
        (status = BAD_REQUEST, description = "Request object incorrect"),
        (status = NOT_FOUND, description = "Index id does not exist"),
        (status = NOT_FOUND, description = "Document id does not exist"),
        (status = NOT_FOUND, description = "api_key does not exists"),
        (status = UNAUTHORIZED, description = "api_key does not exists"),
        (status = UNAUTHORIZED, description = "api_key missing"),
    )
)]
pub(crate) async fn get_document_api(
    index_arc: &IndexArc,
    document_id: usize,
    get_document_request: GetDocumentRequest,
) -> Option<Document> {
    if !index_arc.read().await.stored_field_names.is_empty() {
        let highlighter_option = if get_document_request.highlights.is_empty()
            || get_document_request.query_terms.is_empty()
        {
            None
        } else {
            Some(
                highlighter(
                    index_arc,
                    get_document_request.highlights,
                    get_document_request.query_terms,
                )
                .await,
            )
        };

        index_arc
            .read()
            .await
            .get_document(
                document_id,
                true,
                &highlighter_option,
                &HashSet::from_iter(get_document_request.fields),
                &get_document_request.distance_fields,
            )
            .ok()
    } else {
        None
    }
}

/// Update Document(s)
/// Update a JSON document or an array of JSON documents (bulk), each consisting of arbitrary key-value pairs to the index with the specified apikey and index_id, and return the number of indexed docs.
/// Update document is a combination of delete_document and index_document.
/// All current limitations of delete_document apply.
/// Update documents enables true real-time search (as opposed to near realtime.search):
/// When in query_index the parameter `realtime` is set to `true` then indexed, but uncommitted documents are immediately included in the search results, without requiring a commit or refresh.
/// Therefore a explicit commit_index is almost never required, as it is invoked automatically after 64k documents are indexed or on close_index for persistence.
#[utoipa::path(
    patch,
    tag = "Document",
    path = "/api/v1/index/{index_id}/doc",
    params(
        ("apikey" = String, Header, description = "YOUR_SECRET_API_KEY",example="AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="),
        ("index_id" = u64, Path, description = "index id"),
    ),
    request_body(content = (u64, HashMap<String, Value>), description = "Tuple of (doc_id, JSON document) or array of tuples (doc_id, JSON documents), each JSON document consisting of arbitrary key-value pairs", content_type = "application/json", example=json!([0,{"title":"title1 test","body":"body1","url":"url1"}])),
    responses(
        (status = 200, description = "Document indexed, returns the number of indexed documents", body = usize),
        (status = BAD_REQUEST, description = "Document object invalid"),
        (status = NOT_FOUND, description = "Index id does not exist"),
        (status = NOT_FOUND, description = "API key does not exist"),
        (status = UNAUTHORIZED, description = "api_key does not exists"),
        (status = UNAUTHORIZED, description = "api_key missing")
    )
)]
pub(crate) async fn update_document_api(
    index_arc: &IndexArc,
    id_document: (u64, Document),
) -> Result<u64, String> {
    index_arc.update_document(id_document).await;
    Ok(index_arc.read().await.indexed_doc_count as u64)
}

pub(crate) async fn update_documents_api(
    index_arc: &IndexArc,
    id_document_vec: Vec<(u64, Document)>,
) -> Result<u64, String> {
    index_arc.update_documents(id_document_vec).await;
    Ok(index_arc.read().await.indexed_doc_count as u64)
}

/// Delete Document
/// Delete document by document_id from index with index_id
/// Document ID can by obtained by search.
/// Immediately effective, indpendent of commit.
/// Index space used by deleted documents is not reclaimed (until compaction is implemented), but result_count_total is updated.
/// By manually deleting the delete.bin file the deleted documents can be recovered (until compaction).
/// Deleted documents impact performance, especially but not limited to counting (Count, TopKCount). They also increase the size of the index (until compaction is implemented).
/// For minimal query latency delete index and reindexing documents is preferred over deleting documents (until compaction is implemented).
/// BM25 scores are not updated (until compaction is implemented), but the impact is minimal.
#[utoipa::path(
    delete,
    tag = "Document",
    path = "/api/v1/index/{index_id}/doc/{document_id}",
    params(
        ("apikey" = String, Header, description = "YOUR_SECRET_API_KEY",example="AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="),
        ("index_id" = u64, Path, description = "index id"),
        ("document_id" = u64, Path, description = "document id"),
    ),
    responses(
        (status = 200, description = "Document deleted, returns indexed documents count", body = usize),
        (status = BAD_REQUEST, description = "index_id invalid or missing"),
        (status = BAD_REQUEST, description = "doc_id invalid or missing"),
        (status = BAD_REQUEST, description = "Request object incorrect"),
        (status = NOT_FOUND, description = "Index id does not exist"),
        (status = NOT_FOUND, description = "Document id does not exist"),
        (status = NOT_FOUND, description = "api_key does not exists"),
        (status = UNAUTHORIZED, description = "api_key does not exists"),
        (status = UNAUTHORIZED, description = "api_key missing"),
    )
)]
pub(crate) async fn delete_document_by_parameter_api(
    index_arc: &IndexArc,
    document_id: u64,
) -> Result<u64, String> {
    index_arc.delete_document(document_id).await;
    Ok(index_arc.read().await.indexed_doc_count as u64)
}

/// Delete Document(s) by Request Object
/// Delete document by document_id, by array of document_id (bulk), by query (SearchRequestObject) from index with index_id, or clear all documents from index.
/// Immediately effective, indpendent of commit.
/// Index space used by deleted documents is not reclaimed (until compaction is implemented), but result_count_total is updated.
/// By manually deleting the delete.bin file the deleted documents can be recovered (until compaction).
/// Deleted documents impact performance, especially but not limited to counting (Count, TopKCount). They also increase the size of the index (until compaction is implemented).
/// For minimal query latency delete index and reindexing documents is preferred over deleting documents (until compaction is implemented).
/// BM25 scores are not updated (until compaction is implemented), but the impact is minimal.
/// Document ID can by obtained by search. When deleting by query (SearchRequestObject), it is advised to perform a dry run search first, to see which documents will be deleted.
#[utoipa::path(
    delete,
    tag = "Document",
    path = "/api/v1/index/{index_id}/doc",
    params(
        ("apikey" = String, Header, description = "YOUR_SECRET_API_KEY",example="AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="),
        ("index_id" = u64, Path, description = "index id"),
    ),
    request_body(content = SearchRequestObject, description = "Specifies the document(s) to delete by different request objects\n- 'clear' : delete all documents in index (clear index)\n- u64 : delete single doc ID\n- [u64] : delete array of doc ID \n- SearchRequestObject : delete documents by query", content_type = "application/json", example=json!({
        "query":"test",
        "offset":0,
        "length":10,
        "realtime": false,
        "field_filter": ["title", "body"]
    })),

    responses(
        (status = 200, description = "Document deleted, returns indexed documents count", body = usize),
        (status = BAD_REQUEST, description = "index_id invalid or missing"),
        (status = BAD_REQUEST, description = "doc_id invalid or missing"),
        (status = BAD_REQUEST, description = "Request object incorrect"),
        (status = NOT_FOUND, description = "Index id does not exist"),
        (status = NOT_FOUND, description = "Document id does not exist"),
        (status = NOT_FOUND, description = "api_key does not exists"),
        (status = UNAUTHORIZED, description = "api_key does not exists"),
        (status = UNAUTHORIZED, description = "api_key missing"),
    )
)]
pub(crate) async fn delete_document_by_object_api(
    index_arc: &IndexArc,
    document_id: u64,
) -> Result<u64, String> {
    index_arc.delete_document(document_id).await;
    Ok(index_arc.read().await.indexed_doc_count as u64)
}

pub(crate) async fn delete_documents_by_object_api(
    index_arc: &IndexArc,
    document_id_vec: Vec<u64>,
) -> Result<u64, String> {
    index_arc.delete_documents(document_id_vec).await;
    Ok(index_arc.read().await.indexed_doc_count as u64)
}

pub(crate) async fn delete_documents_by_query_api(
    index_arc: &IndexArc,
    search_request: SearchRequestObject,
) -> Result<u64, String> {
    index_arc
        .delete_documents_by_query(
            search_request.query_string.to_owned(),
            search_request.query_type_default,
            search_request.offset,
            search_request.length,
            search_request.realtime,
            search_request.field_filter,
            search_request.facet_filter,
            search_request.result_sort,
        )
        .await;

    Ok(index_arc.read().await.indexed_doc_count as u64)
}

pub(crate) async fn clear_index_api(index_arc: &IndexArc) -> Result<u64, String> {
    let mut index_mut = index_arc.write().await;
    index_mut.clear_index();
    Ok(index_arc.read().await.indexed_doc_count as u64)
}

/// Query Index
/// Query results from index with index_id
/// The following parameters are supported:
/// - Result type
/// - Result sorting
/// - Realtime search
/// - Field filter
/// - Fields to include in search results
/// - Distance fields: derived fields from distance calculations
/// - Highlights: keyword-in-context snippets and term highlighting
/// - Query facets: which facets fields to calculate and return at query time
/// - Facet filter: filter facets by field and value
/// - Result sort: sort results by field and direction
/// - Query type default: default query type, if not specified in query
#[utoipa::path(
    post,
    tag = "Query",
    path = "/api/v1/index/{index_id}/query",
    params(
        ("apikey" = String, Header, description = "YOUR_SECRET_API_KEY",example="AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="),
        ("index_id" = u64, Path, description = "index id"),
    ),
    request_body = inline(SearchRequestObject),
    responses(
        (status = 200, description = "Results found, returns the SearchResultObject", body = SearchResultObject),
        (status = BAD_REQUEST, description = "Request object incorrect"),
        (status = NOT_FOUND, description = "Index id does not exist"),
        (status = NOT_FOUND, description = "API key does not exist"),
        (status = UNAUTHORIZED, description = "api_key does not exists"),
        (status = UNAUTHORIZED, description = "api_key missing"),
    )
)]
pub(crate) async fn query_index_api_post(
    index_arc: &IndexArc,
    search_request: SearchRequestObject,
) -> SearchResultObject {
    query_index_api(index_arc, search_request).await
}

/// Query Index
/// Query results from index with index_id.
/// Query index via GET is a convenience function, that offers only a limited set of parameters compared to Query Index via POST.
#[utoipa::path(
    get,
    tag = "Query",
    path = "/api/v1/index/{index_id}/query",
    params(
        ("apikey" = String, Header, description = "YOUR_SECRET_API_KEY",example="AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="),
        ("index_id" = u64, Path, description = "index id", example=0),
        ("query" = String, Query,  description = "query string", example="hello"),
        ("offset" = u64, Query,  description = "result offset", minimum = 0, example=0),
        ("length" = u64, Query,  description = "result length", minimum = 1, example=10),
        ("realtime" = bool, Query,  description = "include uncommitted documents", example=false)
    ),
    responses(
        (status = 200, description = "Results found, returns the SearchResultObject", body = SearchResultObject),
        (status = BAD_REQUEST, description = "No query specified"),
        (status = NOT_FOUND, description = "Index id does not exist"),
        (status = NOT_FOUND, description = "API key does not exist"),
        (status = UNAUTHORIZED, description = "api_key does not exists"),
        (status = UNAUTHORIZED, description = "api_key missing"),
    )
)]
pub(crate) async fn query_index_api_get(
    index_arc: &IndexArc,
    search_request: SearchRequestObject,
) -> SearchResultObject {
    query_index_api(index_arc, search_request).await
}

pub(crate) async fn query_index_api(
    index_arc: &IndexArc,
    search_request: SearchRequestObject,
) -> SearchResultObject {
    let start_time = Instant::now();

    let result_object = index_arc
        .search(
            search_request.query_string.to_owned(),
            search_request.query_type_default,
            search_request.offset,
            search_request.length,
            search_request.result_type,
            search_request.realtime,
            search_request.field_filter,
            search_request.query_facets,
            search_request.facet_filter,
            search_request.result_sort,
        )
        .await;

    let elapsed_time = start_time.elapsed().as_nanos();

    let return_fields_filter = HashSet::from_iter(search_request.fields);

    let mut results: Vec<Document> = Vec::new();

    if !index_arc.read().await.stored_field_names.is_empty() {
        let highlighter_option = if search_request.highlights.is_empty() {
            None
        } else {
            Some(
                highlighter(
                    index_arc,
                    search_request.highlights,
                    result_object.query_terms.clone(),
                )
                .await,
            )
        };

        for result in result_object.results.iter() {
            match index_arc.read().await.get_document(
                result.doc_id,
                search_request.realtime,
                &highlighter_option,
                &return_fields_filter,
                &search_request.distance_fields,
            ) {
                Ok(doc) => {
                    let mut doc = doc;
                    doc.insert("_id".to_string(), result.doc_id.into());
                    doc.insert("_score".to_string(), result.score.into());

                    results.push(doc);
                }
                Err(_e) => {}
            }
        }
    }

    SearchResultObject {
        query: search_request.query_string.to_owned(),
        time: elapsed_time,
        offset: search_request.offset,
        length: search_request.length,
        count: result_object.results.len(),
        count_total: result_object.result_count_total,
        query_terms: result_object.query_terms,
        results,
        facets: result_object.facets,
        suggestions: Vec::new(),
    }
}

#[derive(OpenApi, Default)]
#[openapi(paths(
    create_apikey_api,
    get_apikey_indices_info_api,
    delete_apikey_api,
    create_index_api,
    get_index_info_api,
    commit_index_api,
    delete_index_api,
    index_document_api,
    update_document_api,
    index_file_api,
    get_document_api,
    get_file_api,
    delete_document_by_parameter_api,
    delete_document_by_object_api,
    query_index_api_post,
    query_index_api_get,
),
tags(
    (name="API Key", description="Create and delete API keys"),
    (name="Index", description="Create and delete indices"),
    (name="Document", description="Index, update, get and delete documents"),
    (name="PDF File", description="Index, and get PDF file"),
    (name="Query", description="Query an index"),
)
)]
#[openapi(info(title = "SeekStorm REST API documentation"))]
#[openapi(servers((url = "http://127.0.0.1", description = "Local SeekStorm server")))]
struct ApiDoc;

pub fn generate_openapi() {
    let openapi = ApiDoc::openapi();

    println!("{}", openapi.to_pretty_json().unwrap());

    let mut path = current_exe().unwrap();
    path.pop();
    let path_json = path.join("openapi.json");
    let path_yml = path.join("openapi.yml");

    serde_json::to_writer_pretty(&File::create(path_json.clone()).unwrap(), &openapi).unwrap();
    fs::write(path_yml.clone(), openapi.to_yaml().unwrap()).unwrap();

    println!(
        "OpenAPI documents generated: {} {}",
        path_json.to_string_lossy(),
        path_yml.to_string_lossy()
    );
}
