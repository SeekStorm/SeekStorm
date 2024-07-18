use std::{
    collections::HashMap,
    fs::{self},
    path::{Path, PathBuf},
    sync::Arc,
    time::Instant,
};

use derivative::Derivative;
use itertools::Itertools;
use std::collections::HashSet;

use seekstorm::{
    commit::Commit,
    highlighter::{highlighter, Highlight},
    index::{
        create_index, open_index, AccessType, Document, IndexArc, IndexDocument, IndexDocuments,
        IndexMetaObject, SchemaField, SimilarityType, TokenizerType,
    },
    search::{QueryType, ResultType, Search},
};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::{
    http_server::calculate_hash,
    multi_tenancy::{ApikeyObject, ApikeyQuotaObject},
    VERSION,
};

const APIKEY_PATH: &str = "apikey.json";

#[derive(Debug, Deserialize, Serialize, Clone, Derivative)]
pub struct SearchRequestObject {
    #[serde(rename = "query")]
    pub query_string: String,
    pub offset: usize,
    pub length: usize,
    #[serde(default)]
    #[derivative(Default(value = "false"))]
    pub realtime: bool,
    #[serde(default)]
    pub highlights: Vec<Highlight>,
    #[serde(default)]
    pub field_filter: Vec<String>,
    #[serde(default)]
    pub fields: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ResultObject {
    pub time: u128,
    pub query: String,
    pub offset: usize,
    pub length: usize,
    pub count: usize,
    pub count_total: usize,
    pub results: Vec<Document>,
    pub suggestions: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, Derivative)]
pub struct CreateIndexRequest {
    pub index_name: String,
    #[serde(default)]
    pub schema: Vec<SchemaField>,
    #[derivative(Default(value = "Bm25f"))]
    pub similarity: SimilarityType,
    #[derivative(Default(value = "Alpha"))]
    pub tokenizer: TokenizerType,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DeleteApikeyRequest {
    pub apikey_base64: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct GetDocumentRequest {
    #[serde(default)]
    pub query_terms: Vec<String>,
    #[serde(default)]
    pub highlights: Vec<Highlight>,
    #[serde(default)]
    pub fields: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub(crate) struct IndexResponseObject {
    pub id: u64,
    pub name: String,
    pub schema: HashMap<String, SchemaField>,
    pub indexed_doc_count: usize,
    pub operations_count: u64,
    pub query_count: u64,
    pub version: String,
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

pub(crate) fn create_index_api<'a>(
    index_path: &'a PathBuf,
    index_name: String,
    schema: Vec<SchemaField>,
    similarity: SimilarityType,
    tokenizer: TokenizerType,
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
        access_type: AccessType::Mmap,
    };

    let index = create_index(&index_id_path, meta, &schema, true, 11, false).unwrap();

    let index_arc = Arc::new(RwLock::new(index));
    apikey_object.index_list.insert(index_id, index_arc);

    index_id
}

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

pub(crate) async fn commit_index_api(index_arc: &IndexArc) -> Result<u64, String> {
    let mut index_arc_clone = index_arc.clone();
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

pub(crate) async fn get_index_stats_api(
    _index_path: &Path,
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
        })
    } else {
        Err("index_id not found".to_string())
    }
}

pub(crate) async fn get_all_index_stats_api(
    _index_path: &Path,
    _index_list: &HashMap<u64, IndexArc>,
) -> Result<Vec<IndexResponseObject>, String> {
    Err("err".to_string())
}

pub(crate) async fn index_document_api(
    index: &IndexArc,
    document: Document,
) -> Result<usize, String> {
    index.index_document(document).await;
    Ok(index.read().await.indexed_doc_count)
}

pub(crate) async fn index_documents_api(
    index: &IndexArc,
    document_vec: Vec<Document>,
) -> Result<usize, String> {
    index.index_documents(document_vec).await;
    Ok(index.read().await.indexed_doc_count)
}

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
            Some(highlighter(
                get_document_request.highlights,
                get_document_request.query_terms,
            ))
        };

        match index_arc.read().await.get_document(
            document_id,
            true,
            &highlighter_option,
            &HashSet::from_iter(get_document_request.fields),
        ) {
            Ok(doc) => Some(doc),
            Err(_e) => None,
        }
    } else {
        None
    }
}

pub(crate) async fn update_document_api(
    _index: &IndexArc,
    _document: Document,
) -> Result<u64, String> {
    Ok(0)
}

pub(crate) async fn update_documents_api(
    _index: &IndexArc,
    _document_vec: Vec<Document>,
) -> Result<u64, String> {
    Ok(0)
}

pub(crate) async fn delete_document_api(
    _index: &IndexArc,
    _document_id: String,
) -> Result<u64, String> {
    Ok(0)
}

pub(crate) async fn delete_documents_api(
    _index: &IndexArc,
    _document_id_vec: Vec<String>,
) -> Result<u64, String> {
    Ok(0)
}

pub(crate) async fn query_index_api(
    index_arc: &IndexArc,
    search_request: SearchRequestObject,
) -> ResultObject {
    let start_time = Instant::now();

    let rlo = index_arc
        .search(
            search_request.query_string.to_owned(),
            QueryType::Intersection,
            search_request.offset,
            search_request.length,
            ResultType::TopkCount,
            search_request.realtime,
            search_request.field_filter,
        )
        .await;

    let fields_hashset = HashSet::from_iter(search_request.fields);

    let mut results: Vec<Document> = Vec::new();

    if !index_arc.read().await.stored_field_names.is_empty() {
        let highlighter_option = if search_request.highlights.is_empty() {
            None
        } else {
            Some(highlighter(
                search_request.highlights,
                rlo.query_term_strings,
            ))
        };

        for result in rlo.results.iter() {
            match index_arc.read().await.get_document(
                result.doc_id,
                search_request.realtime,
                &highlighter_option,
                &fields_hashset,
            ) {
                Ok(doc) => {
                    results.push(doc);
                }
                Err(_e) => {}
            }
        }
    }

    ResultObject {
        query: search_request.query_string.to_owned(),
        time: start_time.elapsed().as_nanos(),
        offset: search_request.offset,
        length: search_request.length,
        count: rlo.results.len(),
        count_total: rlo.result_count_total as usize,
        results,
        suggestions: Vec::new(),
    }
}
