use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::io;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process;
use std::str::{self, from_utf8};
use std::sync::Arc;
use std::{convert::Infallible, net::SocketAddr};

use chrono::Utc;
use rand::TryRngCore;
use rand::rngs::OsRng;

use http_body_util::{BodyExt, Full, combinators::BoxBody};
use hyper::{
    Method, Request, Response, StatusCode,
    body::{Bytes, Incoming},
    service::service_fn,
};
use hyper_util::{
    rt::{TokioExecutor, TokioIo},
    server::conn::auto::Builder,
};

use seekstorm::index::{Document, Synonym};
use seekstorm::search::{QueryRewriting, QueryType, ResultType};

use sha2::Digest;
use sha2::Sha256;

use tokio::net::TcpListener;

use base64::{Engine as _, engine::general_purpose};

use crate::api_endpoints::{CreateIndexRequest, hello_api};
use crate::api_endpoints::DeleteApikeyRequest;
use crate::api_endpoints::update_document_api;
use crate::api_endpoints::update_documents_api;
use crate::api_endpoints::{GetDocumentRequest, delete_apikey_api};
use crate::api_endpoints::{SearchRequestObject, create_index_api};
use crate::api_endpoints::{add_synonyms_api, get_index_info_api, set_synonyms_api};
use crate::api_endpoints::{clear_index_api, close_index_api};
use crate::api_endpoints::{commit_index_api, create_apikey_api};
use crate::api_endpoints::{
    delete_document_by_object_api, delete_document_by_parameter_api, index_documents_api,
};
use crate::api_endpoints::{delete_documents_by_object_api, delete_documents_by_query_api};
use crate::api_endpoints::{delete_index_api, get_file_api};
use crate::api_endpoints::{get_apikey_indices_info_api, index_file_api};
use crate::api_endpoints::{get_document_api, get_synonyms_api};
use crate::api_endpoints::{index_document_api, query_index_api_get, query_index_api_post};
use crate::multi_tenancy::ApikeyObject;
use crate::multi_tenancy::get_apikey_hash;
use crate::{MASTER_KEY_SECRET, VERSION};

const INDEX_HTML: &str = include_str!("web/index.html");
const FLEXBOX_CSS: &str = include_str!("web/css/flexboxgrid.min.css");
const MASTER_CSS: &str = include_str!("web/css/master.css");
const MASTER_JS: &str = include_str!("web/js/master.js");
const JQUERY_JS: &str = include_str!("web/js/jquery-3.7.1.min.js");
const LOGO_SVG: &[u8] = include_bytes!("web/svg/logo.svg");
const FAVICON_16: &[u8] = include_bytes!("web/favicon-16x16.png");
const FAVICON_32: &[u8] = include_bytes!("web/favicon-32x32.png");

const HISTOGRAM_CSS: &str = include_str!("web/css/bootstrap.histogram.slider.css");
const SLIDER_CSS: &str = include_str!("web/css/histogram.slider.css");
const HISTOGRAM_JS: &str = include_str!("web/js/bootstrap.histogram.slider.js");
const SLIDER_JS: &str = include_str!("web/js/bootstrap-slider.js");

pub(crate) fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}

pub(crate) fn status(
    status: StatusCode,
    error_message: String,
) -> Response<BoxBody<Bytes, Infallible>> {
    Response::builder()
        .status(status)
        .body(BoxBody::new(Full::new(error_message.into())))
        .unwrap()
}

enum HttpServerError {
    IndexNotFound,
    ApiKeyNotFound,
    SynonymsNotFound,
    Unauthorized,
    BadRequest(String),
    NotImplemented,
    FileNotFound,
    DocumentNotFound,
}

impl From<HttpServerError> for Result<Response<BoxBody<Bytes, Infallible>>, Infallible> {
    fn from(error: HttpServerError) -> Self {
        Ok(match error {
            HttpServerError::IndexNotFound => {
                status(StatusCode::NOT_FOUND, "index does not exist".to_string())
            }
            HttpServerError::ApiKeyNotFound => {
                status(StatusCode::NOT_FOUND, "apikey does not exist".to_string())
            }
            HttpServerError::SynonymsNotFound => {
                status(StatusCode::NOT_FOUND, "synonyms not found".to_string())
            }
            HttpServerError::Unauthorized => status(
                StatusCode::UNAUTHORIZED,
                "apikey invalid or missing".to_string(),
            ),
            HttpServerError::BadRequest(error_message) => status(
                StatusCode::BAD_REQUEST,
                format!("bad request:{}", error_message),
            ),
            HttpServerError::NotImplemented => {
                status(StatusCode::NOT_IMPLEMENTED, "not implemented".to_string())
            }
            HttpServerError::FileNotFound => {
                status(StatusCode::NOT_FOUND, "file not found".to_string())
            }
            HttpServerError::DocumentNotFound => {
                status(StatusCode::NOT_FOUND, "document not found".to_string())
            }
        })
    }
}

pub(crate) async fn http_request_handler(
    index_path: PathBuf,
    apikey_list: Arc<tokio::sync::RwLock<HashMap<u128, ApikeyObject>>>,
    req: Request<Incoming>,
    _remote_addr: SocketAddr,
) -> Result<Response<BoxBody<Bytes, Infallible>>, Infallible> {
    let apikey_header = req
        .headers()
        .get("apikey")
        .map(|v| v.to_str().unwrap_or("").to_string());

    let mut parts: [&str; 6] = ["", "", "", "", "", ""];
    let mut i = 0;
    let path = req.uri().path();
    for part in path.split('/') {
        if part.is_empty() {
            continue;
        }
        parts[i] = part;

        i += 1;

        if i >= parts.len() {
            break;
        }
    }

    match (
        parts[0],
        parts[1],
        parts[2],
        parts[3],
        parts[4],
        parts[5],
        req.method(),
    ) {
        ("api", "v1", "hello", _, _, _, &Method::GET) => {
            let hello_message =
                serde_json::to_vec(&hello_api()).unwrap();
            Ok(Response::new(BoxBody::new(Full::new(
                hello_message.into(),
            ))))
        }

        ("api", "v1", "index", _, "query", _, &Method::POST) => {
            let Some(apikey) = apikey_header else {
                return HttpServerError::Unauthorized.into();
            };
            let Some(apikey_hash) = get_apikey_hash(apikey, &apikey_list).await else {
                return HttpServerError::Unauthorized.into();
            };

            let Ok(index_id) = parts[3].parse() else {
                return HttpServerError::IndexNotFound.into();
            };

            let apikey_list_ref = apikey_list.read().await;
            let Some(apikey_object) = apikey_list_ref.get(&apikey_hash) else {
                return HttpServerError::Unauthorized.into();
            };

            let Some(index_arc) = apikey_object.index_list.get(&index_id) else {
                return HttpServerError::IndexNotFound.into();
            };

            let index_arc_clone = index_arc.clone();
            drop(apikey_list_ref);

            let request_bytes = req.into_body().collect().await.unwrap().to_bytes();
            let search_request = match serde_json::from_slice::<SearchRequestObject>(&request_bytes)
            {
                Ok(search_request) => search_request,
                Err(e) => {
                    return HttpServerError::BadRequest(e.to_string()).into();
                }
            };

            let search_result_local = query_index_api_post(&index_arc_clone, search_request).await;

            let search_result_json = serde_json::to_vec(&search_result_local).unwrap();
            Ok(Response::new(BoxBody::new(Full::new(
                search_result_json.into(),
            ))))
        }

        ("api", "v1", "index", _, "query", _, &Method::GET) => {
            let Some(apikey) = apikey_header else {
                return HttpServerError::Unauthorized.into();
            };
            let Some(apikey_hash) = get_apikey_hash(apikey, &apikey_list).await else {
                return HttpServerError::Unauthorized.into();
            };

            let Ok(index_id) = parts[3].parse() else {
                return HttpServerError::IndexNotFound.into();
            };

            let apikey_list_ref = apikey_list.read().await;
            let Some(apikey_object) = apikey_list_ref.get(&apikey_hash) else {
                return HttpServerError::Unauthorized.into();
            };

            let Some(index_arc) = apikey_object.index_list.get(&index_id) else {
                return HttpServerError::IndexNotFound.into();
            };

            let index_arc_clone = index_arc.clone();
            drop(apikey_list_ref);

            let params: HashMap<String, String> = req
                .uri()
                .query()
                .map(|v| {
                    url::form_urlencoded::parse(v.as_bytes())
                        .into_owned()
                        .collect()
                })
                .unwrap_or_default();

            let search_request = if !params.is_empty() {
                let query_string = if let Some(query_string) = params.get("query") {
                    query_string.to_string()
                } else {
                    "".to_string()
                };

                let offset = if let Some(value) = params.get("offset") {
                    let Ok(api_offset) = value.parse::<usize>() else {
                        return HttpServerError::BadRequest(
                            "api_offset invalid or missing".to_string(),
                        )
                        .into();
                    };
                    api_offset
                } else {
                    0
                };

                let length = if let Some(value) = params.get("length") {
                    let Ok(api_length) = value.parse::<usize>() else {
                        return HttpServerError::BadRequest(
                            "api_length invalid or missing".to_string(),
                        )
                        .into();
                    };
                    api_length
                } else {
                    10
                };

                let realtime = if let Some(value) = params.get("realtime") {
                    let Ok(realtime) = value.parse::<bool>() else {
                        return HttpServerError::BadRequest(
                            "api_length invalid or missing".to_string(),
                        )
                        .into();
                    };
                    realtime
                } else {
                    true
                };

                SearchRequestObject {
                    query_string,
                    offset,
                    length,
                    result_type: ResultType::default(),
                    realtime,
                    highlights: Vec::new(),
                    field_filter: Vec::new(),
                    fields: Vec::new(),
                    distance_fields: Vec::new(),
                    query_facets: Vec::new(),
                    facet_filter: Vec::new(),
                    result_sort: Vec::new(),
                    query_type_default: QueryType::Intersection,
                    query_rewriting: QueryRewriting::SearchOnly,
                }
            } else {
                let request_bytes = req.into_body().collect().await.unwrap().to_bytes();

                match request_bytes.is_empty() {
                    true => {
                        return HttpServerError::BadRequest("no query specified".to_string())
                            .into();
                    }
                    false => {
                        let search_request: SearchRequestObject =
                            match serde_json::from_slice::<SearchRequestObject>(&request_bytes) {
                                Ok(document_object) => document_object,
                                Err(e) => {
                                    return HttpServerError::BadRequest(e.to_string()).into();
                                }
                            };
                        search_request
                    }
                }
            };

            let search_result_local = query_index_api_get(&index_arc_clone, search_request).await;

            let search_result_json = serde_json::to_vec(&search_result_local).unwrap();
            Ok(Response::new(BoxBody::new(Full::new(
                search_result_json.into(),
            ))))
        }

        ("api", "v1", "index", "", _, _, &Method::POST) => {
            let Some(apikey) = apikey_header else {
                return HttpServerError::Unauthorized.into();
            };
            let Some(apikey_hash) = get_apikey_hash(apikey, &apikey_list).await else {
                return HttpServerError::Unauthorized.into();
            };

            let request_bytes = req.into_body().collect().await.unwrap().to_bytes();

            let create_index_request_object =
                match serde_json::from_slice::<CreateIndexRequest>(&request_bytes) {
                    Ok(create_index_request_object) => create_index_request_object,
                    Err(e) => {
                        return HttpServerError::BadRequest(e.to_string()).into();
                    }
                };

            let mut apikey_list_mut = apikey_list.write().await;
            let Some(apikey_object) = apikey_list_mut.get_mut(&apikey_hash) else {
                return HttpServerError::Unauthorized.into();
            };
            let index_id = create_index_api(
                &index_path,
                create_index_request_object.index_name,
                create_index_request_object.schema,
                create_index_request_object.similarity,
                create_index_request_object.tokenizer,
                create_index_request_object.stemmer,
                create_index_request_object.stop_words,
                create_index_request_object.frequent_words,
                create_index_request_object.ngram_indexing,
                create_index_request_object.synonyms,
                create_index_request_object.force_shard_number,
                apikey_object,
                create_index_request_object.spelling_correction,
            )
            .await;
            drop(apikey_list_mut);
            Ok(Response::new(BoxBody::new(Full::new(
                index_id.to_string().into(),
            ))))
        }

        ("api", "v1", "index", _, "", "", &Method::DELETE) => {
            let Some(apikey) = apikey_header else {
                return HttpServerError::Unauthorized.into();
            };
            let Some(apikey_hash) = get_apikey_hash(apikey, &apikey_list).await else {
                return HttpServerError::Unauthorized.into();
            };

            let Ok(index_id) = parts[3].parse() else {
                return HttpServerError::IndexNotFound.into();
            };

            let mut apikey_list_mut = apikey_list.write().await;
            if let Some(apikey_object) = apikey_list_mut.get_mut(&apikey_hash) {
                let Ok(_) = delete_index_api(index_id, &mut apikey_object.index_list).await else {
                    return HttpServerError::IndexNotFound.into();
                };

                let index_count = apikey_object.index_list.len();
                drop(apikey_list_mut);

                Ok(Response::new(BoxBody::new(Full::new(
                    index_count.to_string().into(),
                ))))
            } else {
                HttpServerError::Unauthorized.into()
            }
        }

        ("api", "v1", "index", _, "", "", &Method::PATCH) => {
            let Some(apikey) = apikey_header else {
                return HttpServerError::Unauthorized.into();
            };
            let Some(apikey_hash) = get_apikey_hash(apikey, &apikey_list).await else {
                return HttpServerError::Unauthorized.into();
            };

            let Ok(index_id) = parts[3].parse() else {
                return HttpServerError::IndexNotFound.into();
            };

            let apikey_list_ref = apikey_list.read().await;
            let Some(apikey_object) = apikey_list_ref.get(&apikey_hash) else {
                return HttpServerError::Unauthorized.into();
            };
            let Some(index_arc) = apikey_object.index_list.get(&index_id) else {
                return HttpServerError::IndexNotFound.into();
            };

            let index_arc_clone = index_arc.clone();
            drop(apikey_list_ref);
            let result = commit_index_api(&index_arc_clone).await;

            Ok(Response::new(BoxBody::new(Full::new(
                result.unwrap().to_string().into(),
            ))))
        }

        ("api", "v1", "index", _, "", "", &Method::PUT) => {
            let Some(apikey) = apikey_header else {
                return HttpServerError::Unauthorized.into();
            };
            let Some(apikey_hash) = get_apikey_hash(apikey, &apikey_list).await else {
                return HttpServerError::Unauthorized.into();
            };

            let Ok(index_id) = parts[3].parse() else {
                return HttpServerError::IndexNotFound.into();
            };

            let apikey_list_ref = apikey_list.read().await;
            let Some(apikey_object) = apikey_list_ref.get(&apikey_hash) else {
                return HttpServerError::Unauthorized.into();
            };
            let Some(index_arc) = apikey_object.index_list.get(&index_id) else {
                return HttpServerError::IndexNotFound.into();
            };

            let index_arc_clone = index_arc.clone();
            drop(apikey_list_ref);
            let result = close_index_api(&index_arc_clone).await;

            Ok(Response::new(BoxBody::new(Full::new(
                result.unwrap().to_string().into(),
            ))))
        }

        ("api", "v1", "apikey", "", "", "", &Method::GET) => {
            let Some(apikey) = apikey_header else {
                return HttpServerError::Unauthorized.into();
            };
            let Some(apikey_hash) = get_apikey_hash(apikey, &apikey_list).await else {
                return HttpServerError::Unauthorized.into();
            };

            let apikey_list_ref = apikey_list.read().await;
            let Some(apikey_object) = apikey_list_ref.get(&apikey_hash) else {
                return HttpServerError::Unauthorized.into();
            };
            let status_object = get_apikey_indices_info_api(&apikey_object.index_list).await;
            drop(apikey_list_ref);
            let status_object_json = serde_json::to_vec(&status_object).unwrap();
            Ok(Response::new(BoxBody::new(Full::new(
                status_object_json.into(),
            ))))
        }

        ("api", "v1", "index", _, "", _, &Method::GET) => {
            let Some(apikey) = apikey_header else {
                return HttpServerError::Unauthorized.into();
            };
            let Some(apikey_hash) = get_apikey_hash(apikey, &apikey_list).await else {
                return HttpServerError::Unauthorized.into();
            };

            let Ok(index_id) = parts[3].parse() else {
                return HttpServerError::BadRequest("index_id invalid or missing".to_string())
                    .into();
            };

            let apikey_list_ref = apikey_list.read().await;
            let Some(apikey_object) = apikey_list_ref.get(&apikey_hash) else {
                return HttpServerError::Unauthorized.into();
            };
            let status_object = get_index_info_api(index_id, &apikey_object.index_list).await;

            drop(apikey_list_ref);

            match status_object {
                Ok(status_object) => {
                    let status_object_json = serde_json::to_vec(&status_object).unwrap();
                    Ok(Response::new(BoxBody::new(Full::new(
                        status_object_json.into(),
                    ))))
                }
                Err(_e) => HttpServerError::IndexNotFound.into(),
            }
        }

        ("api", "v1", "index", _, "file", _, &Method::POST) => {
            let Some(apikey) = apikey_header else {
                return HttpServerError::Unauthorized.into();
            };
            let Some(apikey_hash) = get_apikey_hash(apikey, &apikey_list).await else {
                return HttpServerError::Unauthorized.into();
            };

            let Ok(index_id) = parts[3].parse() else {
                return HttpServerError::BadRequest("index_id invalid or missing".to_string())
                    .into();
            };

            let file_date = req
                .headers()
                .get("date")
                .and_then(|file_date| file_date.to_str().ok())
                .and_then(|date_str| date_str.parse::<i64>().ok())
                .unwrap_or_else(|| Utc::now().timestamp());

            let file_path = req
                .headers()
                .get("file")
                .and_then(|file_path| file_path.to_str().ok())
                .unwrap_or("")
                .to_string();
            let file_path = Path::new(&file_path);

            let request_bytes = req.into_body().collect().await.unwrap().to_bytes();

            let apikey_list_ref = apikey_list.read().await;

            let Some(apikey_object) = apikey_list_ref.get(&apikey_hash) else {
                return HttpServerError::Unauthorized.into();
            };
            let Some(index_arc) = apikey_object.index_list.get(&index_id) else {
                return HttpServerError::IndexNotFound.into();
            };
            let index_arc_clone = index_arc.clone();
            drop(apikey_list_ref);

            let status_object =
                index_file_api(&index_arc_clone, file_path, file_date, &request_bytes).await;
            let status_object_json = serde_json::to_vec(&status_object).unwrap();
            Ok(Response::new(BoxBody::new(Full::new(
                status_object_json.into(),
            ))))
        }

        ("api", "v1", "index", _, "synonyms", _, &Method::POST) => {
            let Some(apikey) = apikey_header else {
                return HttpServerError::Unauthorized.into();
            };
            let Some(apikey_hash) = get_apikey_hash(apikey, &apikey_list).await else {
                return HttpServerError::Unauthorized.into();
            };

            let apikey_list_ref = apikey_list.read().await;
            let Some(apikey_object) = apikey_list_ref.get(&apikey_hash) else {
                return HttpServerError::Unauthorized.into();
            };
            let Ok(index_id) = parts[3].parse::<u64>() else {
                return HttpServerError::BadRequest("index_id invalid or missing".to_string())
                    .into();
            };
            let Some(index_arc) = apikey_object.index_list.get(&index_id) else {
                return HttpServerError::IndexNotFound.into();
            };
            let index_arc_clone = index_arc.clone();
            drop(apikey_list_ref);

            let request_bytes = req.into_body().collect().await.unwrap().to_bytes();
            let synonyms = match serde_json::from_slice::<Vec<Synonym>>(&request_bytes) {
                Ok(create_index_request_object) => create_index_request_object,
                Err(e) => {
                    return HttpServerError::BadRequest(e.to_string()).into();
                }
            };

            if let Ok(result) = add_synonyms_api(&index_arc_clone, synonyms).await {
                let status_object_json = serde_json::to_vec(&result).unwrap();
                Ok(Response::new(BoxBody::new(Full::new(
                    status_object_json.into(),
                ))))
            } else {
                HttpServerError::SynonymsNotFound.into()
            }
        }

        ("api", "v1", "index", _, "synonyms", _, &Method::PUT) => {
            let Some(apikey) = apikey_header else {
                return HttpServerError::Unauthorized.into();
            };
            let Some(apikey_hash) = get_apikey_hash(apikey, &apikey_list).await else {
                return HttpServerError::Unauthorized.into();
            };

            let apikey_list_ref = apikey_list.read().await;
            let Some(apikey_object) = apikey_list_ref.get(&apikey_hash) else {
                return HttpServerError::Unauthorized.into();
            };
            let Ok(index_id) = parts[3].parse::<u64>() else {
                return HttpServerError::BadRequest("index_id invalid or missing".to_string())
                    .into();
            };
            let Some(index_arc) = apikey_object.index_list.get(&index_id) else {
                return HttpServerError::IndexNotFound.into();
            };
            let index_arc_clone = index_arc.clone();
            drop(apikey_list_ref);

            let request_bytes = req.into_body().collect().await.unwrap().to_bytes();
            let synonyms = match serde_json::from_slice::<Vec<Synonym>>(&request_bytes) {
                Ok(create_index_request_object) => create_index_request_object,
                Err(e) => {
                    return HttpServerError::BadRequest(e.to_string()).into();
                }
            };

            if let Ok(result) = set_synonyms_api(&index_arc_clone, synonyms).await {
                let status_object_json = serde_json::to_vec(&result).unwrap();
                Ok(Response::new(BoxBody::new(Full::new(
                    status_object_json.into(),
                ))))
            } else {
                HttpServerError::SynonymsNotFound.into()
            }
        }

        ("api", "v1", "index", _, "synonyms", _, &Method::GET) => {
            let Some(apikey) = apikey_header else {
                return HttpServerError::Unauthorized.into();
            };
            let Some(apikey_hash) = get_apikey_hash(apikey, &apikey_list).await else {
                return HttpServerError::Unauthorized.into();
            };

            let apikey_list_ref = apikey_list.read().await;
            let Some(apikey_object) = apikey_list_ref.get(&apikey_hash) else {
                return HttpServerError::Unauthorized.into();
            };
            let Ok(index_id) = parts[3].parse::<u64>() else {
                return HttpServerError::BadRequest("index_id invalid or missing".to_string())
                    .into();
            };
            let Some(index_arc) = apikey_object.index_list.get(&index_id) else {
                return HttpServerError::IndexNotFound.into();
            };
            let index_arc_clone = index_arc.clone();
            drop(apikey_list_ref);
            let result = get_synonyms_api(&index_arc_clone).await;
            let status_object_json = serde_json::to_vec(&result).unwrap();
            Ok(Response::new(BoxBody::new(Full::new(
                status_object_json.into(),
            ))))
        }

        ("api", "v1", "index", _, "doc", _, &Method::POST) => {
            let Some(apikey) = apikey_header else {
                return HttpServerError::Unauthorized.into();
            };
            let Some(apikey_hash) = get_apikey_hash(apikey, &apikey_list).await else {
                return HttpServerError::Unauthorized.into();
            };
            let Ok(index_id) = parts[3].parse() else {
                return HttpServerError::BadRequest("index_id invalid or missing".to_string())
                    .into();
            };

            let request_bytes = req.into_body().collect().await.unwrap().to_bytes();
            let request_string = from_utf8(&request_bytes).unwrap();

            let apikey_list_ref = apikey_list.read().await;
            let Some(apikey_object) = apikey_list_ref.get(&apikey_hash) else {
                return HttpServerError::Unauthorized.into();
            };
            let Some(index_arc) = apikey_object.index_list.get(&index_id) else {
                return HttpServerError::IndexNotFound.into();
            };
            let index_arc_clone = index_arc.clone();
            drop(apikey_list_ref);

            let status_object = if !request_string.trim().starts_with('[') {
                let document_object = match serde_json::from_str(request_string) {
                    Ok(document_object) => document_object,
                    Err(e) => {
                        return HttpServerError::BadRequest(e.to_string()).into();
                    }
                };

                index_document_api(&index_arc_clone, document_object).await
            } else {
                let document_object_vec = match serde_json::from_str(request_string) {
                    Ok(document_object_vec) => document_object_vec,
                    Err(e) => {
                        return HttpServerError::BadRequest(e.to_string()).into();
                    }
                };

                index_documents_api(&index_arc_clone, document_object_vec).await
            };
            let status_object_json = serde_json::to_vec(&status_object).unwrap();
            Ok(Response::new(BoxBody::new(Full::new(
                status_object_json.into(),
            ))))
        }

        ("api", "v1", "index", _, "doc", _, &Method::PATCH) => {
            let Some(apikey) = apikey_header else {
                return HttpServerError::Unauthorized.into();
            };
            let Some(apikey_hash) = get_apikey_hash(apikey, &apikey_list).await else {
                return HttpServerError::Unauthorized.into();
            };

            let Ok(index_id) = parts[3].parse() else {
                return HttpServerError::BadRequest("index_id invalid or missing".to_string())
                    .into();
            };
            let request_bytes = req.into_body().collect().await.unwrap().to_bytes();
            let request_string = from_utf8(&request_bytes).unwrap();
            let apikey_list_ref = apikey_list.read().await;
            let Some(apikey_object) = apikey_list_ref.get(&apikey_hash) else {
                return HttpServerError::Unauthorized.into();
            };
            let Some(index_arc) = apikey_object.index_list.get(&index_id) else {
                return HttpServerError::IndexNotFound.into();
            };
            let index_arc_clone = index_arc.clone();
            drop(apikey_list_ref);

            let is_doc_vector = if let Some(pos) = request_string.find('[') {
                request_string[pos + 1..].find('[').is_some()
            } else {
                return HttpServerError::BadRequest(String::new()).into();
            };

            let status_object = if !is_doc_vector {
                let document_object: (u64, Document) = match serde_json::from_str(request_string) {
                    Ok(id_document_object) => id_document_object,
                    Err(e) => {
                        return HttpServerError::BadRequest(e.to_string()).into();
                    }
                };

                update_document_api(&index_arc_clone, document_object).await
            } else {
                let id_document_object_vec: Vec<(u64, Document)> =
                    match serde_json::from_str(request_string) {
                        Ok(document_object_vec) => document_object_vec,
                        Err(e) => {
                            return HttpServerError::BadRequest(e.to_string()).into();
                        }
                    };

                update_documents_api(&index_arc_clone, id_document_object_vec).await
            };

            let status_object_json = serde_json::to_vec(&status_object).unwrap();
            Ok(Response::new(BoxBody::new(Full::new(
                status_object_json.into(),
            ))))
        }

        ("api", "v1", "index", _, "file", _, &Method::GET) => {
            let Some(apikey) = apikey_header else {
                return HttpServerError::Unauthorized.into();
            };
            let Some(apikey_hash) = get_apikey_hash(apikey, &apikey_list).await else {
                return HttpServerError::Unauthorized.into();
            };
            let Ok(index_id) = parts[3].parse() else {
                return HttpServerError::BadRequest("index_id invalid or missing".to_string())
                    .into();
            };
            let Ok(doc_id) = parts[5].parse() else {
                return HttpServerError::BadRequest("doc_id invalid or missing".to_string()).into();
            };
            let apikey_list_ref = apikey_list.read().await;
            let Some(apikey_object) = apikey_list_ref.get(&apikey_hash) else {
                return HttpServerError::Unauthorized.into();
            };
            let Some(index_arc) = apikey_object.index_list.get(&index_id) else {
                return HttpServerError::IndexNotFound.into();
            };
            let file = get_file_api(index_arc, doc_id).await;
            drop(apikey_list_ref);

            let Some(file) = file else {
                return HttpServerError::FileNotFound.into();
            };
            let response = Response::builder()
                .header("Content-Type", "application/pdf")
                .header("content-length", file.len())
                .header("Content-Disposition", "attachment;filename=file.pdf")
                .body(BoxBody::new(Full::new(file.into())))
                .unwrap();
            Ok(response)
        }

        ("api", "v1", "index", _, "doc", _, &Method::GET) => {
            let Some(apikey) = apikey_header else {
                return HttpServerError::Unauthorized.into();
            };
            let Some(apikey_hash) = get_apikey_hash(apikey, &apikey_list).await else {
                return HttpServerError::Unauthorized.into();
            };
            let Ok(index_id) = parts[3].parse() else {
                return HttpServerError::BadRequest("index_id invalid or missing".to_string())
                    .into();
            };
            let Ok(doc_id) = parts[5].parse() else {
                return HttpServerError::BadRequest("doc_id invalid or missing".to_string()).into();
            };

            let request_bytes = req.into_body().collect().await.unwrap().to_bytes();

            let get_document_request = if !request_bytes.is_empty() {
                let get_document_request: GetDocumentRequest =
                    match serde_json::from_slice(&request_bytes) {
                        Ok(document_object) => document_object,
                        Err(e) => {
                            return HttpServerError::BadRequest(e.to_string()).into();
                        }
                    };
                get_document_request
            } else {
                GetDocumentRequest {
                    query_terms: Vec::new(),
                    highlights: Vec::new(),
                    fields: Vec::new(),
                    distance_fields: Vec::new(),
                }
            };

            let apikey_list_ref = apikey_list.read().await;
            let Some(apikey_object) = apikey_list_ref.get(&apikey_hash) else {
                return HttpServerError::Unauthorized.into();
            };
            let Some(index_arc) = apikey_object.index_list.get(&index_id) else {
                return HttpServerError::IndexNotFound.into();
            };
            let status_object = get_document_api(index_arc, doc_id, get_document_request).await;
            drop(apikey_list_ref);

            if let Some(status_object) = status_object {
                let status_object_json = serde_json::to_vec(&status_object).unwrap();
                Ok(Response::new(BoxBody::new(Full::new(
                    status_object_json.into(),
                ))))
            } else {
                HttpServerError::DocumentNotFound.into()
            }
        }

        ("api", "v1", "index", _, "doc", _, &Method::DELETE) => {
            let Some(apikey) = apikey_header else {
                return HttpServerError::Unauthorized.into();
            };
            let Some(apikey_hash) = get_apikey_hash(apikey, &apikey_list).await else {
                return HttpServerError::Unauthorized.into();
            };
            let apikey_list_ref = apikey_list.read().await;
            let Some(apikey_object) = apikey_list_ref.get(&apikey_hash) else {
                return HttpServerError::Unauthorized.into();
            };
            let Ok(index_id) = parts[3].parse() else {
                return HttpServerError::BadRequest("index_id invalid or missing".to_string())
                    .into();
            };
            let Some(index_arc) = apikey_object.index_list.get(&index_id) else {
                return HttpServerError::IndexNotFound.into();
            };
            let index_arc_clone = index_arc.clone();
            drop(apikey_list_ref);

            match parts[5].parse() {
                Ok(document_id) => {
                    let status_object =
                        delete_document_by_parameter_api(&index_arc_clone, document_id).await;
                    let status_object_json = serde_json::to_vec(&status_object).unwrap();
                    Ok(Response::new(BoxBody::new(Full::new(
                        status_object_json.into(),
                    ))))
                }

                Err(_) => {
                    let request_bytes = req.into_body().collect().await.unwrap().to_bytes();

                    if *request_bytes == *b"clear" {
                        let status_object = clear_index_api(&index_arc_clone).await;
                        let status_object_json = serde_json::to_vec(&status_object).unwrap();
                        Ok(Response::new(BoxBody::new(Full::new(
                            status_object_json.into(),
                        ))))
                    } else {
                        match serde_json::from_slice::<SearchRequestObject>(&request_bytes) {
                            Ok(search_request) => {
                                let status_object =
                                    delete_documents_by_query_api(&index_arc_clone, search_request)
                                        .await;
                                let status_object_json =
                                    serde_json::to_vec(&status_object).unwrap();
                                Ok(Response::new(BoxBody::new(Full::new(
                                    status_object_json.into(),
                                ))))
                            }
                            Err(_) => {
                                let request_string = from_utf8(&request_bytes).unwrap();
                                let is_doc_vector = request_string.trim().starts_with('[');
                                let status_object = if !is_doc_vector {
                                    let document_id = match serde_json::from_str(request_string) {
                                        Ok(document_id) => document_id,
                                        Err(e) => {
                                            return HttpServerError::BadRequest(e.to_string())
                                                .into();
                                        }
                                    };
                                    delete_document_by_object_api(&index_arc_clone, document_id)
                                        .await
                                } else {
                                    let document_id_vec = match serde_json::from_str(request_string)
                                    {
                                        Ok(document_id_vec) => document_id_vec,
                                        Err(e) => {
                                            return HttpServerError::BadRequest(e.to_string())
                                                .into();
                                        }
                                    };
                                    delete_documents_by_object_api(
                                        &index_arc_clone,
                                        document_id_vec,
                                    )
                                    .await
                                };
                                let status_object_json =
                                    serde_json::to_vec(&status_object).unwrap();
                                Ok(Response::new(BoxBody::new(Full::new(
                                    status_object_json.into(),
                                ))))
                            }
                        }
                    }
                }
            }
        }

        ("api", "v1", "apikey", "", "", "", &Method::POST) => {
            let Some(apikey_header) = apikey_header else {
                return HttpServerError::Unauthorized.into();
            };
            let mut hasher = Sha256::new();
            hasher.update(MASTER_KEY_SECRET.to_string());
            let master_apikey = hasher.finalize();
            let master_apikey_base64 = general_purpose::STANDARD.encode(master_apikey);

            if apikey_header != master_apikey_base64 {
                return HttpServerError::Unauthorized.into();
            };
            let request_bytes = req.into_body().collect().await.unwrap().to_bytes();
            let apikey_quota_object = match serde_json::from_slice(&request_bytes) {
                Ok(apikey_quota_object) => apikey_quota_object,
                Err(e) => {
                    return Ok(status(StatusCode::BAD_REQUEST, e.to_string()));
                }
            };

            let mut apikey = [0u8; 32];
            OsRng.try_fill_bytes(&mut apikey).unwrap();
            let api_key_base64 = general_purpose::STANDARD.encode(apikey);
            let mut apikey_list_mut = apikey_list.write().await;
            create_apikey_api(
                &index_path,
                apikey_quota_object,
                &apikey,
                &mut apikey_list_mut,
            );
            drop(apikey_list_mut);

            Ok(Response::new(BoxBody::new(Full::new(
                api_key_base64.into(),
            ))))
        }

        ("api", "v1", "apikey", "", "", "", &Method::DELETE) => {
            let Some(apikey_header) = apikey_header else {
                return HttpServerError::Unauthorized.into();
            };
            let mut hasher = Sha256::new();
            hasher.update(MASTER_KEY_SECRET.to_string());
            let master_apikey = hasher.finalize();
            let master_apikey_base64 = general_purpose::STANDARD.encode(master_apikey);

            if apikey_header != master_apikey_base64 {
                return HttpServerError::Unauthorized.into();
            };
            let request_bytes = req.into_body().collect().await.unwrap().to_bytes();

            let request_object: DeleteApikeyRequest = match serde_json::from_slice(&request_bytes) {
                Ok(request_object) => request_object,
                Err(e) => {
                    return HttpServerError::BadRequest(e.to_string()).into();
                }
            };

            let Ok(apikey) = general_purpose::STANDARD.decode(&request_object.apikey_base64) else {
                return HttpServerError::Unauthorized.into();
            };
            let apikey_hash = calculate_hash(&apikey) as u128;

            let mut apikey_list_mut = apikey_list.write().await;
            let result = delete_apikey_api(&index_path, &mut apikey_list_mut, apikey_hash);
            drop(apikey_list_mut);

            match result {
                Ok(count) => Ok(Response::new(BoxBody::new(Full::new(
                    count.to_string().into(),
                )))),
                Err(_) => HttpServerError::ApiKeyNotFound.into(),
            }
        }

        ("api", "v1", "status", "", "", "", &Method::GET) => HttpServerError::NotImplemented.into(),

        (_, _, _, _, _, _, &Method::GET) => match path {
            "/" => Ok(Response::new(BoxBody::new(INDEX_HTML.to_string()))),
            "/css/flexboxgrid.min.css" => Ok(Response::new(BoxBody::new(FLEXBOX_CSS.to_string()))),
            "/css/master.css" => Ok(Response::new(BoxBody::new(MASTER_CSS.to_string()))),
            "/js/master.js" => Ok(Response::new(BoxBody::new(MASTER_JS.to_string()))),
            "/js/jquery-3.7.1.min.js" => Ok(Response::new(BoxBody::new(JQUERY_JS.to_string()))),

            "/css/bootstrap.histogram.slider.css" => {
                Ok(Response::new(BoxBody::new(HISTOGRAM_CSS.to_string())))
            }
            "/css/histogram.slider.css" => Ok(Response::new(BoxBody::new(SLIDER_CSS.to_string()))),
            "/js/bootstrap.histogram.slider.js" => {
                Ok(Response::new(BoxBody::new(HISTOGRAM_JS.to_string())))
            }
            "/js/bootstrap-slider.js" => Ok(Response::new(BoxBody::new(SLIDER_JS.to_string()))),

            "/svg/logo.svg" => {
                let response = Response::builder()
                    .header("Content-Type", "image/svg+xml")
                    .header("content-length", LOGO_SVG.len())
                    .body(BoxBody::new(Full::new(LOGO_SVG.into())))
                    .unwrap();
                Ok(response)
            }

            "/favicon-16x16.png" => Ok(Response::new(BoxBody::new(Full::new(FAVICON_16.into())))),
            "/favicon-32x32.png" => Ok(Response::new(BoxBody::new(Full::new(FAVICON_32.into())))),
            "/version" => Ok(Response::new(BoxBody::new(Full::new(VERSION.into())))),
            _ => HttpServerError::NotImplemented.into(),
        },
        _ => HttpServerError::NotImplemented.into(),
    }
}

pub(crate) async fn http_server(
    index_path: &Path,
    apikey_list: Arc<tokio::sync::RwLock<HashMap<u128, ApikeyObject>>>,
    local_ip: &String,
    local_port: &u16,
) {
    let local_address: SocketAddr = format!("{}:{}", local_ip, local_port)
        .parse()
        .expect("Unable to parse socket address");

    match TcpListener::bind(local_address).await {
        Ok(listener) => {
            let mut hasher = Sha256::new();
            hasher.update(MASTER_KEY_SECRET.to_string());
            let master_apikey = hasher.finalize();
            let master_apikey_base64 = general_purpose::STANDARD.encode(master_apikey);

            println!(
                "Listening on: {} index dir {} master key {}\n\n",
                local_address,
                index_path.display(),
                master_apikey_base64
            );

            io::stdout().flush().unwrap();

            let index_path = index_path.to_path_buf();

            loop {
                let (tcp, remote_address) = listener.accept().await.unwrap();
                let io = TokioIo::new(tcp);

                let index_path = index_path.clone();
                let apikey_list = apikey_list.clone();

                tokio::task::spawn(async move {
                    if let Err(err) = Builder::new(TokioExecutor::new())
                        .serve_connection(
                            io,
                            service_fn(move |request: Request<Incoming>| {
                                let index_path = index_path.clone();
                                let apikey_list = apikey_list.clone();
                                async move {
                                    let t: Result<_, Infallible> = http_request_handler(
                                        index_path,
                                        apikey_list,
                                        request,
                                        remote_address,
                                    )
                                    .await;

                                    t
                                }
                            }),
                        )
                        .await
                    {
                        eprintln!("error serving connection: {:?}", err);
                    }
                });
            }
        }

        Err(_e) => {
            println!(
                "Starting the server at {:?} failed. \
            Check if there is another SeekStorm server instance running on the same port. \
            Try changing the port.",
                local_address
            );
            process::exit(1)
        }
    }
}
