use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::io;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process;
use std::str;
use std::sync::Arc;

use rand::rngs::OsRng;
use rand::RngCore;

use hyper::body;
use hyper::server::conn::AddrStream;
use hyper::service::{make_service_fn, service_fn};
use hyper::Method;
use hyper::StatusCode;
use hyper::{Body, Request, Response, Server};
use seekstorm::index::Document;
use seekstorm::search::{QueryType, ResultType};
use sha2::Digest;
use sha2::Sha256;
use std::{convert::Infallible, net::SocketAddr};

use base64::{engine::general_purpose, Engine as _};

use crate::api_endpoints::delete_index_api;
use crate::api_endpoints::get_all_index_stats_api;
use crate::api_endpoints::get_document_api;
use crate::api_endpoints::get_index_stats_api;
use crate::api_endpoints::index_document_api;
use crate::api_endpoints::index_documents_api;
use crate::api_endpoints::query_index_api;
use crate::api_endpoints::update_document_api;
use crate::api_endpoints::update_documents_api;
use crate::api_endpoints::CreateIndexRequest;
use crate::api_endpoints::DeleteApikeyRequest;
use crate::api_endpoints::{close_index_api, delete_document_api};
use crate::api_endpoints::{commit_index_api, create_apikey_api};
use crate::api_endpoints::{create_index_api, SearchRequestObject};
use crate::api_endpoints::{delete_apikey_api, GetDocumentRequest};
use crate::api_endpoints::{delete_documents_api, delete_documents_by_query_api};
use crate::multi_tenancy::get_apikey_hash;
use crate::multi_tenancy::ApikeyObject;
use crate::{MASTER_KEY_SECRET, VERSION};

const INDEX_HTML: &str = include_str!("web/index.html");
const FLEXBOX_CSS: &str = include_str!("web/css/flexboxgrid.min.css");
const MASTER_CSS: &str = include_str!("web/css/master.css");
const MASTER_JS: &str = include_str!("web/js/master.js");
const JQUERY_JS: &str = include_str!("web/js/jquery-3.7.1.min.js");
const LOGO_SVG: &[u8] = include_bytes!("web/svg/logo.svg");
const FAVICON_16: &[u8] = include_bytes!("web/favicon-16x16.png");
const FAVICON_32: &[u8] = include_bytes!("web/favicon-32x32.png");

pub(crate) fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}

pub(crate) fn status(status: StatusCode, error_message: String) -> Response<Body> {
    Response::builder()
        .status(status)
        .body(error_message.into())
        .unwrap()
}

pub(crate) async fn http_request_handler(
    index_path: PathBuf,
    apikey_list: Arc<tokio::sync::RwLock<HashMap<u128, ApikeyObject>>>,
    req: Request<Body>,
    _remote_addr: SocketAddr,
) -> Result<Response<Body>, Infallible> {
    let headers = req.headers();

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
        ("api", "v1", "index", _, "query", _, &Method::POST) => {
            if let Some(apikey) = headers.get("apikey") {
                if let Some(apikey_hash) =
                    get_apikey_hash(apikey.to_str().unwrap().to_string(), &apikey_list).await
                {
                    let index_id: u64 = parts[3].parse().unwrap();

                    let apikey_list_ref = apikey_list.read().await;
                    if let Some(apikey_object) = apikey_list_ref.get(&apikey_hash) {
                        if let Some(index_arc) = apikey_object.index_list.get(&index_id) {
                            let index_arc_clone = index_arc.clone();
                            drop(apikey_list_ref);

                            let request_bytes = body::to_bytes(req.into_body()).await.unwrap();

                            let search_request =
                                match serde_json::from_slice::<SearchRequestObject>(&request_bytes)
                                {
                                    Ok(search_request) => search_request,
                                    Err(e) => {
                                        return Ok(status(StatusCode::BAD_REQUEST, e.to_string()));
                                    }
                                };

                            let search_result_local =
                                query_index_api(&index_arc_clone, search_request).await;

                            let search_result_json =
                                serde_json::to_string(&search_result_local).unwrap();
                            Ok(Response::new(search_result_json.into()))
                        } else {
                            Ok(status(
                                StatusCode::NOT_FOUND,
                                "index does not exists".to_string(),
                            ))
                        }
                    } else {
                        Ok(status(
                            StatusCode::NOT_FOUND,
                            "api_key does not exists".to_string(),
                        ))
                    }
                } else {
                    Ok(status(StatusCode::UNAUTHORIZED, String::new()))
                }
            } else {
                Ok(status(StatusCode::UNAUTHORIZED, String::new()))
            }
        }

        ("api", "v1", "index", _, "query", _, &Method::GET) => {
            if let Some(apikey) = headers.get("apikey") {
                if let Some(apikey_hash) =
                    get_apikey_hash(apikey.to_str().unwrap().to_string(), &apikey_list).await
                {
                    let Ok(index_id) = parts[3].parse() else {
                        return Ok(status(
                            StatusCode::BAD_REQUEST,
                            "index_id invalid or missing".to_string(),
                        ));
                    };

                    let apikey_list_ref = apikey_list.read().await;
                    if let Some(apikey_object) = apikey_list_ref.get(&apikey_hash) {
                        if let Some(index_arc) = apikey_object.index_list.get(&index_id) {
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
                                        return Ok(status(
                                            StatusCode::BAD_REQUEST,
                                            "api_offset invalid or missing".to_string(),
                                        ));
                                    };
                                    api_offset
                                } else {
                                    0
                                };

                                let length = if let Some(value) = params.get("length") {
                                    let Ok(api_length) = value.parse::<usize>() else {
                                        return Ok(status(
                                            StatusCode::BAD_REQUEST,
                                            "api_length invalid or missing".to_string(),
                                        ));
                                    };
                                    api_length
                                } else {
                                    10
                                };

                                let realtime = if let Some(value) = params.get("realtime") {
                                    let Ok(realtime) = value.parse::<bool>() else {
                                        return Ok(status(
                                            StatusCode::BAD_REQUEST,
                                            "api_length invalid or missing".to_string(),
                                        ));
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
                                }
                            } else {
                                let request_bytes = body::to_bytes(req.into_body()).await.unwrap();

                                match request_bytes.is_empty() {
                                    true => {
                                        return Ok(status(
                                            StatusCode::BAD_REQUEST,
                                            "no query specified".to_string(),
                                        ));
                                    }
                                    false => {
                                        let search_request: SearchRequestObject =
                                            match serde_json::from_slice::<SearchRequestObject>(
                                                &request_bytes,
                                            ) {
                                                Ok(document_object) => document_object,
                                                Err(e) => {
                                                    return Ok(status(
                                                        StatusCode::BAD_REQUEST,
                                                        e.to_string(),
                                                    ));
                                                }
                                            };
                                        search_request
                                    }
                                }
                            };

                            let search_result_local =
                                query_index_api(&index_arc_clone, search_request).await;
                            let search_result_json =
                                serde_json::to_string(&search_result_local).unwrap();

                            Ok(Response::new(search_result_json.into()))
                        } else {
                            Ok(status(
                                StatusCode::NOT_FOUND,
                                "index does not exists".to_string(),
                            ))
                        }
                    } else {
                        Ok(status(
                            StatusCode::NOT_FOUND,
                            "api_key does not exists".to_string(),
                        ))
                    }
                } else {
                    Ok(status(
                        StatusCode::UNAUTHORIZED,
                        "api_key invalid or missing".to_string(),
                    ))
                }
            } else {
                Ok(status(
                    StatusCode::UNAUTHORIZED,
                    "api_key invalid or missing".to_string(),
                ))
            }
        }

        ("api", "v1", "index", "", _, _, &Method::POST) => {
            if let Some(apikey) = headers.get("apikey") {
                if let Some(apikey_hash) =
                    get_apikey_hash(apikey.to_str().unwrap().to_string(), &apikey_list).await
                {
                    let request_bytes = body::to_bytes(req.into_body()).await.unwrap();

                    let create_index_request_object =
                        match serde_json::from_slice::<CreateIndexRequest>(&request_bytes) {
                            Ok(create_index_request_object) => create_index_request_object,
                            Err(e) => {
                                return Ok(status(StatusCode::BAD_REQUEST, e.to_string()));
                            }
                        };

                    let mut apikey_list_mut = apikey_list.write().await;
                    if let Some(apikey_object) = apikey_list_mut.get_mut(&apikey_hash) {
                        let index_id = create_index_api(
                            &index_path,
                            create_index_request_object.index_name,
                            create_index_request_object.schema,
                            create_index_request_object.similarity,
                            create_index_request_object.tokenizer,
                            apikey_object,
                        );
                        drop(apikey_list_mut);
                        Ok(Response::new(index_id.to_string().into()))
                    } else {
                        Ok(status(
                            StatusCode::NOT_FOUND,
                            "api_key does not exists".to_string(),
                        ))
                    }
                } else {
                    Ok(status(StatusCode::UNAUTHORIZED, String::new()))
                }
            } else {
                Ok(status(StatusCode::UNAUTHORIZED, String::new()))
            }
        }

        ("api", "v1", "index", _, "", "", &Method::DELETE) => {
            if let Some(apikey) = headers.get("apikey") {
                if let Some(apikey_hash) =
                    get_apikey_hash(apikey.to_str().unwrap().to_string(), &apikey_list).await
                {
                    let Ok(index_id) = parts[3].parse() else {
                        return Ok(status(
                            StatusCode::BAD_REQUEST,
                            "index_id invalid or missing".to_string(),
                        ));
                    };

                    let mut apikey_list_mut = apikey_list.write().await;
                    if let Some(apikey_object) = apikey_list_mut.get_mut(&apikey_hash) {
                        let Ok(_) = delete_index_api(index_id, &mut apikey_object.index_list).await
                        else {
                            return Ok(status(
                                StatusCode::NOT_FOUND,
                                "index_id does not exists".to_string(),
                            ));
                        };

                        let index_count = apikey_object.index_list.len();
                        drop(apikey_list_mut);

                        Ok(Response::new(index_count.to_string().into()))
                    } else {
                        Ok(status(
                            StatusCode::NOT_FOUND,
                            "api_key does not exists".to_string(),
                        ))
                    }
                } else {
                    Ok(status(StatusCode::UNAUTHORIZED, String::new()))
                }
            } else {
                Ok(status(StatusCode::UNAUTHORIZED, String::new()))
            }
        }

        ("api", "v1", "index", _, "", "", &Method::PATCH) => {
            if let Some(apikey) = headers.get("apikey") {
                if let Some(apikey_hash) =
                    get_apikey_hash(apikey.to_str().unwrap().to_string(), &apikey_list).await
                {
                    let Ok(index_id) = parts[3].parse() else {
                        return Ok(status(
                            StatusCode::BAD_REQUEST,
                            "index_id invalid or missing".to_string(),
                        ));
                    };

                    let apikey_list_ref = apikey_list.read().await;
                    if let Some(apikey_object) = apikey_list_ref.get(&apikey_hash) {
                        if let Some(index_arc) = apikey_object.index_list.get(&index_id) {
                            let index_arc_clone = index_arc.clone();
                            drop(apikey_list_ref);
                            let result = commit_index_api(&index_arc_clone).await;

                            Ok(Response::new(result.unwrap().to_string().into()))
                        } else {
                            Ok(status(
                                StatusCode::NOT_FOUND,
                                "index does not exists".to_string(),
                            ))
                        }
                    } else {
                        Ok(status(
                            StatusCode::NOT_FOUND,
                            "api_key does not exists".to_string(),
                        ))
                    }
                } else {
                    Ok(status(StatusCode::UNAUTHORIZED, String::new()))
                }
            } else {
                Ok(status(StatusCode::UNAUTHORIZED, String::new()))
            }
        }

        ("api", "v1", "index", _, "", "", &Method::PUT) => {
            if let Some(apikey) = headers.get("apikey") {
                if let Some(apikey_hash) =
                    get_apikey_hash(apikey.to_str().unwrap().to_string(), &apikey_list).await
                {
                    let Ok(index_id) = parts[3].parse() else {
                        return Ok(status(
                            StatusCode::BAD_REQUEST,
                            "index_id invalid or missing".to_string(),
                        ));
                    };

                    let apikey_list_ref = apikey_list.read().await;
                    if let Some(apikey_object) = apikey_list_ref.get(&apikey_hash) {
                        if let Some(index_arc) = apikey_object.index_list.get(&index_id) {
                            let index_arc_clone = index_arc.clone();
                            drop(apikey_list_ref);
                            let result = close_index_api(&index_arc_clone).await;

                            Ok(Response::new(result.unwrap().to_string().into()))
                        } else {
                            Ok(status(
                                StatusCode::NOT_FOUND,
                                "index does not exists".to_string(),
                            ))
                        }
                    } else {
                        Ok(status(
                            StatusCode::NOT_FOUND,
                            "api_key does not exists".to_string(),
                        ))
                    }
                } else {
                    Ok(status(StatusCode::UNAUTHORIZED, String::new()))
                }
            } else {
                Ok(status(StatusCode::UNAUTHORIZED, String::new()))
            }
        }

        ("api", "v1", "index", "", "", "", &Method::GET) => {
            if let Some(apikey) = headers.get("apikey") {
                if let Some(apikey_hash) =
                    get_apikey_hash(apikey.to_str().unwrap().to_string(), &apikey_list).await
                {
                    let apikey_list_ref = apikey_list.read().await;
                    let apikey_object = apikey_list_ref.get(&apikey_hash).unwrap();
                    let status_object =
                        get_all_index_stats_api(&index_path, &apikey_object.index_list).await;
                    drop(apikey_list_ref);
                    let status_object_json = serde_json::to_string(&status_object).unwrap();

                    Ok(Response::new(status_object_json.into()))
                } else {
                    Ok(status(StatusCode::UNAUTHORIZED, String::new()))
                }
            } else {
                Ok(status(StatusCode::UNAUTHORIZED, String::new()))
            }
        }

        ("api", "v1", "index", _, "", _, &Method::GET) => {
            if let Some(apikey) = headers.get("apikey") {
                if let Some(apikey_hash) =
                    get_apikey_hash(apikey.to_str().unwrap().to_string(), &apikey_list).await
                {
                    let Ok(index_id) = parts[3].parse() else {
                        return Ok(status(
                            StatusCode::BAD_REQUEST,
                            "index_id invalid or missing".to_string(),
                        ));
                    };

                    let apikey_list_ref = apikey_list.read().await;
                    if let Some(apikey_object) = apikey_list_ref.get(&apikey_hash) {
                        let status_object =
                            get_index_stats_api(&index_path, index_id, &apikey_object.index_list)
                                .await;

                        drop(apikey_list_ref);

                        match status_object {
                            Ok(status_object) => {
                                let status_object_json =
                                    serde_json::to_string(&status_object).unwrap();
                                Ok(Response::new(status_object_json.into()))
                            }
                            Err(_e) => Ok(status(
                                StatusCode::NOT_FOUND,
                                "index does not exists".to_string(),
                            )),
                        }
                    } else {
                        Ok(status(
                            StatusCode::NOT_FOUND,
                            "api_key does not exists".to_string(),
                        ))
                    }
                } else {
                    Ok(status(StatusCode::UNAUTHORIZED, String::new()))
                }
            } else {
                Ok(status(StatusCode::UNAUTHORIZED, String::new()))
            }
        }

        ("api", "v1", "index", _, "doc", _, &Method::POST) => {
            if let Some(apikey) = headers.get("apikey") {
                if let Some(apikey_hash) =
                    get_apikey_hash(apikey.to_str().unwrap().to_string(), &apikey_list).await
                {
                    let Ok(index_id) = parts[3].parse() else {
                        return Ok(status(
                            StatusCode::BAD_REQUEST,
                            "index_id invalid or missing".to_string(),
                        ));
                    };

                    let request_bytes = body::to_bytes(req.into_body()).await.unwrap();
                    let request_string = str::from_utf8(&request_bytes).unwrap();

                    let apikey_list_ref = apikey_list.read().await;

                    if let Some(apikey_object) = apikey_list_ref.get(&apikey_hash) {
                        if let Some(index_arc) = apikey_object.index_list.get(&index_id) {
                            let index_arc_clone = index_arc.clone();
                            drop(apikey_list_ref);

                            let status_object = if !request_string.trim().starts_with('[') {
                                let document_object = match serde_json::from_str(request_string) {
                                    Ok(document_object) => document_object,
                                    Err(e) => {
                                        return Ok(status(StatusCode::BAD_REQUEST, e.to_string()));
                                    }
                                };

                                index_document_api(&index_arc_clone, document_object).await
                            } else {
                                let document_object_vec = match serde_json::from_str(request_string)
                                {
                                    Ok(document_object_vec) => document_object_vec,
                                    Err(e) => {
                                        return Ok(status(StatusCode::BAD_REQUEST, e.to_string()));
                                    }
                                };

                                index_documents_api(&index_arc_clone, document_object_vec).await
                            };
                            let status_object_json = serde_json::to_string(&status_object).unwrap();
                            Ok(Response::new(status_object_json.into()))
                        } else {
                            Ok(status(
                                StatusCode::NOT_FOUND,
                                "index does not exists".to_string(),
                            ))
                        }
                    } else {
                        Ok(status(
                            StatusCode::NOT_FOUND,
                            "api_key does not exists".to_string(),
                        ))
                    }
                } else {
                    Ok(status(StatusCode::UNAUTHORIZED, String::new()))
                }
            } else {
                Ok(status(StatusCode::UNAUTHORIZED, String::new()))
            }
        }

        ("api", "v1", "index", _, "doc", _, &Method::PATCH) => {
            if let Some(apikey) = headers.get("apikey") {
                if let Some(apikey_hash) =
                    get_apikey_hash(apikey.to_str().unwrap().to_string(), &apikey_list).await
                {
                    let index_id: u64 = parts[3].parse().unwrap();
                    let request_bytes = body::to_bytes(req.into_body()).await.unwrap();
                    let request_string = str::from_utf8(&request_bytes).unwrap();
                    let apikey_list_ref = apikey_list.read().await;
                    let apikey_object = apikey_list_ref.get(&apikey_hash).unwrap();
                    let index_arc = apikey_object.index_list.get(&index_id).unwrap();
                    let index_arc_clone = index_arc.clone();
                    drop(apikey_list_ref);

                    let is_doc_vector = if let Some(pos) = request_string.find('[') {
                        request_string[pos + 1..].find('[').is_some()
                    } else {
                        return Ok(status(StatusCode::BAD_REQUEST, String::new()));
                    };

                    let status_object = if !is_doc_vector {
                        let document_object: (u64, Document) =
                            match serde_json::from_str(request_string) {
                                Ok(id_document_object) => id_document_object,
                                Err(e) => {
                                    return Ok(status(StatusCode::BAD_REQUEST, e.to_string()));
                                }
                            };

                        update_document_api(&index_arc_clone, document_object).await
                    } else {
                        let id_document_object_vec: Vec<(u64, Document)> =
                            match serde_json::from_str(request_string) {
                                Ok(document_object_vec) => document_object_vec,
                                Err(e) => {
                                    return Ok(status(StatusCode::BAD_REQUEST, e.to_string()));
                                }
                            };

                        update_documents_api(&index_arc_clone, id_document_object_vec).await
                    };

                    let status_object_json = serde_json::to_string(&status_object).unwrap();
                    Ok(Response::new(status_object_json.into()))
                } else {
                    Ok(status(StatusCode::UNAUTHORIZED, String::new()))
                }
            } else {
                Ok(status(StatusCode::UNAUTHORIZED, String::new()))
            }
        }

        ("api", "v1", "index", _, "doc", _, &Method::GET) => {
            if let Some(apikey) = headers.get("apikey") {
                if let Some(apikey_hash) =
                    get_apikey_hash(apikey.to_str().unwrap().to_string(), &apikey_list).await
                {
                    let Ok(index_id) = parts[3].parse() else {
                        return Ok(status(
                            StatusCode::BAD_REQUEST,
                            "index_id invalid or missing".to_string(),
                        ));
                    };
                    let Ok(doc_id) = parts[5].parse() else {
                        return Ok(status(
                            StatusCode::BAD_REQUEST,
                            "doc_id invalid or missing".to_string(),
                        ));
                    };

                    let request_bytes = body::to_bytes(req.into_body()).await.unwrap();

                    let get_document_request = if !request_bytes.is_empty() {
                        let get_document_request: GetDocumentRequest =
                            match serde_json::from_slice(&request_bytes) {
                                Ok(document_object) => document_object,
                                Err(e) => {
                                    return Ok(status(StatusCode::BAD_REQUEST, e.to_string()));
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
                    if let Some(apikey_object) = apikey_list_ref.get(&apikey_hash) {
                        if let Some(index_arc) = apikey_object.index_list.get(&index_id) {
                            let status_object =
                                get_document_api(index_arc, doc_id, get_document_request).await;
                            drop(apikey_list_ref);

                            if let Some(status_object) = status_object {
                                let status_object_json =
                                    serde_json::to_string(&status_object).unwrap();
                                Ok(Response::new(status_object_json.into()))
                            } else {
                                Ok(status(
                                    StatusCode::NOT_FOUND,
                                    "doc_id does not exists".to_string(),
                                ))
                            }
                        } else {
                            Ok(status(
                                StatusCode::NOT_FOUND,
                                "index does not exists".to_string(),
                            ))
                        }
                    } else {
                        Ok(status(
                            StatusCode::NOT_FOUND,
                            "api_key does not exists".to_string(),
                        ))
                    }
                } else {
                    Ok(status(StatusCode::UNAUTHORIZED, String::new()))
                }
            } else {
                Ok(status(StatusCode::UNAUTHORIZED, String::new()))
            }
        }

        ("api", "v1", "index", _, "doc", _, &Method::DELETE) => {
            if let Some(apikey) = headers.get("apikey") {
                if let Some(apikey_hash) =
                    get_apikey_hash(apikey.to_str().unwrap().to_string(), &apikey_list).await
                {
                    let Ok(index_id) = parts[3].parse() else {
                        return Ok(status(
                            StatusCode::BAD_REQUEST,
                            "index_id invalid or missing".to_string(),
                        ));
                    };

                    let apikey_list_ref = apikey_list.read().await;
                    let apikey_object = apikey_list_ref.get(&apikey_hash).unwrap();
                    let index_arc = apikey_object.index_list.get(&index_id).unwrap();
                    let index_arc_clone = index_arc.clone();
                    drop(apikey_list_ref);

                    let Ok(document_id) = parts[5].parse() else {
                        let request_bytes = body::to_bytes(req.into_body()).await.unwrap();

                        match serde_json::from_slice::<SearchRequestObject>(&request_bytes) {
                            Ok(search_request) => {
                                let status_object =
                                    delete_documents_by_query_api(&index_arc_clone, search_request)
                                        .await;
                                let status_object_json =
                                    serde_json::to_string(&status_object).unwrap();
                                return Ok(Response::new(status_object_json.into()));
                            }
                            Err(_) => {
                                let request_string = str::from_utf8(&request_bytes).unwrap();
                                let is_doc_vector = request_string.trim().starts_with('[');
                                let status_object = if !is_doc_vector {
                                    let document_id = match serde_json::from_str(request_string) {
                                        Ok(document_id) => document_id,
                                        Err(e) => {
                                            return Ok(status(
                                                StatusCode::BAD_REQUEST,
                                                e.to_string(),
                                            ));
                                        }
                                    };

                                    delete_document_api(&index_arc_clone, document_id).await
                                } else {
                                    let document_id_vec = match serde_json::from_str(request_string)
                                    {
                                        Ok(document_id_vec) => document_id_vec,
                                        Err(e) => {
                                            return Ok(status(
                                                StatusCode::BAD_REQUEST,
                                                e.to_string(),
                                            ));
                                        }
                                    };

                                    delete_documents_api(&index_arc_clone, document_id_vec).await
                                };
                                let status_object_json =
                                    serde_json::to_string(&status_object).unwrap();
                                return Ok(Response::new(status_object_json.into()));
                            }
                        };
                    };

                    let status_object = delete_document_api(&index_arc_clone, document_id).await;
                    let status_object_json = serde_json::to_string(&status_object).unwrap();
                    Ok(Response::new(status_object_json.into()))
                } else {
                    Ok(status(StatusCode::UNAUTHORIZED, String::new()))
                }
            } else {
                Ok(status(StatusCode::UNAUTHORIZED, String::new()))
            }
        }

        ("api", "v1", "apikey", "", "", "", &Method::POST) => {
            if let Some(apikey_header) = headers.get("apikey") {
                let mut hasher = Sha256::new();
                hasher.update(MASTER_KEY_SECRET.to_string());
                let peer_master_apikey = hasher.finalize();
                let peer_master_apikey_base64 =
                    general_purpose::STANDARD.encode(peer_master_apikey);

                if apikey_header.to_str().unwrap_or("") == peer_master_apikey_base64 {
                    let request_bytes = body::to_bytes(req.into_body()).await.unwrap();
                    let apikey_quota_object = match serde_json::from_slice(&request_bytes) {
                        Ok(apikey_quota_object) => apikey_quota_object,
                        Err(e) => {
                            return Ok(status(StatusCode::BAD_REQUEST, e.to_string()));
                        }
                    };

                    let mut apikey = [0u8; 32];
                    OsRng.fill_bytes(&mut apikey);
                    let api_key_base64 = general_purpose::STANDARD.encode(apikey);

                    let mut apikey_list_mut = apikey_list.write().await;
                    create_apikey_api(
                        &index_path,
                        apikey_quota_object,
                        &apikey,
                        &mut apikey_list_mut,
                    );
                    drop(apikey_list_mut);

                    Ok(Response::new(api_key_base64.into()))
                } else {
                    Ok(status(StatusCode::UNAUTHORIZED, String::new()))
                }
            } else {
                Ok(status(StatusCode::UNAUTHORIZED, String::new()))
            }
        }

        ("api", "v1", "apikey", "", "", "", &Method::DELETE) => {
            if let Some(apikey_header) = headers.get("apikey") {
                let mut hasher = Sha256::new();
                hasher.update(MASTER_KEY_SECRET.to_string());
                let master_apikey = hasher.finalize();
                let master_apikey_base64 = general_purpose::STANDARD.encode(master_apikey);

                if apikey_header.to_str().unwrap_or("") == master_apikey_base64 {
                    let request_bytes = body::to_bytes(req.into_body()).await.unwrap();
                    let request_object: DeleteApikeyRequest =
                        match serde_json::from_slice(&request_bytes) {
                            Ok(request_object) => request_object,
                            Err(e) => {
                                return Ok(status(StatusCode::BAD_REQUEST, e.to_string()));
                            }
                        };

                    let Ok(apikey) =
                        general_purpose::STANDARD.decode(&request_object.apikey_base64)
                    else {
                        return Ok(status(StatusCode::UNAUTHORIZED, String::new()));
                    };

                    let apikey_hash = calculate_hash(&apikey) as u128;

                    let mut apikey_list_mut = apikey_list.write().await;
                    let result = delete_apikey_api(&index_path, &mut apikey_list_mut, apikey_hash);
                    drop(apikey_list_mut);

                    match result {
                        Ok(count) => Ok(Response::new(count.to_string().into())),
                        Err(_) => Ok(status(
                            StatusCode::NOT_FOUND,
                            "api_key does not exists".to_string(),
                        )),
                    }
                } else {
                    Ok(status(
                        StatusCode::UNAUTHORIZED,
                        "api_key invalid or missing".to_string(),
                    ))
                }
            } else {
                Ok(status(
                    StatusCode::UNAUTHORIZED,
                    "api_key invalid or missing".to_string(),
                ))
            }
        }

        ("api", "v1", "status", "", "", "", &Method::GET) => {
            Ok(status(StatusCode::NOT_IMPLEMENTED, String::new()))
        }

        (_, _, _, _, _, _, &Method::GET) => match path {
            "/" => Ok(Response::new(INDEX_HTML.into())),
            "/css/flexboxgrid.min.css" => Ok(Response::new(FLEXBOX_CSS.into())),
            "/css/master.css" => Ok(Response::new(MASTER_CSS.into())),
            "/js/master.js" => Ok(Response::new(MASTER_JS.into())),
            "/js/jquery-3.7.1.min.js" => Ok(Response::new(JQUERY_JS.into())),

            "/svg/logo.svg" => {
                let body: Body = LOGO_SVG.into();
                let response = Response::builder()
                    .header("Content-Type", "image/svg+xml")
                    .header("content-length", LOGO_SVG.len())
                    .body(body)
                    .unwrap();
                Ok(response)
            }

            "/favicon-16x16.png" => Ok(Response::new(FAVICON_16.into())),
            "/favicon-32x32.png" => Ok(Response::new(FAVICON_32.into())),
            "/version" => Ok(Response::new(VERSION.into())),
            _ => Ok(status(StatusCode::NOT_IMPLEMENTED, String::new())),
        },
        _ => Ok(status(StatusCode::NOT_IMPLEMENTED, String::new())),
    }
}

pub(crate) async fn http_server(
    index_path: &Path,
    apikey_list: Arc<tokio::sync::RwLock<HashMap<u128, ApikeyObject>>>,
    local_ip: &String,
    local_port: &u16,
) {
    let addr: SocketAddr = format!("{}:{}", local_ip, local_port)
        .parse()
        .expect("Unable to parse socket address");

    let make_svc = make_service_fn(move |conn: &AddrStream| {
        let index_path = index_path.to_path_buf();
        let addr = conn.remote_addr();
        let apikey_list = apikey_list.clone();

        async move {
            Ok::<_, Infallible>(service_fn(move |req| {
                http_request_handler(index_path.clone(), apikey_list.clone(), req, addr)
            }))
        }
    });

    match Server::try_bind(&addr) {
        Ok(s) => {
            let server = s.serve(make_svc);

            let mut hasher = Sha256::new();
            hasher.update(MASTER_KEY_SECRET.to_string());
            let peer_master_apikey = hasher.finalize();
            let peer_master_apikey_base64 = general_purpose::STANDARD.encode(peer_master_apikey);

            println!(
                "Listening on: {} {} index dir {} master key {}\n\n",
                local_ip,
                local_port,
                index_path.display(),
                peer_master_apikey_base64
            );

            io::stdout().flush().unwrap();

            if let Err(e) = server.await {
                eprintln!("server error: {}", e);
            }
        }

        Err(_e) => {
            println!(
                "Starting the server at {:?} failed. \
                Check if there is another SeekStorm server instance running on the same port. \
                Try changing the port.",
                addr
            );
            process::exit(1)
        }
    };
}
