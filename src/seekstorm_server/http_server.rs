use seekstorm::index::Document;
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
use sha2::Digest;
use sha2::Sha256;
use std::{convert::Infallible, net::SocketAddr};

use base64::{engine::general_purpose, Engine as _};

use crate::api_endpoints::delete_apikey_api;
use crate::api_endpoints::delete_document_api;
use crate::api_endpoints::delete_documents_api;
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
use crate::api_endpoints::{commit_index_api, create_apikey_api};
use crate::api_endpoints::{create_index_api, QueryObjectPost};
use crate::multi_tenancy::get_apikey_hash;
use crate::multi_tenancy::ApikeyObject;
use crate::multi_tenancy::ApikeyQuotaObject;
use crate::server::DEBUG;
use crate::{SECRET_MASTER_KEY, VERSION};

const INDEX_HTML: &str = include_str!("web/index.html");
const FLEXBOX_CSS: &str = include_str!("web/css/flexboxgrid.min.css");
const MASTER_CSS: &str = include_str!("web/css/master.css");
const MASTER_JS: &str = include_str!("web/js/master.js");
const JQUERY_JS: &str = include_str!("web/js/jquery-3.3.1.min.js");
const LOGO_SVG: &[u8] = include_bytes!("web/svg/logo.svg");
const FAVICON_16: &[u8] = include_bytes!("web/favicon-16x16.png");
const FAVICON_32: &[u8] = include_bytes!("web/favicon-32x32.png");

pub(crate) fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}

pub(crate) fn not_found() -> Response<Body> {
    Response::builder()
        .status(StatusCode::NOT_FOUND)
        .body(Body::empty())
        .unwrap()
}

pub(crate) fn unauthorized() -> Response<Body> {
    Response::builder()
        .status(StatusCode::UNAUTHORIZED)
        .body(Body::empty())
        .unwrap()
}

pub(crate) fn not_implemented() -> Response<Body> {
    Response::builder()
        .status(StatusCode::NOT_IMPLEMENTED)
        .body(Body::empty())
        .unwrap()
}

pub(crate) async fn http_request_handler(
    index_path: PathBuf,
    apikey_list: Arc<tokio::sync::RwLock<HashMap<u128, ApikeyObject>>>,
    req: Request<Body>,
    remote_addr: SocketAddr,
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
                    if DEBUG {
                        println!("search API POST request: {}", req.uri());
                    }

                    let index_id: u64 = parts[3].parse().unwrap();

                    let apikey_list_ref = apikey_list.read().await;
                    let index_arc = &apikey_list_ref[&apikey_hash].index_list[&index_id];
                    let index_arc_clone = index_arc.clone();
                    drop(apikey_list_ref);

                    let request_bytes = body::to_bytes(req.into_body()).await.unwrap();
                    let request_json =
                        serde_json::from_slice::<QueryObjectPost>(&request_bytes).unwrap();

                    let query_string = request_json.query_string;
                    let api_offset = request_json.offset;
                    let api_length = request_json.length;

                    let search_result_local = query_index_api(
                        &index_arc_clone,
                        &query_string,
                        api_offset,
                        api_length,
                        request_json.highlights,
                        request_json.realtime,
                        request_json.field_filter,
                    )
                    .await;

                    if DEBUG {
                        println!(
                            "search result object {} {} : {} {} {} ",
                            api_offset,
                            api_length,
                            search_result_local.offset,
                            search_result_local.length,
                            search_result_local.results.len()
                        );
                    }

                    let search_result_json = serde_json::to_string(&search_result_local).unwrap();
                    Ok(Response::new(search_result_json.into()))
                } else {
                    Ok(unauthorized())
                }
            } else {
                Ok(unauthorized())
            }
        }

        ("api", "v1", "index", _, "query", _, &Method::GET) => {
            if let Some(apikey) = headers.get("apikey") {
                if let Some(apikey_hash) =
                    get_apikey_hash(apikey.to_str().unwrap().to_string(), &apikey_list).await
                {
                    if DEBUG {
                        println!("search API GET request: {}", req.uri());
                    }

                    let index_id: u64 = parts[3].parse().unwrap();

                    let apikey_list_ref = apikey_list.read().await;
                    let index_arc = &apikey_list_ref[&apikey_hash].index_list[&index_id];
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

                    let mut query_string = "";
                    if let Some(value) = params.get("query") {
                        query_string = value;
                    }
                    let mut api_offset = 0;
                    if let Some(value) = params.get("offset") {
                        api_offset = value.parse::<usize>().unwrap();
                    }
                    let mut api_length = 10;
                    if let Some(value) = params.get("length") {
                        api_length = value.parse::<usize>().unwrap();
                    }

                    let highlights = Vec::new();

                    let search_result_local = query_index_api(
                        &index_arc_clone,
                        query_string,
                        api_offset,
                        api_length,
                        highlights,
                        false,
                        Vec::new(),
                    )
                    .await;
                    let search_result_json = serde_json::to_string(&search_result_local).unwrap();

                    if DEBUG {
                        println!(
                            "search result object {} {} : {} {} {} : {}",
                            api_offset,
                            api_length,
                            search_result_local.offset,
                            search_result_local.length,
                            search_result_local.results.len(),
                            search_result_json
                        );
                    }

                    Ok(Response::new(search_result_json.into()))
                } else {
                    Ok(unauthorized())
                }
            } else {
                Ok(unauthorized())
            }
        }

        ("api", "v1", "index", "", _, _, &Method::POST) => {
            if let Some(apikey) = headers.get("apikey") {
                if let Some(apikey_hash) =
                    get_apikey_hash(apikey.to_str().unwrap().to_string(), &apikey_list).await
                {
                    let request_bytes = body::to_bytes(req.into_body()).await.unwrap();
                    let request_string = str::from_utf8(&request_bytes).unwrap();
                    let create_index_request_object: CreateIndexRequest =
                        serde_json::from_str(request_string).unwrap();

                    let mut apikey_list_mut = apikey_list.write().await;
                    let apikey_object = apikey_list_mut.get_mut(&apikey_hash).unwrap();
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
                    Ok(unauthorized())
                }
            } else {
                Ok(unauthorized())
            }
        }

        ("api", "v1", "index", _, "", "", &Method::DELETE) => {
            if let Some(apikey) = headers.get("apikey") {
                if let Some(apikey_hash) =
                    get_apikey_hash(apikey.to_str().unwrap().to_string(), &apikey_list).await
                {
                    let index_id: u64 = parts[3].parse().unwrap();

                    let mut apikey_list_mut = apikey_list.write().await;
                    let apikey_object = apikey_list_mut.get_mut(&apikey_hash).unwrap();
                    let _ = delete_index_api(index_id, &mut apikey_object.index_list).await;

                    let index_count = apikey_object.index_list.len();
                    drop(apikey_list_mut);

                    Ok(Response::new(index_count.to_string().into()))
                } else {
                    Ok(unauthorized())
                }
            } else {
                Ok(unauthorized())
            }
        }

        ("api", "v1", "index", _, "", "", &Method::PATCH) => {
            if let Some(apikey) = headers.get("apikey") {
                if let Some(apikey_hash) =
                    get_apikey_hash(apikey.to_str().unwrap().to_string(), &apikey_list).await
                {
                    let index_id: u64 = parts[3].parse().unwrap();
                    let apikey_list_ref = apikey_list.read().await;
                    let apikey_object = apikey_list_ref.get(&apikey_hash).unwrap();
                    let index_arc = apikey_object.index_list.get(&index_id).unwrap();
                    let index_arc_clone = index_arc.clone();
                    drop(apikey_list_ref);
                    let result = commit_index_api(&index_arc_clone).await;

                    Ok(Response::new(result.unwrap().to_string().into()))
                } else {
                    Ok(unauthorized())
                }
            } else {
                Ok(unauthorized())
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
                    Ok(unauthorized())
                }
            } else {
                Ok(unauthorized())
            }
        }

        ("api", "v1", "index", _, "", _, &Method::GET) => {
            if let Some(apikey) = headers.get("apikey") {
                if let Some(apikey_hash) =
                    get_apikey_hash(apikey.to_str().unwrap().to_string(), &apikey_list).await
                {
                    let index_id: u64 = parts[3].parse().unwrap();

                    let apikey_list_ref = apikey_list.read().await;
                    let apikey_object = apikey_list_ref.get(&apikey_hash).unwrap();
                    let status_object =
                        get_index_stats_api(&index_path, index_id, &apikey_object.index_list)
                            .await
                            .unwrap();

                    drop(apikey_list_ref);
                    let status_object_json = serde_json::to_string(&status_object).unwrap();
                    Ok(Response::new(status_object_json.into()))
                } else {
                    Ok(unauthorized())
                }
            } else {
                Ok(unauthorized())
            }
        }

        ("api", "v1", "index", _, "doc", _, &Method::POST) => {
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

                    let is_doc_vector = request_string.trim().starts_with('[');
                    let status_object = if !is_doc_vector {
                        let document_object: Document =
                            serde_json::from_str(request_string).unwrap();
                        index_document_api(&index_arc_clone, document_object).await
                    } else {
                        let document_object_vec: Vec<Document> =
                            serde_json::from_str(request_string).unwrap();
                        index_documents_api(&index_arc_clone, document_object_vec).await
                    };
                    let status_object_json = serde_json::to_string(&status_object).unwrap();
                    Ok(Response::new(status_object_json.into()))
                } else {
                    Ok(unauthorized())
                }
            } else {
                Ok(unauthorized())
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

                    let is_doc_vector = request_string.trim().starts_with('[');
                    let status_object = if !is_doc_vector {
                        let document_object: Document =
                            serde_json::from_str(request_string).unwrap();
                        update_document_api(&index_arc_clone, document_object).await
                    } else {
                        let document_object_vec: Vec<Document> =
                            serde_json::from_str(request_string).unwrap();
                        update_documents_api(&index_arc_clone, document_object_vec).await
                    };

                    let status_object_json = serde_json::to_string(&status_object).unwrap();
                    Ok(Response::new(status_object_json.into()))
                } else {
                    Ok(unauthorized())
                }
            } else {
                Ok(unauthorized())
            }
        }

        ("api", "v1", "index", _, "doc", _, &Method::GET) => {
            if let Some(apikey) = headers.get("apikey") {
                if let Some(apikey_hash) =
                    get_apikey_hash(apikey.to_str().unwrap().to_string(), &apikey_list).await
                {
                    let index_id: u64 = parts[3].parse().unwrap();
                    let doc_id = parts[5].to_string();
                    let apikey_list_ref = apikey_list.read().await;
                    let apikey_object = apikey_list_ref.get(&apikey_hash).unwrap();
                    let index_arc = apikey_object.index_list.get(&index_id).unwrap();
                    let status_object = get_document_api(index_arc, doc_id).await;
                    drop(apikey_list_ref);
                    let status_object_json = serde_json::to_string(&status_object).unwrap();
                    Ok(Response::new(status_object_json.into()))
                } else {
                    Ok(unauthorized())
                }
            } else {
                Ok(unauthorized())
            }
        }

        ("api", "v1", "index", _, "doc", _, &Method::DELETE) => {
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

                    let is_doc_vector = request_string.trim().starts_with('[');
                    let status_object = if !is_doc_vector {
                        let document_id: String = serde_json::from_str(request_string).unwrap();
                        delete_document_api(&index_arc_clone, document_id).await
                    } else {
                        let document_id_vec: Vec<String> =
                            serde_json::from_str(request_string).unwrap();
                        delete_documents_api(&index_arc_clone, document_id_vec).await
                    };
                    let status_object_json = serde_json::to_string(&status_object).unwrap();
                    Ok(Response::new(status_object_json.into()))
                } else {
                    Ok(unauthorized())
                }
            } else {
                Ok(unauthorized())
            }
        }

        ("api", "v1", "apikey", "", "", "", &Method::POST) => {
            if let Some(apikey_header) = headers.get("apikey") {
                let mut hasher = Sha256::new();
                hasher.update(SECRET_MASTER_KEY);
                let peer_master_apikey = hasher.finalize();
                let peer_master_apikey_base64 =
                    general_purpose::STANDARD.encode(peer_master_apikey);

                if apikey_header.to_str().unwrap() == peer_master_apikey_base64 {
                    let request_bytes = body::to_bytes(req.into_body()).await.unwrap();
                    let request_string = str::from_utf8(&request_bytes).unwrap();
                    let apikey_quota_object: ApikeyQuotaObject =
                        serde_json::from_str(request_string).unwrap();

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

                    if DEBUG {
                        println!("user_key: {}", api_key_base64);
                    }
                    Ok(Response::new(api_key_base64.into()))
                } else {
                    Ok(unauthorized())
                }
            } else {
                Ok(unauthorized())
            }
        }

        ("api", "v1", "apikey", "", "", "", &Method::DELETE) => {
            if let Some(apikey_header) = headers.get("apikey") {
                let mut hasher = Sha256::new();
                hasher.update(SECRET_MASTER_KEY);
                let master_apikey = hasher.finalize();
                let master_apikey_base64 = general_purpose::STANDARD.encode(master_apikey);

                if apikey_header.to_str().unwrap() == master_apikey_base64 {
                    let request_bytes = body::to_bytes(req.into_body()).await.unwrap();
                    let request_string = str::from_utf8(&request_bytes).unwrap();
                    let request_object: DeleteApikeyRequest =
                        serde_json::from_str(request_string).unwrap();

                    let apikey = general_purpose::STANDARD
                        .decode(&request_object.apikey_base64)
                        .unwrap();

                    let apikey_hash = calculate_hash(&apikey) as u128;

                    let mut apikey_list_mut = apikey_list.write().await;
                    let result = delete_apikey_api(&index_path, &mut apikey_list_mut, apikey_hash);
                    drop(apikey_list_mut);

                    match result {
                        Ok(count) => Ok(Response::new(count.to_string().into())),
                        Err(_) => Ok(not_found()),
                    }
                } else {
                    Ok(unauthorized())
                }
            } else {
                Ok(unauthorized())
            }
        }

        ("api", "v1", "status", "", "", "", &Method::GET) => Ok(not_implemented()),

        (_, _, _, _, _, _, &Method::GET) => {
            if DEBUG {
                println!(
                    "search UI GET request: {} from {} {}",
                    path,
                    remote_addr.ip(),
                    remote_addr.port()
                );
            }

            match path {
                "/" => Ok(Response::new(INDEX_HTML.into())),
                "/css/flexboxgrid.min.css" => Ok(Response::new(FLEXBOX_CSS.into())),
                "/css/master.css" => Ok(Response::new(MASTER_CSS.into())),
                "/js/master.js" => Ok(Response::new(MASTER_JS.into())),
                "/js/jquery-3.3.1.min.js" => Ok(Response::new(JQUERY_JS.into())),

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
                _ => Ok(not_implemented()),
            }
        }
        _ => {
            if DEBUG {
                println!("invalid request received: {}", req.uri());
            }

            Ok(not_implemented())
        }
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
            hasher.update(SECRET_MASTER_KEY);
            let peer_master_apikey = hasher.finalize();
            let peer_master_apikey_base64 = general_purpose::STANDARD.encode(peer_master_apikey);

            if DEBUG {
                println!(
                    "server listening on: {} {} master key {}\n\n",
                    local_ip, local_port, peer_master_apikey_base64
                );
            } else {
                println!(
                    "Listening on: {} {} index dir {} master key {}\n\n",
                    local_ip,
                    local_port,
                    index_path.display(),
                    peer_master_apikey_base64
                );
            }
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
