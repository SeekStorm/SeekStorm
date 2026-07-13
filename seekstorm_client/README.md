# SeekStorm Rust client

Rust client for the SeekStorm open-source, sub-millisecond vector & lexical search server.

<img src="../assets/logo.png" width="450" alt="Logo"><br>
[![Crates.io](https://img.shields.io/crates/v/seekstorm_client.svg)](https://crates.io/crates/seekstorm_client)
[![Downloads](https://img.shields.io/crates/d/seekstorm_client.svg?style=flat-square)](https://crates.io/crates/seekstorm_client)
[![Documentation](https://docs.rs/seekstorm_client/badge.svg)](https://docs.rs/seekstorm_client)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/SeekStorm/SeekStorm?tab=Apache-2.0-1-ov-file#readme)

## Installation

```bash
cargo add seekstorm_client
```

## Example


### Add required crates to your project
```text
cargo add seekstorm_client_rs
cargo add tokio
cargo add serde_json
```

### use an asynchronous Rust runtime
```rust ,no_run
 use std::error::Error;
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {

// your SeekStorm code here

   Ok(())
}
```
### live
```rust ,no_run
# tokio_test::block_on(async {
use seekstorm_client_rs::api_endpoints::RestClient;
use std::sync::LazyLock;

pub static BASE_URL: &str = "http://127.0.0.1:80";
pub static CLIENT: LazyLock<RestClient> = LazyLock::new(|| RestClient::new());

let result=CLIENT.live(BASE_URL).await;
# });
```
### create API key
```rust ,no_run
# tokio_test::block_on(async {
use seekstorm::{
     index::{
         self, ApikeyQuotaObject,
     },
 };
use seekstorm_client_rs::api_endpoints::RestClient;
use std::sync::LazyLock;

pub static BASE_URL: &str = "http://127.0.0.1:80";
pub static MASTER_API_KEY: &str = "/iWStCpyfpd/BVlHOFtwnMgrFrmof4jGq/OQDWXQzcM=";
pub static CLIENT: LazyLock<RestClient> = LazyLock::new(|| RestClient::new());

let apikey_quota_object=ApikeyQuotaObject {
   indices_max: 10,
   indices_size_max: 100_000_000_000,
   documents_max: 100_000_000,
   operations_max: 1_000_000_000,
   rate_limit:None,
   demo: true,
   ..Default::default()
 };

 let result = CLIENT
   .create_apikey(BASE_URL, MASTER_API_KEY, &apikey_quota_object)
   .await;
 # });
```
### delete API key
```rust ,no_run
# tokio_test::block_on(async {
use seekstorm_client_rs::api_endpoints::RestClient;
use std::sync::LazyLock;

pub static BASE_URL: &str = "http://127.0.0.1:80";
pub static DEMO_API_KEY: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
pub static MASTER_API_KEY: &str = "/iWStCpyfpd/BVlHOFtwnMgrFrmof4jGq/OQDWXQzcM=";
pub static CLIENT: LazyLock<RestClient> = LazyLock::new(|| RestClient::new());

let result = CLIENT
  .delete_apikey(BASE_URL, DEMO_API_KEY, MASTER_API_KEY)
  .await;
# });
```
### get API key info
```rust ,no_run
# tokio_test::block_on(async {
use seekstorm_client_rs::api_endpoints::RestClient;
use std::sync::LazyLock;

pub static BASE_URL: &str = "http://127.0.0.1:80";
pub static DEMO_API_KEY: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
pub static CLIENT: LazyLock<RestClient> = LazyLock::new(|| RestClient::new());

let result = CLIENT
  .get_apikey_info(BASE_URL, DEMO_API_KEY)
  .await;
# });
```
### create index
```rust ,no_run
# tokio_test::block_on(async {
use seekstorm::{
    index::{
        self, ApikeyQuotaObject, Clustering, CreateIndexRequest, DocumentCompression, FrequentwordType, LexicalSimilarity, NgramSet, StemmerType, StopwordType, TokenizerType,
    }, vector::Inference,
};
use seekstorm_client_rs::api_endpoints::RestClient;
use std::sync::LazyLock;

pub static BASE_URL: &str = "http://127.0.0.1:80";
pub static DEMO_API_KEY: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
pub static CLIENT: LazyLock<RestClient> = LazyLock::new(|| RestClient::new());

let schema_json = r#"
  [{"field":"title","field_type":"Text","store":false,"index_lexical":false},
  {"field":"body","field_type":"Text","store":true,"index_lexical":true,"longest":true},
  {"field":"url","field_type":"Text","store":false,"index_lexical":false}]"#;
let schema = serde_json::from_str(schema_json).unwrap();

let create_index_request = CreateIndexRequest {
  index_name: "test_index".into(),
  similarity: LexicalSimilarity::Bm25f,
  tokenizer: TokenizerType::UnicodeAlphanumeric,
  stemmer: StemmerType::None,
  stop_words: StopwordType::None,
  frequent_words: FrequentwordType::English,
  synonyms: Vec::new(), //not supported in REST API?
  ngram_indexing: NgramSet::NgramFF as u8 | NgramSet::NgramFFF as u8,
  document_compression: DocumentCompression::Snappy,
  spelling_correction: None,
  query_completion: None,
  clustering: Clustering::None,
  inference: Inference::None,
  schema,
};
let result = CLIENT
  .create_index(BASE_URL, DEMO_API_KEY, &create_index_request)
 .await;
# });
```
### get index info
```rust ,no_run
# tokio_test::block_on(async {
use seekstorm_client_rs::api_endpoints::RestClient;
use std::sync::LazyLock;

pub static BASE_URL: &str = "http://127.0.0.1:80";
pub static DEMO_API_KEY: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
pub static CLIENT: LazyLock<RestClient> = LazyLock::new(|| RestClient::new());
let index_id=0;

let result = CLIENT
  .get_index_info(BASE_URL, DEMO_API_KEY, index_id)
  .await;
# });
```
### index document
```rust ,no_run
# tokio_test::block_on(async {
use seekstorm_client_rs::api_endpoints::RestClient;
use std::sync::LazyLock;
use seekstorm::index::Document;

pub static BASE_URL: &str = "http://127.0.0.1:80";
pub static DEMO_API_KEY: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
pub static CLIENT: LazyLock<RestClient> = LazyLock::new(|| RestClient::new());

let document_json = r#"
{"title":"title1 test","body":"body1","url":"url1"}"#;
let document=serde_json::from_str(document_json).unwrap();
let _result = CLIENT.index_document(BASE_URL, DEMO_API_KEY, 0,&document).await;
# });
```
### index documents
```rust ,no_run
# tokio_test::block_on(async {
use seekstorm_client_rs::api_endpoints::RestClient;
use std::sync::LazyLock;
use seekstorm::index::Document;

pub static BASE_URL: &str = "http://127.0.0.1:80";
pub static DEMO_API_KEY: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
pub static CLIENT: LazyLock<RestClient> = LazyLock::new(|| RestClient::new());

let documents_json = r#"
[{"title":"title1 test","body":"body1","url":"url1"},
{"title":"title2","body":"body2 test","url":"url2"},
{"title":"title3 test","body":"body3 test","url":"url3"}]"#;
let documents_vec:Vec<Document>=serde_json::from_str(documents_json).unwrap();

CLIENT.index_documents(BASE_URL, DEMO_API_KEY, 0, &documents_vec).await;
# });
```
### delete document by document id
```rust ,no_run
# tokio_test::block_on(async {
use seekstorm_client_rs::api_endpoints::RestClient;
use std::sync::LazyLock;

pub static BASE_URL: &str = "http://127.0.0.1:80";
pub static DEMO_API_KEY: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
pub static CLIENT: LazyLock<RestClient> = LazyLock::new(|| RestClient::new());

let index_id=0;
let doc_id=1;
CLIENT.delete_document_by_docid(BASE_URL, DEMO_API_KEY, index_id, doc_id).await;
# });
```
### delete documents by document id
```rust ,no_run
# tokio_test::block_on(async {
use seekstorm_client_rs::api_endpoints::RestClient;
use std::sync::LazyLock;

pub static BASE_URL: &str = "http://127.0.0.1:80";
pub static DEMO_API_KEY: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
pub static CLIENT: LazyLock<RestClient> = LazyLock::new(|| RestClient::new());

let docid_vec=vec![1,2];
CLIENT.delete_documents_by_docid(BASE_URL, DEMO_API_KEY, 0, docid_vec).await;
# });
```
### delete documents by query
```rust ,no_run
# tokio_test::block_on(async {
use seekstorm::{
    index::{
        self, ApikeyQuotaObject, Clustering, CreateIndexRequest, SearchRequestObject, DocumentCompression, FrequentwordType, LexicalSimilarity, NgramSet, StemmerType, StopwordType, TokenizerType,
    }, search::{QueryRewriting, QueryType, ResultType, SearchMode}, vector::Inference,
};
use seekstorm_client_rs::api_endpoints::RestClient;
use std::sync::LazyLock;

pub static BASE_URL: &str = "http://127.0.0.1:80";
pub static DEMO_API_KEY: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
pub static CLIENT: LazyLock<RestClient> = LazyLock::new(|| RestClient::new());

let query = "test".into();

let search_request_object = SearchRequestObject {
  query_string: query,
  query_vector: None,
  enable_empty_query: false,
  offset: 0,
  length: 10,
  result_type: ResultType::TopkCount,
  query_type_default: QueryType::Intersection,
  search_mode: SearchMode::Lexical,
  realtime: false,
  query_rewriting: QueryRewriting::SearchOnly,
  highlights: Vec::new(),
  fields: Vec::new(),
  field_filter: Vec::new(),
  facet_filter: Vec::new(),
  distance_fields: Vec::new(),
  query_facets: Vec::new(),
  result_sort: Vec::new(),
};

CLIENT.delete_documents_by_query(BASE_URL, DEMO_API_KEY, 0, &search_request_object).await;
# });
```
### update document
```rust ,no_run
# tokio_test::block_on(async {
use seekstorm_client_rs::api_endpoints::RestClient;
use std::sync::LazyLock;
use seekstorm::index::Document;

pub static BASE_URL: &str = "http://127.0.0.1:80";
pub static DEMO_API_KEY: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
pub static CLIENT: LazyLock<RestClient> = LazyLock::new(|| RestClient::new());

let id_document_json = r#"
[2,{"title":"title3 test","body":"body3 test","url":"url3"}]"#;
let id_document=serde_json::from_str(id_document_json).unwrap();
CLIENT.update_document(BASE_URL, DEMO_API_KEY, 0, id_document).await;

// ### commit index
let result = CLIENT.commit_index(BASE_URL, DEMO_API_KEY, 0).await;
# });
```
### update documents
```rust ,no_run
# tokio_test::block_on(async {
use seekstorm_client_rs::api_endpoints::RestClient;
use std::sync::LazyLock;
use seekstorm::index::Document;

pub static BASE_URL: &str = "http://127.0.0.1:80";
pub static DEMO_API_KEY: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
pub static CLIENT: LazyLock<RestClient> = LazyLock::new(|| RestClient::new());

let id_document_vec_json = r#"
[[1,{"title":"title1 test","body":"body1","url":"url1"}],
[2,{"title":"title3 test","body":"body3 test","url":"url3"}]]"#;
let id_document_vec=serde_json::from_str(id_document_vec_json).unwrap();
CLIENT.update_documents(BASE_URL, DEMO_API_KEY, 0, id_document_vec).await;

// ### commit index
let result = CLIENT.commit_index(BASE_URL, DEMO_API_KEY, 0).await;
# });
```
### query index
```rust ,no_run
# tokio_test::block_on(async {
use seekstorm::{
    index::{
        self, ApikeyQuotaObject, Clustering, CreateIndexRequest, SearchRequestObject, DocumentCompression, FrequentwordType, LexicalSimilarity, NgramSet, StemmerType, StopwordType, TokenizerType,
    }, search::{QueryRewriting, QueryType, ResultType, SearchMode}, vector::Inference,
};
use seekstorm_client_rs::api_endpoints::RestClient;
use std::sync::LazyLock;

pub static BASE_URL: &str = "http://127.0.0.1:80";
pub static DEMO_API_KEY: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
pub static CLIENT: LazyLock<RestClient> = LazyLock::new(|| RestClient::new());

let query = "test".into();

let search_request_object = SearchRequestObject {
  query_string: query,
  query_vector: None,
  enable_empty_query: false,
  offset: 0,
  length: 10,
  result_type: ResultType::TopkCount,
  query_type_default: QueryType::Intersection,
  search_mode: SearchMode::Lexical,
  realtime: false,
  query_rewriting: QueryRewriting::SearchOnly,
  highlights: Vec::new(),
  fields: Vec::new(),
  field_filter: Vec::new(),
  facet_filter: Vec::new(),
  distance_fields: Vec::new(),
  query_facets: Vec::new(),
  result_sort: Vec::new(),
};
let result_object = CLIENT.query_index(BASE_URL, DEMO_API_KEY, 0,search_request_object).await;

// ### display results
for result in result_object.as_ref().unwrap().results.iter() {
  println!("result {:?} rank {:?} body field {:?}" , result.get("_id"),result.get("_score"), result.get("body"));
}
println!("result counts {} {} {}",result_object.as_ref().unwrap().results.len(), result_object.as_ref().unwrap().count, result_object.as_ref().unwrap().count_total);
# });
```
### get document
```rust ,no_run
# tokio_test::block_on(async {
use seekstorm_client_rs::api_endpoints::RestClient;
use std::sync::LazyLock;
use seekstorm::{index::GetDocumentRequest, highlighter::{Highlight, highlighter}};
use std::collections::HashSet;

pub static BASE_URL: &str = "http://127.0.0.1:80";
pub static DEMO_API_KEY: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
pub static CLIENT: LazyLock<RestClient> = LazyLock::new(|| RestClient::new());

let index_id=0;
let doc_id=0;
let highlights:Vec<Highlight>= vec![
    Highlight {
        field: "body".to_string(),
        name:String::new(),
        fragment_number: 2,
        fragment_size: 160,
        highlight_markup: true,
        ..Default::default()
        },
    ];    

     let get_document_request = GetDocumentRequest {
        query_terms: Vec::new(),
        highlights: highlights,
        fields: Vec::new(),
        distance_fields: Vec::new(),
    };
//
    let doc=CLIENT.get_document(BASE_URL, DEMO_API_KEY, index_id,doc_id,&get_document_request).await.unwrap();
# });
```
### document iterator
```rust ,no_run
# tokio_test::block_on(async {
use seekstorm_client_rs::api_endpoints::RestClient;
use std::sync::LazyLock;
use seekstorm::{index::GetIteratorRequest};

pub static BASE_URL: &str = "http://127.0.0.1:80";
pub static DEMO_API_KEY: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
pub static CLIENT: LazyLock<RestClient> = LazyLock::new(|| RestClient::new());

let index_id=0;

let get_iterator_request = GetIteratorRequest {
  document_id: Some(0),
  skip: 0,
  take: 1,
  include_deleted: false,
  include_document: true,
  fields: Vec::new(),
};
//
let result=CLIENT.document_iterator(BASE_URL, DEMO_API_KEY, index_id,get_iterator_request).await;
# });
```
### index PDF file bytes
```rust ,no_run
# tokio_test::block_on(async {
use seekstorm_client_rs::api_endpoints::RestClient;
use std::sync::LazyLock;
use std::fs;
use std::path::Path;
use chrono::Utc;

pub static BASE_URL: &str = "http://127.0.0.1:80";
pub static DEMO_API_KEY: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
pub static CLIENT: LazyLock<RestClient> = LazyLock::new(|| RestClient::new());

let index_id=0;
let file_path=Path::new("C:/test.pdf");
let file_date=Utc::now().timestamp();
let document = fs::read(file_path).unwrap();
let result=CLIENT.index_pdf(BASE_URL, DEMO_API_KEY, index_id, file_path, file_date, document).await;
# });
```
### get PDF file bytes
```rust ,no_run
# tokio_test::block_on(async {
use seekstorm_client_rs::api_endpoints::RestClient;
use std::sync::LazyLock;

pub static BASE_URL: &str = "http://127.0.0.1:80";
pub static DEMO_API_KEY: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
pub static CLIENT: LazyLock<RestClient> = LazyLock::new(|| RestClient::new());

let index_id=0;
let doc_id=0;

let result=CLIENT.get_pdf(BASE_URL, DEMO_API_KEY, index_id, doc_id).await;
# });
```
### clear index
```rust ,no_run
# tokio_test::block_on(async {
use seekstorm_client_rs::api_endpoints::RestClient;
use std::sync::LazyLock;

pub static BASE_URL: &str = "http://127.0.0.1:80";
pub static DEMO_API_KEY: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
pub static CLIENT: LazyLock<RestClient> = LazyLock::new(|| RestClient::new());

let index_id=0;

let result=CLIENT.clear_index(BASE_URL, DEMO_API_KEY, index_id).await;
# });
```
### delete index
```rust ,no_run
# tokio_test::block_on(async {
use seekstorm_client_rs::api_endpoints::RestClient;
use std::sync::LazyLock;

pub static BASE_URL: &str = "http://127.0.0.1:80";
pub static DEMO_API_KEY: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
pub static CLIENT: LazyLock<RestClient> = LazyLock::new(|| RestClient::new());

let index_id=0;

let result=CLIENT.delete_index(BASE_URL, DEMO_API_KEY, index_id).await;
# });
```