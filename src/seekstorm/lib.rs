#![crate_type = "lib"]
#![crate_name = "seekstorm"]
#![doc(html_logo_url = "http://seekstorm.com/assets/logo.svg")]
#![doc(html_favicon_url = "http://seekstorm.com/favicon.ico")]

//! # `seekstorm`
//! SeekStorm is an open-source, sub-millisecond full-text search library & multi-tenancy server written in Rust.
//! The **SeekStorm library** can be embedded into your program, while the **SeekStorm server** is a standalone search server to be accessed via HTTP.
//! ### Add required crates to your project
//! ```text
//! cargo add seekstorm
//! cargo add tokio
//! cargo add serde_json
//! ```
//! ### use an asynchronous Rust runtime
//! ```no_run
//! use std::error::Error;
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
//!
//! // your SeekStorm code here
//!
//!   Ok(())
//! }
//! ```
//! ### create index
//! ```no_run
//! # tokio_test::block_on(async {
//! use std::path::Path;
//! use std::sync::{Arc, RwLock};
//! use seekstorm::index::{IndexMetaObject, SimilarityType,TokenizerType,StopwordType,FrequentwordType,AccessType,StemmerType,NgramSet,DocumentCompression,create_index};
//!
//! let index_path=Path::new("C:/index/");
//! let schema_json = r#"
//! [{"field":"title","field_type":"Text","stored":false,"indexed":false},
//! {"field":"body","field_type":"Text","stored":true,"indexed":true},
//! {"field":"url","field_type":"Text","stored":false,"indexed":false}]"#;
//! let schema=serde_json::from_str(schema_json).unwrap();
//! let meta = IndexMetaObject {
//! id: 0,
//! name: "test_index".into(),
//! similarity: SimilarityType::Bm25f,
//! tokenizer: TokenizerType::AsciiAlphabetic,
//! stemmer: StemmerType::None,
//! stop_words: StopwordType::None,
//! frequent_words: FrequentwordType::English,
//! ngram_indexing: NgramSet::NgramFF as u8,
//! document_compression:  DocumentCompression::Snappy,
//! access_type: AccessType::Mmap,
//! spelling_correction: None,
//! query_completion: None,
//! };
//! let segment_number_bits1=11;
//! let serialize_schema=true;
//! let index_arc=create_index(index_path,meta,&schema,&Vec::new(),segment_number_bits1,false,None).await.unwrap();
//! # });
//! ```
//! ### open index (alternatively to create index)
//! ```no_run
//! # tokio_test::block_on(async {
//! use seekstorm::index::open_index;
//! use std::path::Path;
//!
//! let index_path=Path::new("C:/index/");
//! let index_arc=open_index(index_path,false).await.unwrap();
//! # });
//! ```
//! ### index document
//! ```no_run
//! # tokio_test::block_on(async {
//! # use std::path::Path;
//! # use seekstorm::index::open_index;
//! # let index_path=Path::new("C:/index/");
//! # let index_arc=open_index(index_path,false).await.unwrap();
//! use seekstorm::index::IndexDocument;
//! use seekstorm::index::FileType;
//!
//! let document_json = r#"
//! {"title":"title1 test","body":"body1","url":"url1"}"#;
//! let document=serde_json::from_str(document_json).unwrap();
//! index_arc.index_document(document,FileType::None).await;
//! # });
//! ```
//! ### index documents
//! ```no_run
//! # tokio_test::block_on(async {
//! # use std::path::Path;
//! # use seekstorm::index::open_index;
//! # let index_path=Path::new("C:/index/");
//! # let index_arc=open_index(index_path,false).await.unwrap();
//! use seekstorm::index::IndexDocuments;
//! let documents_json = r#"
//! [{"title":"title1 test","body":"body1","url":"url1"},
//! {"title":"title2","body":"body2 test","url":"url2"},
//! {"title":"title3 test","body":"body3 test","url":"url3"}]"#;
//! let documents_vec=serde_json::from_str(documents_json).unwrap();
//! index_arc.index_documents(documents_vec).await;
//! # });
//! ```
//! ### delete documents by document id
//! ```no_run
//! # tokio_test::block_on(async {
//! # use std::path::Path;
//! # use seekstorm::index::open_index;
//! # let index_path=Path::new("C:/index/");
//! # let index_arc=open_index(index_path,false).await.unwrap();
//! use seekstorm::index::DeleteDocuments;
//!
//! let docid_vec=vec![1,2];
//! index_arc.delete_documents(docid_vec).await;
//! # });
//! ```
//! ### delete documents by query
//! ```no_run
//! # tokio_test::block_on(async {
//! # use std::path::Path;
//! # use seekstorm::index::open_index;
//! # let index_path=Path::new("C:/index/");
//! # let index_arc=open_index(index_path,false).await.unwrap();
//! use seekstorm::search::QueryType;
//! use seekstorm::index::DeleteDocumentsByQuery;
//!
//! let query="test".to_string();
//! let offset=0;
//! let length=10;
//! let query_type=QueryType::Intersection;
//! let include_uncommitted=false;
//! let field_filter=Vec::new();
//! let facet_filter=Vec::new();
//! let result_sort=Vec::new();
//! index_arc.delete_documents_by_query(query, query_type, offset, length, include_uncommitted,field_filter,facet_filter,result_sort).await;
//! # });
//! ```
//! ### update documents
//! ```no_run
//! # tokio_test::block_on(async {
//! # use std::path::Path;
//! # use seekstorm::index::open_index;
//! # let index_path=Path::new("C:/index/");
//! # let index_arc=open_index(index_path,false).await.unwrap();
//! use seekstorm::index::UpdateDocuments;
//! use seekstorm::commit::Commit;
//!
//! let id_document_vec_json = r#"
//! [[1,{"title":"title1 test","body":"body1","url":"url1"}],
//! [2,{"title":"title3 test","body":"body3 test","url":"url3"}]]"#;
//! let id_document_vec=serde_json::from_str(id_document_vec_json).unwrap();
//! index_arc.update_documents(id_document_vec).await;
//!
//! // ### commit documents
//!
//! index_arc.commit().await;
//! # });
//! ```
//! ### search index
//! ```no_run
//! # tokio_test::block_on(async {
//! # use std::path::Path;
//! # use seekstorm::index::open_index;
//! # let index_path=Path::new("C:/index/");
//! # let index_arc=open_index(index_path,false).await.unwrap();
//! use seekstorm::search::{Search, QueryType, ResultType, QueryRewriting};
//!
//! let query="test".to_string();
//! let enable_empty_query=false;
//! let offset=10;
//! let length=10;
//! let query_type=QueryType::Intersection;
//! let result_type=ResultType::TopkCount;
//! let include_uncommitted=false;
//! let field_filter=Vec::new();
//! let query_facets=Vec::new();
//! let facet_filter=Vec::new();
//! let result_sort=Vec::new();
//! let result_object = index_arc.search(query, query_type,  enable_empty_query, offset, length, result_type,include_uncommitted,field_filter,query_facets,facet_filter,result_sort,QueryRewriting::SearchOnly).await;
//!
//! // ### display results
//!
//! use seekstorm::highlighter::{Highlight, highlighter};
//! use std::collections::HashSet;
//!
//! let highlights:Vec<Highlight>= vec![
//! Highlight {
//!     field: "body".to_string(),
//!     name:String::new(),
//!     fragment_number: 2,
//!     fragment_size: 160,
//!     highlight_markup: true,
//!     ..Default::default()
//! },
//! ];    
//! let highlighter=Some(highlighter(&index_arc,highlights, result_object.query_terms).await);
//! let return_fields_filter= HashSet::new();
//! let distance_fields=Vec::new();
//! let index=index_arc.read().await;
//! for result in result_object.results.iter() {
//!   let doc=index.get_document(result.doc_id,false,&highlighter,&return_fields_filter,&distance_fields).await.unwrap();
//!   println!("result {} rank {} body field {:?}" , result.doc_id,result.score, doc.get("body"));
//! }
//! println!("result counts {} {} {}",result_object.results.len(), result_object.result_count, result_object.result_count_total);
//! # });
//! ```
//! ### get document
//! ```no_run
//! # tokio_test::block_on(async {
//! # use std::path::Path;
//! # use seekstorm::index::open_index;
//! # let index_path=Path::new("C:/index/");
//! # let index_arc=open_index(index_path,false).await.unwrap();
//! use std::collections::HashSet;
//!
//! let doc_id=0;
//! let highlighter=None;
//! let return_fields_filter= HashSet::new();
//! let distance_fields=Vec::new();
//! let index=index_arc.read().await;
//! let doc=index.get_document(doc_id,false,&highlighter,&return_fields_filter,&distance_fields).await.unwrap();
//! # });
//! ```
//! ### index JSON file in JSON, Newline-delimited JSON and Concatenated JSON format
//! ```no_run
//! # tokio_test::block_on(async {
//! # use seekstorm::index::open_index;
//! # let index_path=Path::new("C:/index/");
//! # let mut index_arc=open_index(index_path,false).await.unwrap();
//! use seekstorm::ingest::IngestJson;
//! use std::path::Path;
//!
//! let file_path=Path::new("wiki-articles.json");
//! let _ =index_arc.ingest_json(file_path).await;
//! # });
//! ```
//! ### index all PDF files in directory and sub-directories
//! - converts pdf to text and indexes it
//! - extracts title from metatag, or first line of text, or from filename
//! - extracts creation date from metatag, or from file creation date (Unix timestamp: the number of seconds since 1 January 1970)
//! - copies all ingested pdf files to "files" subdirectory in index
//! - the following index schema is required (and automatically created by the console `ingest` command):
//! ```no_run
//! let schema_json = r#"
//! [
//!   {
//!     "field": "title",
//!     "stored": true,
//!     "indexed": true,
//!     "field_type": "Text",
//!     "boost": 10
//!   },
//!   {
//!     "field": "body",
//!     "stored": true,
//!     "indexed": true,
//!     "field_type": "Text"
//!   },
//!   {
//!     "field": "url",
//!     "stored": true,
//!     "indexed": false,
//!     "field_type": "Text"
//!   },
//!   {
//!     "field": "date",
//!     "stored": true,
//!     "indexed": false,
//!     "field_type": "Timestamp",
//!     "facet": true
//!   }
//! ]"#;
//! ```
//! ```no_run
//! # tokio_test::block_on(async {
//! # use seekstorm::index::open_index;
//! # let index_path=Path::new("C:/index/");
//! # let mut index_arc=open_index(index_path,false).await.unwrap();
//! use std::path::Path;
//! use seekstorm::ingest::IngestPdf;
//!
//! let file_path=Path::new("C:/Users/johndoe/Downloads");
//! let _ =index_arc.ingest_pdf(file_path).await;
//! # });
//! ```
//! ### index PDF file
//! ```no_run
//! # tokio_test::block_on(async {
//! # use seekstorm::index::open_index;
//! # let index_path=Path::new("C:/index/");
//! # let mut index_arc=open_index(index_path,false).await.unwrap();
//! use std::path::Path;
//! use seekstorm::ingest::IndexPdfFile;
//!
//! let file_path=Path::new("C:/test.pdf");
//! let _ =index_arc.index_pdf_file(file_path).await;
//! # });
//! ```
//! ### index PDF file bytes
//! ```no_run
//! # tokio_test::block_on(async {
//! # use seekstorm::index::open_index;
//! # let index_path=Path::new("C:/index/");
//! # let mut index_arc=open_index(index_path,false).await.unwrap();
//! use std::path::Path;
//! use std::fs;
//! use chrono::Utc;
//! use seekstorm::ingest::IndexPdfBytes;
//!
//! let file_date=Utc::now().timestamp();
//! let file_path=Path::new("C:/test.pdf");
//! let document = fs::read(file_path).unwrap();
//! let _ =index_arc.index_pdf_bytes(file_path, file_date, &document).await;
//! # });
//! ```
//! ### get PDF file bytes
//! ```no_run
//! # tokio_test::block_on(async {
//! # use seekstorm::index::open_index;
//! # use std::path::Path;
//! # let index_path=Path::new("C:/index/");
//! # let mut index_arc=open_index(index_path,false).await.unwrap();
//! let doc_id=0;
//! let _file=index_arc.read().await.get_file(doc_id).await.unwrap();
//! # });
//! ```
//! ### clear index
//! ```no_run
//! # tokio_test::block_on(async {
//! # use seekstorm::index::open_index;
//! # use std::path::Path;
//! # let index_path=Path::new("C:/index/");
//! # let mut index_arc=open_index(index_path,false).await.unwrap();
//! index_arc.write().await.clear_index().await;
//! # });
//! ```
//! ### delete index
//! ```no_run
//! # tokio_test::block_on(async {
//! # use seekstorm::index::open_index;
//! # use std::path::Path;
//! # let index_path=Path::new("C:/index/");
//! # let mut index_arc=open_index(index_path,false).await.unwrap();
//! index_arc.write().await.delete_index();
//! # });
//! ```
//! ### close index
//! ```no_run
//! # tokio_test::block_on(async {
//! # use seekstorm::index::open_index;
//! # use std::path::Path;
//! # let index_path=Path::new("C:/index/");
//! # let mut index_arc=open_index(index_path,false).await.unwrap();
//! use seekstorm::index::Close;
//!
//! index_arc.close().await;
//! # });
//! ```
//! ### seekstorm library version string
//! ```no_run
//! use seekstorm::index::version;
//!
//! let version=version();
//! println!("version {}",version);
//! ```
//!
//! ----------------
//! ### Faceted search - Quick start
//! **Facets are defined in 3 different places:**
//! 1. the facet fields are defined in schema at create_index,
//! 2. the facet field values are set in index_document at index time,
//! 3. the query_facets/facet_filter search parameters are specified at query time.
//!    Facets are then returned in the search result object.
//!
//! A minimal working example of faceted indexing & search requires just 60 lines of code. But to puzzle it all together from the documentation alone might be tedious.
//! This is why we provide a quick start example here:
//! ### create index
//! ```no_run
//! # tokio_test::block_on(async {
//! use std::path::Path;
//! use seekstorm::index::{IndexMetaObject, SimilarityType,TokenizerType,StopwordType,FrequentwordType,AccessType,StemmerType,NgramSet,DocumentCompression,create_index};
//!
//! let index_path=Path::new("C:/index/");
//! let schema_json = r#"
//! [{"field":"title","field_type":"Text","stored":false,"indexed":false},
//! {"field":"body","field_type":"Text","stored":true,"indexed":true},
//! {"field":"url","field_type":"Text","stored":true,"indexed":false},
//! {"field":"town","field_type":"String15","stored":false,"indexed":false,"facet":true}]"#;
//! let schema=serde_json::from_str(schema_json).unwrap();
//! let meta = IndexMetaObject {
//!     id: 0,
//!     name: "test_index".into(),
//!     similarity: SimilarityType::Bm25f,
//!     tokenizer: TokenizerType::AsciiAlphabetic,
//!     stemmer: StemmerType::None,
//!     stop_words: StopwordType::None,
//!     frequent_words: FrequentwordType::English,
//!     ngram_indexing: NgramSet::NgramFF as u8,
//!     document_compression:  DocumentCompression::Snappy,
//!     access_type: AccessType::Mmap,
//!     spelling_correction: None,
//!     query_completion: None,
//! };
//! let serialize_schema=true;
//! let segment_number_bits1=11;
//! let index_arc=create_index(index_path,meta,&schema,&Vec::new(),segment_number_bits1,false,None).await.unwrap();
//! # });
//! ```
//! ### index documents
//! ```no_run
//! # tokio_test::block_on(async {
//! # use std::path::Path;
//! # use seekstorm::index::open_index;
//! # let index_path=Path::new("C:/index/");
//! # let index_arc=open_index(index_path,false).await.unwrap();
//! use seekstorm::index::IndexDocuments;
//! use seekstorm::commit::Commit;
//! use seekstorm::search::{QueryType, ResultType, QueryFacet, FacetFilter};
//!
//! let documents_json = r#"
//! [{"title":"title1 test","body":"body1","url":"url1","town":"Berlin"},
//! {"title":"title2","body":"body2 test","url":"url2","town":"Warsaw"},
//! {"title":"title3 test","body":"body3 test","url":"url3","town":"New York"}]"#;
//! let documents_vec=serde_json::from_str(documents_json).unwrap();
//! index_arc.index_documents(documents_vec).await;
//!
//! // ### commit documents
//!
//! index_arc.commit().await;
//! # });
//! ```
//! ### search index
//! ```no_run
//! # tokio_test::block_on(async {
//! # use std::path::Path;
//! # use seekstorm::index::open_index;
//! # let index_path=Path::new("C:/index/");
//! # let index_arc=open_index(index_path,false).await.unwrap();
//! use seekstorm::search::{QueryType, ResultType, QueryFacet, FacetFilter, QueryRewriting,Search};
//!
//! let query="test".to_string();
//! let enable_empty_query=false;
//! let offset=0;
//! let length=10;
//! let query_type=QueryType::Intersection;
//! let result_type=ResultType::TopkCount;
//! let include_uncommitted=false;
//! let field_filter=Vec::new();
//! let query_facets = vec![QueryFacet::String16 {field: "town".to_string(),prefix: "".to_string(),length: u16::MAX}];
//! let facet_filter=Vec::new();
//! //let facet_filter = vec![FacetFilter {field: "town".to_string(),   filter:Filter::String(vec!["Berlin".to_string()])}];
//! let result_sort=Vec::new();
//! let result_object = index_arc.search(query, query_type,  enable_empty_query, offset, length, result_type,include_uncommitted,field_filter,query_facets,facet_filter,result_sort,QueryRewriting::SearchOnly).await;
//!
//! // ### display results
//!
//! use std::collections::HashSet;
//! use seekstorm::highlighter::{highlighter, Highlight};
//!
//! let highlights:Vec<Highlight>= vec![
//!         Highlight {
//!             field: "body".to_owned(),
//!             name:String::new(),
//!             fragment_number: 2,
//!             fragment_size: 160,
//!             highlight_markup: true,
//!            ..Default::default()
//!         },
//!     ];    
//! let highlighter=Some(highlighter(&index_arc,highlights, result_object.query_terms).await);
//! let return_fields_filter= HashSet::new();
//! let distance_fields=Vec::new();
//! let index=index_arc.write().await;
//! for result in result_object.results.iter() {
//!   let doc=index.get_document(result.doc_id,false,&highlighter,&return_fields_filter,&distance_fields).await.unwrap();
//!   println!("result {} rank {} body field {:?}" , result.doc_id,result.score, doc.get("body"));
//! }
//! println!("result counts {} {} {}",result_object.results.len(), result_object.result_count, result_object.result_count_total);
//!
//! // ### display facets
//!
//! println!("{}", serde_json::to_string_pretty(&result_object.facets).unwrap());
//! # });
//! ```

/// include README.md in documentation
#[cfg_attr(doctest, doc = include_str!("../../README.md"))]
pub struct ReadmeDoctests;

/// include FACETED_SEARCH.md in documentation
#[cfg_attr(doctest, doc = include_str!("../../FACETED_SEARCH.md"))]
pub struct ReadmeDoctests2;

pub(crate) mod add_result;
/// Commit moves indexed documents from the intermediate uncompressed data structure in RAM
/// to the final compressed data structure on disk.
pub mod commit;
pub(crate) mod compatible;
pub(crate) mod compress_postinglist;
pub(crate) mod doc_store;
/// Geo search by indexing geo points (latitude, longitude), proximity searching for points within a specified radius, and proximity sorting.
pub mod geo_search;
/// Extracts the most relevant fragments (snippets, summaries) from specified fields of the document to provide a "keyword in context" (KWIC) functionality.
/// With highlight_markup the matching query terms within the fragments can be highlighted with HTML markup.
pub mod highlighter;
/// Operate the index: reate_index, open_index, clear_index, close_index, delete_index, index_document(s)
pub mod index;
pub(crate) mod index_posting;
/// Ingest JSON, Newline-delimited JSON, Concatenated JSON files, and PDF files into the index.
pub mod ingest;
pub(crate) mod intersection;
pub(crate) mod intersection_simd;
/// Iterator over all documents, also for search with empty query.
pub mod iterator;
pub(crate) mod min_heap;
pub(crate) mod realtime_search;
/// Search the index for all indexed documents, both for committed and uncommitted documents.
/// The latter enables true realtime search: documents are available for search in exact the same millisecond they are indexed.
pub mod search;
pub(crate) mod single;
/// Tokenizes text into tokens (words), supports Chinese word segmentation, folds (converts) diacritics, accents, zalgo text, umlaut, bold, italic, full-width UTF-8 characters into their basic representation.
pub(crate) mod tokenizer;
pub(crate) mod union;
/// Utils `truncate()` and `substring()`
pub mod utils;
#[cfg(feature = "zh")]
pub(crate) mod word_segmentation;
