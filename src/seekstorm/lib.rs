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
//! ### Add use declarations
//! ```
//! use std::{collections::HashSet, error::Error, path::Path, sync::Arc};
//! use seekstorm::{index::*,search::*,highlighter::*,commit::Commit};
//! use tokio::sync::RwLock;
//! ```
//! ### use an asynchronous Rust runtime
//! ```text
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
//! ```
//! ### create index
//! ```rust
//! let index_path=Path::new("C:/index/");
//! let schema_json = r#"
//! [{"field":"title","field_type":"Text","stored":false,"indexed":false},
//! {"field":"body","field_type":"Text","stored":true,"indexed":true},
//! {"field":"url","field_type":"Text","stored":false,"indexed":false}]"#;
//! let schema=serde_json::from_str(schema_json).unwrap();
//! let meta = IndexMetaObject {
//! id: 0,
//! name: "test_index".to_string(),
//! similarity:SimilarityType::Bm25f,
//! tokenizer:TokenizerType::AsciiAlphabetic,
//! access_type: AccessType::Mmap,
//! };
//! let segment_number_bits1=11;
//! let serialize_schema=true;
//! let index=create_index(index_path,meta,&schema,serialize_schema,segment_number_bits1).unwrap();
//! let _index_arc = Arc::new(RwLock::new(index));
//! ```
//! ### open index (alternatively to create index)
//! ```rust
//!let index_path=Path::new("C:/index/");
//! let index_arc=open_index(index_path).await.unwrap();
//! ```
//! ### index documents
//! ```rust
//! let documents_json = r#"
//! [{"title":"title1 test","body":"body1","url":"url1"},
//! {"title":"title2","body":"body2 test","url":"url2"},
//! {"title":"title3 test","body":"body3 test","url":"url3"}]"#;
//! let documents_vec=serde_json::from_str(documents_json).unwrap();
//! index_arc.index_documents(documents_vec).await;
//! ```
//! ### delete documents by document id
//! ```rust
//! let docid_vec=vec![1,2];
//! index_arc.delete_documents(docid_vec).await;
//! ```
//! ### delete documents by query
//! ```rust
//! let query="test".to_string();
//! let offset=0;
//! let length=10;
//! let query_type=QueryType::Intersection;
//! let include_uncommitted=false;
//! let field_filter=Vec::new();
//! index_arc.delete_documents_by_query(query, query_type, offset, length, include_uncommitted,field_filter).await;
//! ```
//! ### update documents
//! ```rust
//! let id_document_vec_json = r#"
//! [[1,{"title":"title1 test","body":"body1","url":"url1"}],
//! [2,{"title":"title3 test","body":"body3 test","url":"url3"}]]"#;
//! let id_document_vec=serde_json::from_str(id_document_vec_json).unwrap();
//! index_arc.update_documents(id_document_vec).await;
//! ```
//! ### commit documents
//! ```rust
//! index_arc.commit().await;
//! ```
//! ### search index
//! ```rust
//! let query="test".to_string();
//! let offset=10;
//! let length=10;
//! let query_type=QueryType::Intersection;
//! let result_type=ResultType::TopkCount;
//! let include_uncommitted=false;
//! let field_filter=Vec::new();
//! let result_object = index_arc.search(query, query_type, offset, length, result_type,include_uncommitted,field_filter).await;
//! ```
//! ### display results
//! ```rust
//! let highlights:Vec<Highlight>= vec![
//! Highlight {
//!     field: "body".to_string(),
//!     name:String::new(),
//!     fragment_number: 2,
//!     fragment_size: 160,
//!     highlight_markup: true,
//! },
//! ];    
//! let highlighter=Some(highlighter(highlights, result_object.query_term_strings));
//! let return_fields_filter= HashSet::new();
//! let mut index=index_arc.write().await;
//! for result in result_object.results.iter() {
//!   let doc=index.get_document(result.doc_id,false,&highlighter,&return_fields_filter).unwrap();
//!   println!("result {} rank {} body field {:?}" , result.doc_id,result.score, doc.get("body"));
//! }
//! ```
//! ### clear index
//! ```rust
//! index.clear_index();
//! ```
//! ### delete index
//! ```rust
//! index.delete_index();
//! ```
//! ### close index
//! ```rust
//! index.close_index();
//! ```
//! ### seekstorm library version string
//! ```rust
//! let version=version();
//! println!("version {}",version);
//! ```
//! ### end of main function
//! ```text
//!    Ok(())
//! }
//! ```
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
//! ### Add required crates to your project
//! ```text
//! cargo add seekstorm
//! cargo add tokio
//! cargo add serde_json
//! ```
//! ### Add use declarations
//! ```
//! use std::{collections::HashSet, error::Error, path::Path, sync::Arc};
//! use seekstorm::{index::*,search::*,highlighter::*,commit::Commit};
//! use tokio::sync::RwLock;
//! ```
//! ### use an asynchronous Rust runtime
//! ```text
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
//! ```
//! ### create index
//! ```rust
//! let index_path=Path::new("C:/index/");//x
//! let schema_json = r#"
//! [{"field":"title","field_type":"Text","stored":false,"indexed":false},
//! {"field":"body","field_type":"Text","stored":true,"indexed":true},
//! {"field":"url","field_type":"Text","stored":true,"indexed":false},
//! {"field":"town","field_type":"String","stored":false,"indexed":false,"facet":true}]"#;
//! let schema=serde_json::from_str(schema_json).unwrap();
//! let meta = IndexMetaObject {
//!     id: 0,
//!     name: "test_index".to_string(),
//!     similarity:SimilarityType::Bm25f,
//!     tokenizer:TokenizerType::AsciiAlphabetic,
//!     access_type: AccessType::Mmap,
//! };
//! let serialize_schema=true;
//! let segment_number_bits1=11;
//! let index=create_index(index_path,meta,&schema,serialize_schema,segment_number_bits1,false).unwrap();
//! let mut index_arc = Arc::new(RwLock::new(index));
//! ```
//! ### index documents
//! ```rust
//! let documents_json = r#"
//! [{"title":"title1 test","body":"body1","url":"url1","town":"Berlin"},
//! {"title":"title2","body":"body2 test","url":"url2","town":"Warsaw"},
//! {"title":"title3 test","body":"body3 test","url":"url3","town":"New York"}]"#;
//! let documents_vec=serde_json::from_str(documents_json).unwrap();
//! index_arc.index_documents(documents_vec).await;
//! ```
//! ### commit documents
//! ```rust
//! index_arc.commit().await;
//! ```
//! ### search index
//! ```rust
//! let query="test".to_string();
//! let offset=0;
//! let length=10;
//! let query_type=QueryType::Intersection;
//! let result_type=ResultType::TopkCount;
//! let include_uncommitted=false;
//! let field_filter=Vec::new();
//! let query_facets = vec![QueryFacet::String {field: "town".to_string(),prefix: "".to_string(),length: u16::MAX}];
//! let facet_filter=Vec::new();
//! //let facet_filter = vec![FacetFilter {field: "town".to_string(),   filter:Filter::String(vec!["Berlin".to_string()])}];
//! let result_object = index_arc.search(query, query_type, offset, length, result_type,include_uncommitted,field_filter,query_facets,facet_filter).await;
//! ```
//! ### display results
//! ```rust
//! let highlights:Vec<Highlight>= vec![
//!         Highlight {
//!             field: "body".to_owned(),
//!             name:String::new(),
//!             fragment_number: 2,
//!             fragment_size: 160,
//!             highlight_markup: true,
//!         },
//!     ];    
//! let highlighter2=Some(highlighter(highlights, result_object.query_terms));
//! let return_fields_filter= HashSet::new();
//! let index=index_arc.write().await;
//! for result in result_object.results.iter() {
//!   let doc=index.get_document(result.doc_id,false,&highlighter2,&return_fields_filter).unwrap();
//!   println!("result {} rank {} body field {:?}" , result.doc_id,result.score, doc.get("body"));
//! }
//! ```
//! ### display facets
//! ```rust
//! println!("{}", serde_json::to_string_pretty(&result_object.facets).unwrap());
//! ```
//! ### end of main function
//! ```text
//!    Ok(())
//! }
//! ```

pub(crate) mod add_result;
/// Commit moves indexed documents from the intermediate uncompressed data structure in RAM
/// to the final compressed data structure on disk.
pub mod commit;
pub(crate) mod compatible;
pub(crate) mod compress_postinglist;
pub(crate) mod doc_store;
/// Extracts the most relevant fragments (snippets, summaries) from specified fields of the document to provide a "keyword in context" (KWIC) functionality.
/// With highlight_markup the matching query terms within the fragments can be highlighted with HTML markup.
pub mod highlighter;
/// Operate the index: reate_index, open_index, clear_index, close_index, delete_index, index_document(s)
pub mod index;
pub(crate) mod index_posting;
pub(crate) mod intersection;
pub(crate) mod intersection_simd;
pub(crate) mod min_heap;
pub(crate) mod realtime_search;
/// Search the index for all indexed documents, both for committed and uncommitted documents.
/// The latter enables true realtime search: documents are available for search in exact the same millisecond they are indexed.
pub mod search;
pub(crate) mod single;
pub(crate) mod tokenizer;
pub(crate) mod union;
pub(crate) mod utils;
