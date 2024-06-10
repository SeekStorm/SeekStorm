// This crate is a library
#![crate_type = "lib"]
// The library is named "seekstorm"
#![crate_name = "seekstorm"]
#![doc(html_logo_url = "http://seekstorm.com/assets/logo.svg")]
#![doc(html_favicon_url = "http://seekstorm.com/favicon.ico")]

//! # `seekstorm`
//!
//! SeekStorm is an open-source, sub-millisecond full-text search library & multi-tenancy server written in Rust.
//! The **SeekStorm library** can be embedded into your program, while the **SeekStorm server** is a standalone search server to be accessed via HTTP.
//!
//!
//! ### Add required crates to your project
//! ```text
//! cargo add seekstorm
//! cargo add tokio
//! cargo add serde_json
//! ```
//!
//! ```
//! use std::{collections::HashSet, error::Error, path::Path, sync::Arc};
//! use seekstorm::{index::*,search::*,highlighter::*};
//! use tokio::sync::RwLock;
//! ```
//!
//! ### use an asynchronous Rust runtime
//! ```text
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
//! ```
//!
//! ### create index
//! ```rust
//! let index_path=Path::new("C:/index/");
//!
//! let schema_json = r#"
//! [{"field_name":"title","field_type":"Text","field_stored":false,"field_indexed":false},
//! {"field_name":"body","field_type":"Text","field_stored":true,"field_indexed":true},
//! {"field_name":"url","field_type":"Text","field_stored":false,"field_indexed":false}]"#;
//! let schema=serde_json::from_str(schema_json).unwrap();
//!
//! let meta = IndexMetaObject {
//! id: 0,
//! name: "test_index".to_string(),
//! similarity:SimilarityType::Bm25f,
//! tokenizer:TokenizerType::AsciiAlphabetic,
//! access_type: AccessType::Mmap,
//! };
//!
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
//!
//! index_arc.index_documents(documents_vec).await;
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
//! let result_list = index_arc.search(query, query_type, offset, length, result_type,include_uncommitted,field_filter).await;
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
//!
//! let highlighter=Some(highlighter(highlights, result_list.query_term_strings));
//! let fields_hashset= HashSet::new();
//! let mut index=index_arc.write().await;
//! for result in result_list.results.iter() {
//!   let doc=index.get_document(result.doc_id,false,&highlighter,&fields_hashset).unwrap();
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

pub(crate) mod add_result;
pub(crate) mod commit;
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
