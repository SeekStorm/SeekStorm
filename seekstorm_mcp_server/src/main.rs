//! SeekStorm MCP server (embedded mode): links the seekstorm library directly and owns a local index.
//! Exposes indexing and search tools over the Model Context Protocol via stdio transport.

use std::collections::HashSet;
use std::env;
use std::fmt;
use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use rmcp::{
    ErrorData as McpError, ServerHandler, ServiceExt,
    handler::server::wrapper::Parameters,
    model::{CallToolResult, ContentBlock, ServerCapabilities, ServerConfig},
    schemars, tool, tool_handler, tool_router,
    transport::stdio,
};
use seekstorm::{
    commit::Commit,
    highlighter::{Highlight, highlighter},
    index::{
        AccessType, Clustering, CreateIndexRequest, DeleteDocuments, DeleteDocumentsByQuery,
        Document, DocumentCompression, FileType, FrequentwordType, IndexArc, IndexDocument,
        IndexDocuments, IndexMetaObject, LexicalSimilarity, NgramSet, SchemaField, StemmerType,
        StopwordType, TokenizerType, UpdateDocument, UpdateDocuments,
        create_index as create_index_library, open_index as open_index_library,
    },
    ingest::IndexPdfFile,
    iterator::GetIterator,
    search::{FacetFilter, QueryRewriting, QueryType, ResultType, Search, SearchMode},
    vector::{Embedding, Inference},
    vector_similarity::AnnMode,
};
use serde::{
    Deserialize, Deserializer,
    de::{self, DeserializeOwned, DeserializeSeed, SeqAccess, Visitor},
};
use serde_json::{Value, json};
use tokio::sync::RwLock as TokioRwLock;

/// Default schema for the embedded index: suited to indexing local docs (title, body, source path).
fn default_schema() -> Vec<SchemaField> {
    let schema_json = r#"
    [{"field":"title","store":true,"index_lexical":true,"field_type":"Text","boost":10},
     {"field":"body","store":true,"index_lexical":true,"field_type":"Text","longest":true},
     {"field":"path","store":true,"index_lexical":false,"field_type":"Text"}]"#;
    serde_json::from_str(schema_json).expect("valid default schema")
}

fn index_meta(
    request: CreateIndexRequest,
) -> (
    IndexMetaObject,
    Vec<SchemaField>,
    Vec<seekstorm::index::Synonym>,
) {
    let meta = IndexMetaObject {
        id: 0,
        name: request.index_name,
        lexical_similarity: request.similarity,
        tokenizer: request.tokenizer,
        stemmer: request.stemmer,
        stop_words: request.stop_words,
        frequent_words: request.frequent_words,
        ngram_indexing: request.ngram_indexing,
        document_compression: request.document_compression,
        access_type: AccessType::Mmap,
        spelling_correction: request.spelling_correction,
        query_completion: request.query_completion,
        clustering: request.clustering,
        inference: request.inference,
    };
    (meta, request.schema, request.synonyms)
}

/// Opens the index at `index_path`, creating it from a REST-compatible config file or defaults.
async fn open_or_create_index(index_path: &Path) -> Result<IndexArc, String> {
    if index_path.join("index.bin").exists() {
        open_index_library(index_path).await
    } else {
        let request = match env::var("SEEKSTORM_INDEX_CONFIG") {
            Ok(config_path) => {
                let config = std::fs::read_to_string(config_path).map_err(|e| e.to_string())?;
                serde_json::from_str::<CreateIndexRequest>(&config).map_err(|e| e.to_string())?
            }
            Err(_) => CreateIndexRequest {
                index_name: "seekstorm_mcp_index".to_string(),
                schema: default_schema(),
                similarity: LexicalSimilarity::Bm25f,
                tokenizer: TokenizerType::UnicodeAlphanumericFolded,
                stemmer: StemmerType::English,
                stop_words: StopwordType::None,
                frequent_words: FrequentwordType::English,
                ngram_indexing: NgramSet::NgramFF as u8 | NgramSet::NgramFFF as u8,
                document_compression: DocumentCompression::Zstd,
                synonyms: Vec::new(),
                spelling_correction: None,
                query_completion: None,
                clustering: Clustering::None,
                inference: Inference::None,
            },
        };
        let (meta, schema, synonyms) = index_meta(request);
        create_index_library(index_path, meta, &schema, &synonyms, 11, true, None).await
    }
}

/// Parameters for indexing one arbitrary SeekStorm document.
#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct IndexDocumentParams {
    /// JSON object whose fields must match the active index schema
    document: Value,
}

impl IndexDocumentParams {
    fn into_document(self) -> Result<Document, String> {
        if !self.document.is_object() {
            return Err("document must be a JSON object".to_string());
        }
        serde_json::from_value(self.document).map_err(|e| e.to_string())
    }
}

/// Parameters for indexing multiple documents in bulk.
#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct IndexDocumentsParams {
    /// Arbitrary JSON documents matching the active index schema
    documents: Vec<Value>,
}

/// Parameters for searching the index.
#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct SearchParams {
    /// The search query string
    query: String,
    /// Number of search results to return (default 10)
    #[serde(default)]
    length: Option<u32>,
    /// Number of results to skip, for pagination (default 0)
    #[serde(default)]
    offset: Option<u32>,
    /// Search mode: Lexical, Vector, or Hybrid
    #[serde(default)]
    mode: Option<String>,
    /// Optional query vector. If omitted, a configured index inference model may generate it.
    #[serde(default)]
    query_vector: Option<Vec<f32>>,
    /// Optional quantized signed 8-bit query vector; set this or query_vector, not both
    #[serde(default)]
    query_vector_i8: Option<Vec<i8>>,
    /// ANN cluster-selection strategy, such as "All" or {"Nprobe": 8}
    #[serde(default)]
    ann_mode: Option<Value>,
    /// Minimum vector similarity threshold
    #[serde(default)]
    similarity_threshold: Option<f32>,
    /// Include uncommitted documents in search
    #[serde(default)]
    realtime: Option<bool>,
    /// Enable empty-query browsing
    #[serde(default)]
    enable_empty_query: Option<bool>,
    /// Default query operator; overrides operators parsed from the query string only as a default
    #[serde(default)]
    query_type: Option<Value>,
    /// ResultType enum value: Count, Topk, or TopkCount
    #[serde(default)]
    result_type: Option<Value>,
    /// QueryRewriting enum value, including correction/completion settings
    #[serde(default)]
    query_rewriting: Option<Value>,
    /// Search only these indexed fields; empty means all indexed fields
    #[serde(default)]
    field_filter: Vec<String>,
    /// Return only these stored fields; empty means all stored fields
    #[serde(default)]
    fields: Vec<String>,
    /// Facet definitions, filters, sort rules, and highlighting in SeekStorm's serialized formats
    #[serde(default)]
    query_facets: Option<Value>,
    #[serde(default)]
    facet_filter: Option<Value>,
    #[serde(default)]
    result_sort: Option<Value>,
    #[serde(default)]
    highlights: Option<Value>,
}

fn parse_json_or_default<T: DeserializeOwned + Default>(value: Option<Value>) -> Result<T, String> {
    match value {
        Some(value) => serde_json::from_value(value).map_err(|e| e.to_string()),
        None => Ok(T::default()),
    }
}

struct DocumentArraySeed(tokio::sync::mpsc::Sender<Document>);

impl<'de> DeserializeSeed<'de> for DocumentArraySeed {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(DocumentArrayVisitor(self.0))
    }
}

struct DocumentArrayVisitor(tokio::sync::mpsc::Sender<Document>);

impl<'de> Visitor<'de> for DocumentArrayVisitor {
    type Value = ();

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("an array of SeekStorm documents")
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        while let Some(document) = sequence.next_element::<Document>()? {
            self.0.blocking_send(document).map_err(de::Error::custom)?;
        }
        Ok(())
    }
}

async fn index_json_file(index_arc: IndexArc, path: PathBuf) -> Result<usize, String> {
    let (sender, mut receiver) = tokio::sync::mpsc::channel(32);
    let parser = tokio::task::spawn_blocking(move || -> Result<(), String> {
        let file = File::open(&path).map_err(|err| err.to_string())?;
        let mut reader = BufReader::new(file);
        let mut first = [0u8; 1];
        loop {
            if reader.read(&mut first).map_err(|err| err.to_string())? == 0 {
                return Ok(());
            }
            if !first[0].is_ascii_whitespace() {
                break;
            }
        }
        reader
            .seek(SeekFrom::Current(-1))
            .map_err(|err| err.to_string())?;

        if first[0] == b'[' {
            let mut deserializer = serde_json::Deserializer::from_reader(reader);
            DocumentArraySeed(sender)
                .deserialize(&mut deserializer)
                .map_err(|err| err.to_string())?;
            deserializer.end().map_err(|err| err.to_string())?;
        } else {
            for document in serde_json::Deserializer::from_reader(reader).into_iter::<Document>() {
                sender
                    .blocking_send(document.map_err(|err| err.to_string())?)
                    .map_err(|err| err.to_string())?;
            }
        }
        Ok(())
    });

    let mut count = 0;
    while let Some(document) = receiver.recv().await {
        index_arc.index_document(document, FileType::None).await;
        count += 1;
    }
    let parse_result = parser.await.map_err(|err| err.to_string())?;
    parse_result?;
    index_arc.commit().await;
    Ok(count)
}

/// Parameters identifying a single document by ID.
#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct DocIdParams {
    /// Document ID, as returned by search results
    doc_id: u64,
}

/// Parameters identifying multiple documents by ID.
#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct DocIdsParams {
    /// Document IDs, as returned by search results
    doc_ids: Vec<u64>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct IndexConfigParams {
    /// Local directory where this index will be stored
    index_path: String,
    /// REST-compatible CreateIndexRequest JSON, including schema, inference, clustering, synonyms, and lexical settings
    config: Value,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct OpenIndexParams {
    /// Local directory containing a SeekStorm index
    index_path: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct UpdateDocumentParams {
    /// Existing document ID
    doc_id: u64,
    /// Replacement document JSON matching the active index schema
    document: Value,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct UpdateDocumentsParams {
    /// Documents to replace, each with an existing document ID and replacement JSON
    documents: Vec<UpdateDocumentParams>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct IteratorParams {
    /// Starting document ID, or null to start from the beginning/end
    #[serde(default)]
    document_id: Option<u64>,
    /// Number of document IDs to skip
    #[serde(default)]
    skip: usize,
    /// Number and direction of IDs to return; negative values iterate backwards
    take: isize,
    /// Include deleted IDs
    #[serde(default)]
    include_deleted: bool,
    /// Include stored document contents
    #[serde(default)]
    include_document: bool,
    /// Stored fields to include, empty means all
    #[serde(default)]
    fields: Vec<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct PathParams {
    /// Local file or directory path
    path: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct GetDocumentParams {
    /// Document ID, as returned by search or iteration
    doc_id: u64,
    /// Stored fields to return, empty means all
    #[serde(default)]
    fields: Vec<String>,
    /// Search terms to highlight
    #[serde(default)]
    query_terms: Vec<String>,
    /// SeekStorm Highlight objects
    #[serde(default)]
    highlights: Option<Value>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct DeleteByQueryParams {
    /// Lexical query whose matching documents will be deleted
    query: String,
    /// Default query operator serialized as a SeekStorm QueryType; defaults to Intersection
    #[serde(default)]
    query_type: Option<Value>,
    /// Maximum number of matching documents to delete
    #[serde(default)]
    length: Option<usize>,
    /// Number of matches to skip
    #[serde(default)]
    offset: Option<usize>,
    /// Search only these indexed fields
    #[serde(default)]
    field_filter: Vec<String>,
    /// SeekStorm-serialized facet filters
    #[serde(default)]
    facet_filter: Option<Value>,
    /// SeekStorm-serialized sort definitions
    #[serde(default)]
    result_sort: Option<Value>,
}

/// Embedded SeekStorm MCP server: owns a local index and exposes indexing/search tools.
#[derive(Clone)]
struct SeekStormMcpServer {
    index_arc: Arc<TokioRwLock<IndexArc>>,
    index_path: Arc<TokioRwLock<PathBuf>>,
    #[allow(dead_code)]
    tool_router: rmcp::handler::server::router::tool::ToolRouter<Self>,
}

#[tool_router]
impl SeekStormMcpServer {
    fn new(index_arc: IndexArc, index_path: PathBuf) -> Self {
        Self {
            index_arc: Arc::new(TokioRwLock::new(index_arc)),
            index_path: Arc::new(TokioRwLock::new(index_path)),
            tool_router: Self::tool_router(),
        }
    }

    async fn active_index(&self) -> IndexArc {
        self.index_arc.read().await.clone()
    }

    #[tool(description = "Index one arbitrary JSON document matching the active index schema.")]
    async fn index_document(
        &self,
        Parameters(params): Parameters<IndexDocumentParams>,
    ) -> Result<CallToolResult, McpError> {
        let index_arc = self.active_index().await;
        let document = match params.into_document() {
            Ok(document) => document,
            Err(err) => return Ok(CallToolResult::error(vec![ContentBlock::text(err)])),
        };
        index_arc.index_document(document, FileType::None).await;
        let count = index_arc.read().await.indexed_doc_count().await;
        Ok(CallToolResult::success(vec![ContentBlock::text(format!(
            "Document indexed. Total indexed documents: {count}"
        ))]))
    }

    #[tool(description = "Index arbitrary JSON documents matching the active index schema.")]
    async fn index_documents(
        &self,
        Parameters(params): Parameters<IndexDocumentsParams>,
    ) -> Result<CallToolResult, McpError> {
        let index_arc = self.active_index().await;
        let document_vec: Result<Vec<Document>, _> = params
            .documents
            .into_iter()
            .map(serde_json::from_value)
            .collect();
        let document_vec = match document_vec {
            Ok(documents) => documents,
            Err(err) => {
                return Ok(CallToolResult::error(vec![ContentBlock::text(
                    err.to_string(),
                )]));
            }
        };
        index_arc.index_documents(document_vec).await;
        let count = index_arc.read().await.indexed_doc_count().await;
        Ok(CallToolResult::success(vec![ContentBlock::text(format!(
            "Documents indexed. Total indexed documents: {count}"
        ))]))
    }

    #[tool(
        description = "Search the local index using lexical, vector, or hybrid retrieval with filters, facets, sorting, highlighting, and pagination."
    )]
    async fn search(
        &self,
        Parameters(params): Parameters<SearchParams>,
    ) -> Result<CallToolResult, McpError> {
        let index_arc = self.active_index().await;
        let offset = params.offset.unwrap_or(0) as usize;
        let length = params.length.unwrap_or(10) as usize;
        if length == 0 {
            return Ok(CallToolResult::error(vec![ContentBlock::text(
                "length must be greater than zero",
            )]));
        }

        let parsed = (|| -> Result<_, String> {
            let query_type: QueryType = match params.query_type.clone() {
                Some(value) => serde_json::from_value(value).map_err(|err| err.to_string())?,
                None => QueryType::Intersection,
            };
            let result_type: ResultType = parse_json_or_default(params.result_type.clone())?;
            let query_rewriting: QueryRewriting =
                parse_json_or_default(params.query_rewriting.clone())?;
            let query_facets = parse_json_or_default(params.query_facets.clone())?;
            let facet_filter = parse_json_or_default(params.facet_filter.clone())?;
            let result_sort = parse_json_or_default(params.result_sort.clone())?;
            let highlights: Vec<Highlight> = parse_json_or_default(params.highlights.clone())?;
            let ann_mode: AnnMode = parse_json_or_default(params.ann_mode.clone())?;
            let threshold = params.similarity_threshold;
            let search_mode = match params
                .mode
                .as_deref()
                .unwrap_or("Lexical")
                .to_ascii_lowercase()
                .as_str()
            {
                "lexical" => SearchMode::Lexical,
                "vector" => SearchMode::Vector {
                    similarity_threshold: threshold,
                    ann_mode,
                },
                "hybrid" => SearchMode::Hybrid {
                    similarity_threshold: threshold,
                    ann_mode,
                },
                other => return Err(format!("unsupported search mode: {other}")),
            };
            if params.query_vector.is_some() && params.query_vector_i8.is_some() {
                return Err("set only one of query_vector or query_vector_i8".to_string());
            }
            let query_vector = params
                .query_vector
                .map(Embedding::F32)
                .or_else(|| params.query_vector_i8.map(Embedding::I8));
            Ok((
                query_type,
                result_type,
                query_rewriting,
                query_facets,
                facet_filter,
                result_sort,
                highlights,
                search_mode,
                query_vector,
            ))
        })();
        let (
            query_type,
            result_type,
            query_rewriting,
            query_facets,
            facet_filter,
            result_sort,
            highlights,
            search_mode,
            query_vector,
        ) = match parsed {
            Ok(parsed) => parsed,
            Err(err) => return Ok(CallToolResult::error(vec![ContentBlock::text(err)])),
        };

        if query_vector.is_none()
            && matches!(
                &search_mode,
                SearchMode::Vector { .. } | SearchMode::Hybrid { .. }
            )
        {
            let index = index_arc.read().await;
            if matches!(
                &index.meta.inference,
                Inference::None | Inference::External { .. }
            ) {
                return Ok(CallToolResult::error(vec![ContentBlock::text(
                    "vector and hybrid search need query_vector/query_vector_i8 unless the index has an internal inference model configured",
                )]));
            }
        }
        if let Some(query_vector) = &query_vector
            && matches!(
                &search_mode,
                SearchMode::Vector { .. } | SearchMode::Hybrid { .. }
            )
        {
            let expected_dimensions = index_arc.read().await.vector_dimensions;
            let actual_dimensions = match query_vector {
                Embedding::F32(values) => values.len(),
                Embedding::I8(values) => values.len(),
            };
            if actual_dimensions != expected_dimensions {
                return Ok(CallToolResult::error(vec![ContentBlock::text(format!(
                    "query vector has {actual_dimensions} dimensions; active index expects {expected_dimensions}"
                ))]));
            }
        }

        let result_object = index_arc
            .search(
                params.query,
                query_vector,
                query_type,
                search_mode,
                params.enable_empty_query.unwrap_or(false),
                offset,
                length,
                result_type,
                params.realtime.unwrap_or(false),
                params.field_filter,
                query_facets,
                facet_filter,
                result_sort,
                query_rewriting,
            )
            .await;

        let highlighter_option = if highlights.is_empty() || result_object.query_terms.is_empty() {
            None
        } else {
            Some(highlighter(&index_arc, highlights, result_object.query_terms.clone()).await)
        };
        let return_fields = HashSet::from_iter(params.fields);
        let index = index_arc.read().await;
        let mut results = Vec::new();
        for result in result_object.results.iter() {
            let doc = index
                .get_document(
                    result.doc_id,
                    params.realtime.unwrap_or(false),
                    &highlighter_option,
                    &return_fields,
                    &Vec::new(),
                )
                .await
                .unwrap_or_default();
            let mut doc_value = serde_json::to_value(doc).unwrap_or_else(|_| json!({}));
            if let Some(doc_object) = doc_value.as_object_mut() {
                doc_object.insert("_id".to_string(), json!(result.doc_id));
                doc_object.insert("_score".to_string(), json!(result.score));
            }
            results.push(doc_value);
        }

        let response = json!({
            "query": result_object.query,
            "original_query": result_object.original_query,
            "query_terms": result_object.query_terms,
            "results": results,
            "result_count": result_object.result_count,
            "result_count_total": result_object.result_count_total,
            "facets": result_object.facets,
            "suggestions": result_object.suggestions,
        });
        Ok(CallToolResult::success(vec![ContentBlock::text(
            serde_json::to_string_pretty(&response).unwrap_or_default(),
        )]))
    }

    #[tool(description = "Delete a document from the index by its document ID.")]
    async fn delete_document(
        &self,
        Parameters(params): Parameters<DocIdParams>,
    ) -> Result<CallToolResult, McpError> {
        let index_arc = self.active_index().await;
        index_arc.delete_documents(vec![params.doc_id]).await;
        Ok(CallToolResult::success(vec![ContentBlock::text(format!(
            "Document {} deleted",
            params.doc_id
        ))]))
    }

    #[tool(description = "Delete multiple documents from the index by their document IDs.")]
    async fn delete_documents(
        &self,
        Parameters(params): Parameters<DocIdsParams>,
    ) -> Result<CallToolResult, McpError> {
        let index_arc = self.active_index().await;
        let count = params.doc_ids.len();
        index_arc.delete_documents(params.doc_ids).await;
        Ok(CallToolResult::success(vec![ContentBlock::text(format!(
            "{count} document(s) deleted"
        ))]))
    }

    #[tool(
        description = "Commit the index to disk, persisting all indexed documents since the last commit."
    )]
    async fn commit(&self) -> Result<CallToolResult, McpError> {
        self.active_index().await.commit().await;
        Ok(CallToolResult::success(vec![ContentBlock::text(
            "Index committed",
        )]))
    }

    #[tool(
        description = "Get information about the local index: name, indexed/committed document counts."
    )]
    async fn get_index_info(&self) -> Result<CallToolResult, McpError> {
        let index_arc = self.active_index().await;
        let index_path = self.index_path.read().await.clone();
        let index = index_arc.read().await;
        let response = json!({
            "id": index.meta.id,
            "name": index.meta.name,
            "path": index_path.display().to_string(),
            "schema": index.schema_map,
            "indexed_doc_count": index.indexed_doc_count().await,
            "committed_doc_count": index.committed_doc_count().await,
        });
        Ok(CallToolResult::success(vec![ContentBlock::text(
            serde_json::to_string_pretty(&response).unwrap_or_default(),
        )]))
    }

    #[tool(
        description = "Create and activate a local SeekStorm index from a REST-compatible CreateIndexRequest JSON config, including arbitrary schema and vector settings."
    )]
    async fn create_index(
        &self,
        Parameters(params): Parameters<IndexConfigParams>,
    ) -> Result<CallToolResult, McpError> {
        let request: CreateIndexRequest = match serde_json::from_value(params.config) {
            Ok(request) => request,
            Err(err) => {
                return Ok(CallToolResult::error(vec![ContentBlock::text(
                    err.to_string(),
                )]));
            }
        };
        let index_path = PathBuf::from(params.index_path);
        if index_path.join("index.bin").exists() {
            return Ok(CallToolResult::error(vec![ContentBlock::text(
                "index_path already contains an index; choose another directory or open the existing index",
            )]));
        }
        let (meta, schema, synonyms) = index_meta(request);
        let new_index =
            match create_index_library(&index_path, meta, &schema, &synonyms, 11, true, None).await
            {
                Ok(index) => index,
                Err(err) => return Ok(CallToolResult::error(vec![ContentBlock::text(err)])),
            };
        self.active_index().await.commit().await;
        *self.index_arc.write().await = new_index;
        *self.index_path.write().await = index_path.clone();
        Ok(CallToolResult::success(vec![ContentBlock::text(format!(
            "Created and activated index at {}",
            index_path.display()
        ))]))
    }

    #[tool(
        description = "Open and activate an existing local SeekStorm index, committing the current index first."
    )]
    async fn open_index(
        &self,
        Parameters(params): Parameters<OpenIndexParams>,
    ) -> Result<CallToolResult, McpError> {
        let index_path = PathBuf::from(params.index_path);
        let new_index = match open_index_library(&index_path).await {
            Ok(index) => index,
            Err(err) => return Ok(CallToolResult::error(vec![ContentBlock::text(err)])),
        };
        self.active_index().await.commit().await;
        *self.index_arc.write().await = new_index;
        *self.index_path.write().await = index_path.clone();
        Ok(CallToolResult::success(vec![ContentBlock::text(format!(
            "Opened and activated index at {}",
            index_path.display()
        ))]))
    }

    #[tool(
        description = "Replace one indexed document by ID with arbitrary JSON matching the active index schema."
    )]
    async fn update_document(
        &self,
        Parameters(params): Parameters<UpdateDocumentParams>,
    ) -> Result<CallToolResult, McpError> {
        let document: Document = match serde_json::from_value(params.document) {
            Ok(document) => document,
            Err(err) => {
                return Ok(CallToolResult::error(vec![ContentBlock::text(
                    err.to_string(),
                )]));
            }
        };
        self.active_index()
            .await
            .update_document((params.doc_id, document))
            .await;
        Ok(CallToolResult::success(vec![ContentBlock::text(format!(
            "Document {} updated",
            params.doc_id
        ))]))
    }

    #[tool(
        description = "Replace multiple indexed documents by ID with arbitrary JSON matching the active index schema."
    )]
    async fn update_documents(
        &self,
        Parameters(params): Parameters<UpdateDocumentsParams>,
    ) -> Result<CallToolResult, McpError> {
        let documents: Result<Vec<(u64, Document)>, _> = params
            .documents
            .into_iter()
            .map(|item| {
                serde_json::from_value(item.document).map(|document| (item.doc_id, document))
            })
            .collect();
        let documents = match documents {
            Ok(documents) => documents,
            Err(err) => {
                return Ok(CallToolResult::error(vec![ContentBlock::text(
                    err.to_string(),
                )]));
            }
        };
        let count = documents.len();
        self.active_index().await.update_documents(documents).await;
        Ok(CallToolResult::success(vec![ContentBlock::text(format!(
            "Updated {count} document(s)"
        ))]))
    }

    #[tool(
        description = "Get a document by ID with optional stored-field selection and keyword highlighting."
    )]
    async fn get_document(
        &self,
        Parameters(params): Parameters<GetDocumentParams>,
    ) -> Result<CallToolResult, McpError> {
        let highlights: Vec<Highlight> = match parse_json_or_default(params.highlights) {
            Ok(highlights) => highlights,
            Err(err) => return Ok(CallToolResult::error(vec![ContentBlock::text(err)])),
        };
        let index_arc = self.active_index().await;
        let highlighter_option = if highlights.is_empty() || params.query_terms.is_empty() {
            None
        } else {
            Some(highlighter(&index_arc, highlights, params.query_terms).await)
        };
        let index = index_arc.read().await;
        match index
            .get_document(
                params.doc_id as usize,
                true,
                &highlighter_option,
                &HashSet::from_iter(params.fields),
                &Vec::new(),
            )
            .await
        {
            Ok(document) => Ok(CallToolResult::success(vec![ContentBlock::text(
                serde_json::to_string_pretty(&document).unwrap_or_default(),
            )])),
            Err(err) => Ok(CallToolResult::error(vec![ContentBlock::text(err)])),
        }
    }

    #[tool(
        description = "Iterate through valid document IDs, optionally returning stored documents. Signed take controls forward/backward iteration."
    )]
    async fn document_iterator(
        &self,
        Parameters(params): Parameters<IteratorParams>,
    ) -> Result<CallToolResult, McpError> {
        if params.take == 0 {
            return Ok(CallToolResult::error(vec![ContentBlock::text(
                "take must not be zero",
            )]));
        }
        let result = self
            .active_index()
            .await
            .get_iterator(
                params.document_id,
                params.skip,
                params.take,
                params.include_deleted,
                params.include_document,
                params.fields,
            )
            .await;
        Ok(CallToolResult::success(vec![ContentBlock::text(
            serde_json::to_string_pretty(&result).unwrap_or_default(),
        )]))
    }

    #[tool(
        description = "Stream and index a local JSON, NDJSON, or concatenated-JSON file into the active index."
    )]
    async fn ingest_json(
        &self,
        Parameters(params): Parameters<PathParams>,
    ) -> Result<CallToolResult, McpError> {
        let path = PathBuf::from(params.path);
        if !path.is_file() {
            return Ok(CallToolResult::error(vec![ContentBlock::text(format!(
                "file does not exist: {}",
                path.display()
            ))]));
        }
        match index_json_file(self.active_index().await, path.clone()).await {
            Ok(count) => Ok(CallToolResult::success(vec![ContentBlock::text(format!(
                "Indexed {count} document(s) from {}",
                path.display(),
            ))])),
            Err(err) => Ok(CallToolResult::error(vec![ContentBlock::text(err)])),
        }
    }

    #[tool(
        description = "Extract and index a local PDF file. Requires the SeekStorm pdf feature and an available Pdfium library."
    )]
    async fn index_pdf_file(
        &self,
        Parameters(params): Parameters<PathParams>,
    ) -> Result<CallToolResult, McpError> {
        let path = PathBuf::from(params.path);
        match self.active_index().await.index_pdf_file(&path).await {
            Ok(()) => Ok(CallToolResult::success(vec![ContentBlock::text(format!(
                "Indexed PDF {}",
                path.display()
            ))])),
            Err(err) => Ok(CallToolResult::error(vec![ContentBlock::text(err)])),
        }
    }

    #[tool(
        description = "Retrieve source-file bytes associated with a document ID, returned as base64 text."
    )]
    async fn get_file(
        &self,
        Parameters(params): Parameters<DocIdParams>,
    ) -> Result<CallToolResult, McpError> {
        use base64::Engine;
        match self
            .active_index()
            .await
            .read()
            .await
            .get_file(params.doc_id as usize)
            .await
        {
            Ok(bytes) => Ok(CallToolResult::success(vec![ContentBlock::text(
                base64::engine::general_purpose::STANDARD.encode(bytes),
            )])),
            Err(err) => Ok(CallToolResult::error(vec![ContentBlock::text(err)])),
        }
    }

    #[tool(
        description = "Delete documents matching a lexical query. The length bounds the number deleted; search first to verify the intended matches."
    )]
    async fn delete_documents_by_query(
        &self,
        Parameters(params): Parameters<DeleteByQueryParams>,
    ) -> Result<CallToolResult, McpError> {
        let parsed = (|| -> Result<_, String> {
            let query_type: QueryType = parse_json_or_default(params.query_type.clone())?;
            let facet_filter: Vec<FacetFilter> =
                parse_json_or_default(params.facet_filter.clone())?;
            let result_sort = parse_json_or_default(params.result_sort.clone())?;
            Ok((query_type, facet_filter, result_sort))
        })();
        let (query_type, facet_filter, result_sort) = match parsed {
            Ok(parsed) => parsed,
            Err(err) => return Ok(CallToolResult::error(vec![ContentBlock::text(err)])),
        };
        let length = params.length.unwrap_or(100);
        if length == 0 {
            return Ok(CallToolResult::error(vec![ContentBlock::text(
                "length must be greater than zero",
            )]));
        }
        self.active_index()
            .await
            .delete_documents_by_query(
                params.query,
                query_type,
                params.offset.unwrap_or(0),
                length,
                true,
                params.field_filter,
                facet_filter,
                result_sort,
            )
            .await;
        Ok(CallToolResult::success(vec![ContentBlock::text(format!(
            "Deleted up to {length} matching document(s)"
        ))]))
    }

    #[tool(
        description = "Clear all documents from the active index while retaining its schema and settings."
    )]
    async fn clear_index(&self) -> Result<CallToolResult, McpError> {
        let index_arc = self.active_index().await;
        index_arc.write().await.clear_index().await;
        Ok(CallToolResult::success(vec![ContentBlock::text(
            "Index cleared",
        )]))
    }
}

#[tool_handler]
impl ServerHandler for SeekStormMcpServer {
    fn get_info(&self) -> ServerConfig {
        ServerConfig::new(ServerCapabilities::builder().enable_tools().build())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let index_path =
        env::var("SEEKSTORM_INDEX_PATH").unwrap_or_else(|_| "./seekstorm_index".to_string());
    let index_path = PathBuf::from(index_path);

    let index_arc = open_or_create_index(&index_path).await.map_err(|err| {
        format!(
            "failed to open or create index at {}: {err}",
            index_path.display()
        )
    })?;

    let server = SeekStormMcpServer::new(index_arc, index_path);
    let index_state = server.index_arc.clone();
    let service = server.serve(stdio()).await?;
    service.waiting().await?;
    index_state.read().await.commit().await;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_schema_keeps_the_bootstrap_document_fields() {
        let schema = default_schema();
        let fields: Vec<_> = schema.iter().map(|field| field.field.as_str()).collect();

        assert_eq!(fields, ["title", "body", "path"]);
        assert!(schema.iter().all(|field| field.store));
    }

    #[test]
    fn document_conversion_accepts_arbitrary_nested_json_fields() {
        let params: IndexDocumentParams = serde_json::from_value(json!({
            "document": {
                "headline": "Rust search",
                "body": "Local document indexing",
                "metadata": {"team": "search", "tags": ["rust", "mcp"]}
            }
        }))
        .unwrap();

        let document = params.into_document().unwrap();
        assert_eq!(document["headline"], "Rust search");
        assert_eq!(document["metadata"]["tags"][1], "mcp");
    }

    #[test]
    fn document_conversion_rejects_non_object_values() {
        let params = IndexDocumentParams {
            document: json!(["not", "a", "document"]),
        };

        assert!(params.into_document().is_err());
    }

    #[test]
    fn create_index_request_keeps_custom_schema_and_settings() {
        let request: CreateIndexRequest = serde_json::from_value(json!({
            "index_name": "custom",
            "schema": [
                {"field": "headline", "store": true, "index_lexical": true, "field_type": "Text"},
                {"field": "body", "store": true, "index_lexical": true, "field_type": "Text", "longest": true},
                {"field": "published", "store": true, "index_lexical": false, "field_type": "Timestamp", "facet": true}
            ],
            "similarity": "Bm25f",
            "tokenizer": "UnicodeAlphanumericFolded"
        }))
        .unwrap();

        let (meta, schema, _) = index_meta(request);
        assert_eq!(meta.name, "custom");
        assert_eq!(meta.lexical_similarity, LexicalSimilarity::Bm25f);
        assert_eq!(schema.len(), 3);
        assert_eq!(schema[2].field, "published");
        assert!(schema[2].facet);
    }
}
