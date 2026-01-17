use add_result::decode_positions_commit;
use ahash::{AHashMap, AHashSet};
use futures::future;
use indexmap::IndexMap;
use itertools::Itertools;
use memmap2::{Mmap, MmapMut, MmapOptions};
use num::FromPrimitive;
use num_derive::FromPrimitive;

use num_format::{Locale, ToFormattedString};

use rust_stemmers::{Algorithm, Stemmer};
use search::{QueryType, Search};
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use std::{
    collections::{HashMap, VecDeque},
    fs::{self, File},
    hint,
    io::{BufRead, BufReader, Read, Seek, Write},
    path::Path,
    sync::Arc,
    time::Instant,
};
use symspell_complete_rs::{PruningRadixTrie, SymSpell};
use tokio::sync::{RwLock, Semaphore};
use utils::{read_u32, write_u16};
use utoipa::ToSchema;

#[cfg(feature = "zh")]
use crate::word_segmentation::WordSegmentationTM;
use crate::{
    add_result::{self, B, K, SIGMA},
    commit::Commit,
    geo_search::encode_morton_2_d,
    search::{
        self, FacetFilter, Point, QueryFacet, QueryRewriting, Ranges, ResultObject, ResultSort,
        ResultType, SearchShard,
    },
    tokenizer::tokenizer,
    utils::{
        self, read_u8_ref, read_u16, read_u16_ref, read_u32_ref, read_u64, read_u64_ref, write_f32,
        write_f64, write_i8, write_i16, write_i32, write_i64, write_u32, write_u64,
    },
};

#[cfg(all(target_feature = "aes", target_feature = "sse2"))]
use gxhash::{gxhash32, gxhash64};

pub(crate) const FILE_PATH: &str = "files";
pub(crate) const INDEX_FILENAME: &str = "index.bin";
pub(crate) const DOCSTORE_FILENAME: &str = "docstore.bin";
pub(crate) const DELETE_FILENAME: &str = "delete.bin";
pub(crate) const SCHEMA_FILENAME: &str = "schema.json";
pub(crate) const SYNONYMS_FILENAME: &str = "synonyms.json";
pub(crate) const META_FILENAME: &str = "index.json";
pub(crate) const FACET_FILENAME: &str = "facet.bin";
pub(crate) const FACET_VALUES_FILENAME: &str = "facet.json";

pub(crate) const DICTIONARY_FILENAME: &str = "dictionary.csv";
pub(crate) const COMPLETIONS_FILENAME: &str = "completions.csv";

pub(crate) const VERSION: &str = env!("CARGO_PKG_VERSION");

const INDEX_HEADER_SIZE: u64 = 4;
/// Incompatible index  format change: new library can't open old format, and old library can't open new format
pub const INDEX_FORMAT_VERSION_MAJOR: u16 = 5;
/// Backward compatible format change: new library can open old format, but old library can't open new format
pub const INDEX_FORMAT_VERSION_MINOR: u16 = 0;

/// Maximum processed positions per term per document: default=65_536. E.g. 65,536 * 'the' per document, exceeding positions are ignored for search.
pub const MAX_POSITIONS_PER_TERM: usize = 65_536;
pub(crate) const STOP_BIT: u8 = 0b10000000;
pub(crate) const FIELD_STOP_BIT_1: u8 = 0b0010_0000;
pub(crate) const FIELD_STOP_BIT_2: u8 = 0b0100_0000;
/// maximum number of documents per block
pub const ROARING_BLOCK_SIZE: usize = 65_536;

pub(crate) const SPEEDUP_FLAG: bool = true;
pub(crate) const SORT_FLAG: bool = true;

pub(crate) const POSTING_BUFFER_SIZE: usize = 400_000_000;
pub(crate) const MAX_QUERY_TERM_NUMBER: usize = 100;
pub(crate) const SEGMENT_KEY_CAPACITY: usize = 1000;

/// A document is a flattened, single level of key-value pairs, where key is an arbitrary string, and value represents any valid JSON value.
pub type Document = HashMap<String, serde_json::Value>;

/// File type for storing documents: Path, Bytes, None.
#[derive(Clone, PartialEq)]
pub enum FileType {
    /// File path
    Path(Box<Path>),
    /// File bytes
    Bytes(Box<Path>, Box<[u8]>),
    /// No file
    None,
}

/// Defines where the index resides during search:
/// - Ram (the complete index is preloaded to Ram when opening the index)
/// - Mmap (the index is accessed via memory-mapped files). See architecture.md for details.
/// - At commit the data is serialized to disk for persistence both in Ram and Mmap mode.
/// - The serialization format is identical for Ram and Mmap mode, allowing to change it retrospectively.
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
pub enum AccessType {
    /// Ram (the complete index is preloaded to Ram when opening the index).
    /// - Index size is limited by available RAM size.
    /// - Slightly fastesr search speed.
    /// - Higher index loading time.
    /// - Higher RAM usage.
    Ram = 0,
    /// Mmap (the index is accessed via memory-mapped files). See architecture.md for details.
    /// - Enables index size scaling beyond RAM size.
    /// - Slightly slower search speed compared to Ram.
    /// - Faster index loading time compared to Ram.
    /// - Lower RAM usage.
    Mmap = 1,
}

/// Similarity type defines the scoring and ranking of the search results:
/// - Bm25f: considers documents composed from several fields, with different field lengths and importance
/// - Bm25fProximity: considers term proximity, e.g. for implicit phrase search with improved relevancy
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq, Default, ToSchema)]
pub enum SimilarityType {
    /// Bm25f considers documents composed from several fields, with different field lengths and importance
    Bm25f = 0,
    /// Bm25fProximity considers term proximity, e.g. for implicit phrase search with improved relevancy
    #[default]
    Bm25fProximity = 1,
}

/// Defines tokenizer behavior:
/// AsciiAlphabetic
/// - Mainly for for benchmark compatibility
/// - Only ASCII alphabetic chars are recognized as token.
///
/// UnicodeAlphanumeric
/// - All Unicode alphanumeric chars are recognized as token.
/// - Allows '+' '-' '#' in middle or end of a token: c++, c#, block-max.
///
/// UnicodeAlphanumericFolded
/// - All Unicode alphanumeric chars are recognized as token.
/// - Allows '+' '-' '#' in middle or end of a token: c++, c#, block-max.
/// - Diacritics, accents, zalgo text, umlaut, bold, italic, full-width UTF-8 characters are converted into its basic representation.
/// - Apostroph handling prevents that short term parts preceding or following the apostroph get indexed (e.g. "s" in "someone's").
/// - Tokenizing might be slower due to folding and apostroph processing.
///
/// UnicodeAlphanumericZH
/// - Implements Chinese word segmentation to segment continuous Chinese text into tokens for indexing and search.
/// - Supports mixed Latin and Chinese texts
/// - Supports Chinese sentence boundary chars for KWIC snippets ahd highlighting.
/// - Requires feature #[cfg(feature = "zh")]
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq, Copy, Default, ToSchema)]
pub enum TokenizerType {
    /// Only ASCII alphabetic chars are recognized as token. Mainly for benchmark compatibility.
    #[default]
    AsciiAlphabetic = 0,
    /// All Unicode alphanumeric chars are recognized as token.
    /// Allow '+' '-' '#' in middle or end of a token: c++, c#, block-max.
    UnicodeAlphanumeric = 1,
    /// All Unicode alphanumeric chars are recognized as token.
    /// Allows '+' '-' '#' in middle or end of a token: c++, c#, block-max.
    /// Diacritics, accents, zalgo text, umlaut, bold, italic, full-width UTF-8 characters are converted into its basic representation.
    /// Apostroph handling prevents that short term parts preceding or following the apostroph get indexed (e.g. "s" in "someone's").
    /// Tokenizing might be slower due to folding and apostroph processing.
    UnicodeAlphanumericFolded = 2,
    /// Tokens are separated by whitespace. Mainly for benchmark compatibility.
    Whitespace = 3,
    /// Tokens are separated by whitespace. Token are converted to lowercase. Mainly for benchmark compatibility.
    WhitespaceLowercase = 4,
    /// Implements Chinese word segmentation to segment continuous Chinese text into tokens for indexing and search.
    /// Supports mixed Latin and Chinese texts
    /// Supports Chinese sentence boundary chars for KWIC snippets ahd highlighting.
    /// Requires feature #[cfg(feature = "zh")]
    #[cfg(feature = "zh")]
    UnicodeAlphanumericZH = 5,
}

/// Defines stemming behavior, reducing inflected words to their word stem, base or root form.
/// Stemming increases recall, but decreases precision. It can introduce false positive results.
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq, Copy, Default, ToSchema)]
pub enum StemmerType {
    /// No stemming
    #[default]
    None = 0,
    /// Arabic stemmer
    Arabic = 1,
    /// Danish stemmer
    Danish = 2,
    /// Dutch stemmer
    Dutch = 3,
    /// English stemmer
    English = 4,
    /// Finnish stemmer
    Finnish = 5,
    /// French stemmer
    French = 6,
    /// German stemmer
    German = 7,
    /// Hungarian stemmer
    Greek = 8,
    /// Hungarian stemmer
    Hungarian = 9,
    /// Italian stemmer
    Italian = 10,
    /// Norwegian stemmer
    Norwegian = 11,
    /// Portuguese stemmer
    Portuguese = 12,
    /// Romanian stemmer
    Romanian = 13,
    /// Russian stemmer
    Russian = 14,
    /// Spanish stemmer
    Spanish = 15,
    /// Swedish stemmer
    Swedish = 16,
    /// Tamil stemmer
    Tamil = 17,
    /// Turkish stemmer
    Turkish = 18,
}

pub(crate) struct LevelIndex {
    pub document_length_compressed_array: Vec<[u8; ROARING_BLOCK_SIZE]>,

    pub docstore_pointer_docs: Vec<u8>,
    pub docstore_pointer_docs_pointer: usize,
    pub document_length_compressed_array_pointer: usize,
}

/// Posting lists are divided into blocks of a doc id range of 65.536 (16 bit).
/// Each block can be compressed with a different method.
#[derive(Default, Debug, Deserialize, Serialize, Clone)]
pub(crate) struct BlockObjectIndex {
    pub max_block_score: f32,
    pub block_id: u32,
    pub compression_type_pointer: u32,
    pub posting_count: u16,
    pub max_docid: u16,
    pub max_p_docid: u16,
    pub pointer_pivot_p_docid: u16,
}

/// PostingListObjectIndex owns all blocks of a postinglist of a term
#[derive(Default)]
pub(crate) struct PostingListObjectIndex {
    pub posting_count: u32,
    pub posting_count_ngram_1: u32,
    pub posting_count_ngram_2: u32,
    pub posting_count_ngram_3: u32,
    pub posting_count_ngram_1_compressed: u8,
    pub posting_count_ngram_2_compressed: u8,
    pub posting_count_ngram_3_compressed: u8,
    pub max_list_score: f32,
    pub blocks: Vec<BlockObjectIndex>,

    pub position_range_previous: u32,
}

#[derive(Default, Debug, Deserialize, Serialize, Clone)]
pub(crate) struct PostingListObject0 {
    pub pointer_first: usize,
    pub pointer_last: usize,
    pub posting_count: usize,

    pub max_block_score: f32,
    pub max_docid: u16,
    pub max_p_docid: u16,

    pub ngram_type: NgramType,
    pub term_ngram1: String,
    pub term_ngram2: String,
    pub term_ngram3: String,
    pub posting_count_ngram_1: f32,
    pub posting_count_ngram_2: f32,
    pub posting_count_ngram_3: f32,
    pub posting_count_ngram_1_compressed: u8,
    pub posting_count_ngram_2_compressed: u8,
    pub posting_count_ngram_3_compressed: u8,

    pub position_count: usize,
    pub pointer_pivot_p_docid: u16,
    pub size_compressed_positions_key: usize,
    pub docid_delta_max: u16,
    pub docid_old: u16,
    pub compression_type_pointer: u32,
}

/// Type of posting list compression.
#[derive(Default, Debug, Deserialize, Serialize, Clone, PartialEq, FromPrimitive)]
pub(crate) enum CompressionType {
    Delta = 0,
    Array = 1,
    Bitmap = 2,
    Rle = 3,
    #[default]
    Error = 4,
}

pub(crate) struct QueueObject<'a> {
    pub query_list: Vec<PostingListObjectQuery<'a>>,
    pub query_index: usize,
    pub max_score: f32,
}

/// PostingListObjectQuery manages thes posting list for each unique query term during intersection.
#[derive(Clone)]
pub(crate) struct PostingListObjectQuery<'a> {
    pub posting_count: u32,
    pub max_list_score: f32,
    pub blocks: &'a Vec<BlockObjectIndex>,
    pub blocks_index: usize,

    pub term: String,
    pub key0: u32,

    pub compression_type: CompressionType,
    pub rank_position_pointer_range: u32,
    pub compressed_doc_id_range: usize,
    pub pointer_pivot_p_docid: u16,

    pub posting_pointer: usize,
    pub posting_pointer_previous: usize,

    pub byte_array: &'a [u8],

    pub p_block: i32,
    pub p_block_max: i32,
    pub p_docid: usize,
    pub p_docid_count: usize,

    pub rangebits: i32,
    pub docid: i32,
    pub bitposition: u32,

    pub intersect: u64,
    pub ulong_pos: usize,

    pub run_end: i32,
    pub p_run: i32,
    pub p_run_count: i32,
    pub p_run_sum: i32,

    pub term_index_unique: usize,
    pub positions_count: u32,
    pub positions_pointer: u32,

    pub idf: f32,
    pub idf_ngram1: f32,
    pub idf_ngram2: f32,
    pub idf_ngram3: f32,
    pub tf_ngram1: u32,
    pub tf_ngram2: u32,
    pub tf_ngram3: u32,
    pub ngram_type: NgramType,

    pub end_flag: bool,
    pub end_flag_block: bool,
    pub is_embedded: bool,
    pub embedded_positions: [u32; 4],
    pub field_vec: SmallVec<[(u16, usize); 2]>,
    pub field_vec_ngram1: SmallVec<[(u16, usize); 2]>,
    pub field_vec_ngram2: SmallVec<[(u16, usize); 2]>,
    pub field_vec_ngram3: SmallVec<[(u16, usize); 2]>,
    pub bm25_flag: bool,
}

pub(crate) static DUMMY_VEC: Vec<BlockObjectIndex> = Vec::new();
pub(crate) static DUMMY_VEC_8: Vec<u8> = Vec::new();

impl Default for PostingListObjectQuery<'_> {
    fn default() -> Self {
        Self {
            posting_count: 0,
            max_list_score: 0.0,
            blocks: &DUMMY_VEC,
            blocks_index: 0,
            term: "".to_string(),
            key0: 0,
            compression_type: CompressionType::Error,
            rank_position_pointer_range: 0,
            compressed_doc_id_range: 0,
            pointer_pivot_p_docid: 0,
            posting_pointer: 0,
            posting_pointer_previous: 0,
            byte_array: &DUMMY_VEC_8,
            p_block: 0,
            p_block_max: 0,
            p_docid: 0,
            p_docid_count: 0,
            rangebits: 0,
            docid: 0,
            bitposition: 0,
            run_end: 0,
            p_run: 0,
            p_run_count: 0,
            p_run_sum: 0,
            term_index_unique: 0,
            positions_count: 0,
            positions_pointer: 0,
            idf: 0.0,
            idf_ngram1: 0.0,
            idf_ngram2: 0.0,
            idf_ngram3: 0.0,
            ngram_type: NgramType::SingleTerm,
            is_embedded: false,
            embedded_positions: [0; 4],
            field_vec: SmallVec::new(),
            tf_ngram1: 0,
            tf_ngram2: 0,
            tf_ngram3: 0,
            field_vec_ngram1: SmallVec::new(),
            field_vec_ngram2: SmallVec::new(),
            field_vec_ngram3: SmallVec::new(),

            end_flag: false,
            end_flag_block: false,
            bm25_flag: true,
            intersect: 0,
            ulong_pos: 0,
        }
    }
}

/// NonUniquePostingListObjectQuery manages these posting list for each non-unique query term during intersection.
/// It references to the unique query terms.
#[derive(Clone)]
pub(crate) struct NonUniquePostingListObjectQuery<'a> {
    pub term_index_unique: usize,
    pub term_index_nonunique: usize,
    pub pos: u32,
    pub p_pos: i32,
    pub positions_pointer: usize,
    pub positions_count: u32,
    pub byte_array: &'a [u8],
    pub key0: u32,
    pub is_embedded: bool,
    pub embedded_positions: [u32; 4],
    pub p_field: usize,
    pub field_vec: SmallVec<[(u16, usize); 2]>,
}

/// Terms are converted to hashs. The index is divided into key hash range partitioned segments.
/// for each strip (key hash range) a separate dictionary (key hash - posting list) is maintained.
/// The index hash multiple segments, each segments contains multiple terms, each term has a postinglist, each postinglist has multiple blocks.
pub(crate) struct SegmentIndex {
    pub byte_array_blocks: Vec<Vec<u8>>,
    pub byte_array_blocks_pointer: Vec<(usize, usize, u32)>,
    pub segment: AHashMap<u64, PostingListObjectIndex>,
}

/// StripObject0 defines a strip (key hash range) within level 0. Level 0 is the mutable level where all writes are taking place.
/// After each 65.536 docs the level 0 is flushed as an immutable block to the next level
#[derive(Default, Debug, Clone)]
pub(crate) struct SegmentLevel0 {
    pub segment: AHashMap<u64, PostingListObject0>,
    pub positions_compressed: Vec<u8>,
}

/// FieldType defines the type of a field in the document: u8, u16, u32, u64, i8, i16, i32, i64, f32, f64, point, string, stringset, text.
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq, Default, ToSchema)]
pub enum FieldType {
    /// Unsigned 8-bit integer
    U8,
    /// Unsigned 16-bit integer
    U16,
    /// Unsigned 32-bit integer
    U32,
    /// Unsigned 64-bit integer
    U64,
    /// Signed 8-bit integer
    I8,
    /// Signed 16-bit integer
    I16,
    /// Signed 32-bit integer
    I32,
    /// Signed 64-bit integer
    I64,
    /// Timestamp is identical to I64, but to be used for Unix timestamps <https://en.wikipedia.org/wiki/Unix_time>.
    /// The reason for a separate FieldType is to enable the UI to interpret I64 as timestamp without using the field name as indicator.
    /// For date facets and filtering.
    Timestamp,
    /// Floating point 32-bit
    F32,
    /// Floating point 64-bit
    F64,
    /// Boolean
    Bool,
    /// String16
    /// allows a maximum cardinality of 65_535 (16 bit) distinct values, is space-saving.
    #[default]
    String16,
    /// String32
    /// allows a maximum cardinality of 4_294_967_295 (32 bit) distinct values
    String32,
    /// StringSet16 is a set of strings, e.g. tags, categories, keywords, authors, genres, etc.
    /// allows a maximum cardinality of 65_535 (16 bit) distinct values, is space-saving.
    StringSet16,
    /// StringSet32 is a set of strings, e.g. tags, categories, keywords, authors, genres, etc.
    /// allows a maximum cardinality of 4_294_967_295 (32 bit) distinct values
    StringSet32,
    /// Point is a geographic field type: A `Vec<f64>` with two coordinate values (latitude and longitude) are internally encoded into a single u64 value (Morton code).
    /// Morton codes enable efficient range queries.
    /// Latitude and longitude are a pair of numbers (coordinates) used to describe a position on the plane of a geographic coordinate system.
    /// The numbers are in decimal degrees format and range from -90 to 90 for latitude and -180 to 180 for longitude.
    /// Coordinates are internally stored as u64 morton code: both f64 values are multiplied by 10_000_000, converted to i32 and bitwise interleaved into a single u64 morton code
    /// The conversion between longitude/latitude coordinates and Morton code is lossy due to rounding errors.
    Point,
    /// Text is a text field, that will be tokenized by the selected Tokenizer into string tokens.
    Text,
}

/// Defines synonyms for terms per index.
#[derive(Debug, Clone, Deserialize, Serialize, ToSchema)]
pub struct Synonym {
    /// List of terms that are synonyms.
    pub terms: Vec<String>,
    /// Creates alternative versions of documents where in each copy a term is replaced with one of its synonyms.
    /// Doesn't impact the query latency, but does increase the index size.
    /// Multi-way synonyms (default): all terms are synonyms of each other.
    /// One-way synonyms: only the first term is a synonym of the following terms, but not vice versa.
    /// E.g. [street, avenue, road] will result in searches for street to return documents containing any of the terms street, avenue or road,
    /// but searches for avenue will only return documents containing avenue, but not documents containing street or road.
    /// Currently only single terms without spaces are supported.
    /// Synonyms are supported in result highlighting.
    /// The synonyms that were created with the synonyms parameter in create_index are stored in synonyms.json in the index directory contains  
    /// Can be manually modified, but becomes effective only after restart and only for newly indexed documents.
    #[serde(default = "default_as_true")]
    pub multiway: bool,
}

fn default_as_true() -> bool {
    true
}

/// Defines a field in index schema: field, stored, indexed , field_type, facet, boost.
#[derive(Debug, Clone, Deserialize, Serialize, ToSchema)]
pub struct SchemaField {
    /// unique name of a field
    pub field: String,
    /// only stored fields are returned in the search results
    pub stored: bool,
    /// only indexed fields can be searched
    pub indexed: bool,
    /// type of a field
    pub field_type: FieldType,
    /// optional faceting for a field
    /// Faceting can be enabled both for string field type and numerical field types.
    /// both numerical and string fields can be indexed (indexed=true) and stored (stored=true) in the json document,
    /// but with field_facet=true they are additionally stored in a binary format, for fast faceting and sorting without docstore access (decompression, deserialization)
    #[serde(skip_serializing_if = "is_default_bool")]
    #[serde(default = "default_false")]
    pub facet: bool,

    /// Indicate the longest field in schema.  
    /// Otherwise the longest field will be automatically detected in first index_document.
    /// Setting/detecting the longest field ensures efficient index encoding.
    #[serde(skip_serializing_if = "is_default_bool")]
    #[serde(default = "default_false")]
    pub longest: bool,

    /// optional custom weight factor for Bm25 ranking
    #[serde(skip_serializing_if = "is_default_f32")]
    #[serde(default = "default_1")]
    pub boost: f32,

    /// if both indexed=true and dictionary_source=true then the terms from this field are added to dictionary to the spelling correction dictionary.
    /// if disabled, then a manually generated dictionary can be used: {index_path}/dictionary.csv
    #[serde(skip_serializing_if = "is_default_bool")]
    #[serde(default = "default_false")]
    pub dictionary_source: bool,

    /// if both indexed=true and completion_source=true then the n-grams (unigrams, bigrams, trigrams) from this field are added to the auto-completion list.
    /// if disabled, then a manually generated completion list can be used: {index_path}/completions.csv
    /// it is recommended to enable completion_source only for fields that contain short text with high-quality terms for auto-completion, e.g. title, author, category, product name, tags,
    /// in order to keep the extraction time and RAM requirement for completions low and the completions relevance high.
    #[serde(skip_serializing_if = "is_default_bool")]
    #[serde(default = "default_false")]
    pub completion_source: bool,

    #[serde(skip)]
    pub(crate) indexed_field_id: usize,
    #[serde(skip_deserializing)]
    pub(crate) field_id: usize,
}

/// Defines a field in index schema: field, stored, indexed , field_type, facet, boost.
/// # Parameters
/// - field: unique name of a field
/// - stored: only stored fields are returned in the search results
/// - indexed: only indexed fields can be searched
/// - field_type: type of a field: u8, u16, u32, u64, i8, i16, i32, i64, f32, f64, point
/// - facet: enable faceting for a field: for sorting results by field values, for range filtering, for result count per field value or range
/// - `longest`: This allows to annotate (manually set) the longest field in schema.  
///   Otherwise the longest field will be automatically detected in first index_document.
///   Setting/detecting the longest field ensures efficient index encoding.
/// - boost: optional custom weight factor for Bm25 ranking
/// # Returns
/// - SchemaField
/// # Example
/// ```rust
/// use seekstorm::index::{SchemaField, FieldType};
/// let schema_field = SchemaField::new("title".to_string(), true, true, FieldType::String16, false, false, 1.0, false, false);
/// ```
impl SchemaField {
    /// Creates a new SchemaField.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        field: String,
        stored: bool,
        indexed: bool,
        field_type: FieldType,
        facet: bool,
        longest: bool,
        boost: f32,
        dictionary_source: bool,
        completion_source: bool,
    ) -> Self {
        SchemaField {
            field,
            stored,
            indexed,
            field_type,
            facet,
            longest,
            boost,
            dictionary_source,
            completion_source,

            indexed_field_id: 0,
            field_id: 0,
        }
    }
}

fn default_false() -> bool {
    false
}

fn is_default_bool(num: &bool) -> bool {
    !(*num)
}

fn default_1() -> f32 {
    1.0
}

fn is_default_f32(num: &f32) -> bool {
    *num == 1.0
}

pub(crate) struct IndexedField {
    pub schema_field_name: String,
    pub field_length_sum: usize,
    pub indexed_field_id: usize,

    pub is_longest_field: bool,
}

/// StopwordType defines the stopword behavior: None, English, German, French, Spanish, Custom.
/// Stopwords are removed, both from index and query: for compact index size and faster queries.
/// Stopword removal has drawbacks: “The Who”, “Take That”, “Let it be”, “To be or not to be”, "The The", "End of days", "What might have been" are all valid queries for bands, songs, movies, literature,
/// but become impossible when stopwords are removed.
/// The lists of stop_words and frequent_words should not overlap.
#[derive(Debug, Clone, Deserialize, Serialize, Default, ToSchema)]
pub enum StopwordType {
    /// No stopwords
    #[default]
    None,
    /// English stopwords
    English,
    /// German stopwords
    German,
    /// French stopwords
    French,
    /// Spanish stopwords
    Spanish,
    /// Custom stopwords
    Custom {
        ///List of stopwords.
        terms: Vec<String>,
    },
}

/// FrequentwordType defines the frequentword behavior: None, English, German, French, Spanish, Custom.
/// Adjacent frequent terms are combined to bi-grams, both in index and query: for shorter posting lists and faster phrase queries (only for bi-grams of frequent terms).
/// The lists of stop_words and frequent_words should not overlap.
#[derive(Debug, Clone, Deserialize, Serialize, Default, ToSchema)]
pub enum FrequentwordType {
    /// No frequent words
    None,
    /// English frequent words
    #[default]
    English,
    /// German frequent words
    German,
    /// French frequent words
    French,
    /// Spanish frequent words
    Spanish,
    /// Custom frequent words
    Custom {
        ///List of frequent terms, max. 256 terms.
        terms: Vec<String>,
    },
}

/// Defines spelling correction (fuzzy search) settings for an index.
#[derive(Debug, Clone, Deserialize, Serialize, ToSchema)]
pub struct SpellingCorrection {
    /// The edit distance thresholds for suggestions: 1..2 recommended; higher values increase latency and memory consumption.
    pub max_dictionary_edit_distance: usize,
    /// Term length thresholds for each edit distance.
    ///   None:    max_dictionary_edit_distance for all terms lengths
    ///   Some(\[4\]):    max_dictionary_edit_distance for all terms lengths >= 4,
    ///   Some(\[2,8\]):    max_dictionary_edit_distance for all terms lengths >=2, max_dictionary_edit_distance +1 for all terms for lengths>=8
    pub term_length_threshold: Option<Vec<usize>>,

    /// The minimum frequency count for dictionary words to be considered eligible for spelling correction.
    /// Depends on the corpus size, 1..20 recommended.
    /// If count_threshold is too high, some correct words might be missed from the dictionary and deemed misspelled,
    /// if count_threshold is too low, some misspelled words from the corpus might be considered correct and added to the dictionary.
    /// Dictionary terms eligible for spelling correction (frequency count >= count_threshold) consume much more RAM, than the candidates (frequency count < count_threshold),  
    /// but the terms below count_threshold will be included in dictionary.csv too.
    pub count_threshold: usize,

    /// Limits the maximum number of dictionary entries (terms >= count_threshold) to generate during indexing, preventing excessive RAM consumption.
    /// The number of terms in dictionary.csv will be higher, because it contains also the terms < count_threshold, to become eligible in the future during incremental dictionary updates.
    /// Dictionary terms eligible for spelling correction (frequency count >= count_threshold) consume much more RAM, than the candidates (frequency count < count_threshold).
    /// ⚠️ Above this threshold no new terms are added to the dictionary, causing them to be deemed incorrect during spelling correction and possibly changed to similar terms that are in the dictionary.
    pub max_dictionary_entries: usize,
}

/// Defines spelling correction (fuzzy search) settings for an index.
#[derive(Debug, Clone, Deserialize, Serialize, ToSchema)]
pub struct QueryCompletion {
    /// Maximum number of completions to generate during indexing
    /// disabled if == 0
    pub max_completion_entries: usize,
}

/// Specifies SimilarityType, TokenizerType and AccessType when creating an new index
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct IndexMetaObject {
    /// unique index ID
    /// Only used in SeekStorm server, not by the SeekStorm library itself.
    /// In the SeekStorm server with REST API, the index ID is used to specify the index (within the API key) where you want to index and search.
    pub id: u64,
    /// index name: used informational purposes
    pub name: String,
    /// SimilarityType defines the scoring and ranking of the search results: Bm25f or Bm25fProximity
    pub similarity: SimilarityType,
    /// TokenizerType defines the tokenizer behavior: AsciiAlphabetic, UnicodeAlphanumeric, UnicodeAlphanumericFolded, UnicodeAlphanumericZH
    pub tokenizer: TokenizerType,
    /// StemmerType defines the stemming behavior: None, Arabic, Armenian, Danish, Dutch, English, French, German, Greek, Hungarian, Italian, Norwegian, Portuguese, Romanian, Russian, Spanish, Swedish, Tamil, Turkish
    pub stemmer: StemmerType,

    /// StopwordType defines the stopword behavior: None, English, German, French, Spanish, Custom.
    /// Stopwords are removed, both from index and query: for compact index size and faster queries.
    /// Stopword removal has drawbacks: “The Who”, “Take That”, “Let it be”, “To be or not to be”, "The The", "End of days", "What might have been" are all valid queries for bands, songs, movies, literature,
    /// but become impossible when stopwords are removed.
    /// The lists of stop_words and frequent_words should not overlap.
    #[serde(default)]
    pub stop_words: StopwordType,
    /// FrequentwordType defines the frequentword behavior: None, English, German, French, Spanish, Custom.
    /// Adjacent frequent terms are combined to bi-grams, both in index and query: for shorter posting lists and faster phrase queries (only for bi-grams of frequent terms).
    /// The lists of stop_words and frequent_words should not overlap.
    #[serde(default)]
    pub frequent_words: FrequentwordType,
    /// N-gram indexing: n-grams are indexed in addition to single terms, for fast phrase search, at the cost of higher index size
    /// Preference valid both for index time and query time. Any change requires reindexing.
    /// bitwise OR flags:
    /// SingleTerm = 0b00000000, always enabled in addition to the other optional NgramSet below
    /// NgramFF    = 0b00000001, frequent frequent
    /// NgramFR    = 0b00000010, frequent rare
    /// NgramRF    = 0b00000100, rare frequent
    /// NgramFFF   = 0b00001000, frequent frequent frequent
    /// NgramRFF   = 0b00010000, rare frequent frequent
    /// NgramFFR   = 0b00100000, frequent frequent rare
    /// NgramFRF   = 0b01000000, frequent rare frequent
    ///
    /// When **minimum index size** is more important than phrase query latency, we recommend **Single Terms**:  
    /// `NgramSet::SingleTerm as u8`
    ///
    /// For a **good balance of latency and index size** cost, we recommend **Single Terms + Frequent Bigrams + Frequent Trigrams** (default):  
    /// `NgramSet::SingleTerm as u8 | NgramSet::NgramFF as u8 | NgramSet::NgramFFF`
    ///
    /// When **minimal phrase query latency** is more important than low index size, we recommend **Single Terms + Mixed Bigrams + Frequent Trigrams**:
    /// `NgramSet::SingleTerm as u8 | NgramSet::NgramFF as u8 | NgramSet::NgramFR as u8 | NgramSet::NgramRF | NgramSet::NgramFFF`
    #[serde(default = "ngram_indexing_default")]
    pub ngram_indexing: u8,

    /// AccessType defines where the index resides during search: Ram or Mmap
    pub access_type: AccessType,
    /// Enable spelling correction for search queries using the SymSpell algorithm.
    /// SymSpell enables finding those spelling suggestions in a dictionary very fast with minimum Damerau-Levenshtein edit distance and maximum word occurrence frequency.
    /// When enabled, a SymSpell dictionary is incrementally created during indexing of documents and stored in the index.
    /// The spelling correction is not based on a generic dictionary, but on a domain specific one derived from your indexed documents (only indexed fields).
    /// This makes it language independent and prevents any discrepancy between corrected word and indexed content.
    /// The creation of an individual dictionary derived from the indexed documents improves the correction quality compared to a generic dictionary.
    /// An dictionary per index improves the privacy compared to a global dictionary derived from all indices.
    /// The dictionary is deleted when delete_index or clear_index is called.
    /// Note: enabling spelling correction increases the index size, indexing time and query latency.
    /// Default: None. Enable by setting CreateDictionary with values for max_dictionary_edit_distance (1..2 recommended) and optionally a term length thresholds for each edit distance.
    /// The higher the value, the higher the number of errors taht can be corrected - but also the memory consumption, lookup latency, and the number of false positives.
    /// ⚠️ In addition to the create_index parameter `meta.spelling_correction` you also need to set the parameter `query_rewriting` in the search method to enable it per query.
    #[serde(default)]
    pub spelling_correction: Option<SpellingCorrection>,

    /// Enable query completion for search queries
    /// When enabled, an auto-completion list is incrementally created during indexing of documents and stored in the index.
    /// Because the completions are not based on a generic dictionary, but on a domain specific one derived from your indexed documents (only from indexed fields with completion_source=true), this increases the relevance of completions.
    /// ⚠️ Deriving completions from indexed documents increases the indexing time and index size.
    #[serde(default)]
    pub query_completion: Option<QueryCompletion>,
}

fn ngram_indexing_default() -> u8 {
    NgramSet::NgramFF as u8 | NgramSet::NgramFFF as u8
}

#[derive(Debug, Clone, Default)]
pub(crate) struct ResultFacet {
    pub field: String,
    pub values: AHashMap<u32, usize>,
    pub prefix: String,
    pub length: u32,
    pub ranges: Ranges,
}

/// DistanceUnit defines the unit for distance calculation: kilometers or miles.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, ToSchema)]
pub enum DistanceUnit {
    /// Kilometers
    Kilometers,
    /// Miles
    Miles,
}

/// DistanceField defines a field for proximity search.
#[derive(Debug, Clone, Deserialize, Serialize, ToSchema)]
pub struct DistanceField {
    /// field name of a numeric facet field (currently onyl Point field type supported)
    pub field: String,
    /// field name of the distance field we are deriving from the numeric facet field (Point type) and the base (Point type)
    pub distance: String,
    /// base point (lat,lon) for distance calculation
    pub base: Point,
    /// distance unit for the distance field: kilometers or miles
    pub unit: DistanceUnit,
}

impl Default for DistanceField {
    fn default() -> Self {
        DistanceField {
            field: String::new(),
            distance: String::new(),
            base: Vec::new(),
            unit: DistanceUnit::Kilometers,
        }
    }
}

/// MinMaxField represents the minimum and maximum value of a field.
#[derive(Deserialize, Serialize, Debug, Clone, Default)]
pub struct MinMaxField {
    /// minimum value of the field
    pub min: ValueType,
    /// maximum value of the field
    pub max: ValueType,
}

/// MinMaxFieldJson is a JSON representation of the minimum and maximum value of a field.
#[derive(Deserialize, Serialize, Debug, Clone, Default, ToSchema)]
pub struct MinMaxFieldJson {
    /// minimum value of the field
    pub min: serde_json::Value,
    /// maximum value of the field
    pub max: serde_json::Value,
}

/// Value type for a field: u8, u16, u32, u64, i8, i16, i32, i64, f32, f64, point, none.
#[derive(Debug, Clone, Deserialize, Serialize, Default, PartialEq)]
pub enum ValueType {
    /// Unsigned 8-bit integer
    U8(u8),
    /// Unsigned 16-bit integer
    U16(u16),
    /// Unsigned 32-bit integer
    U32(u32),
    /// Unsigned 64-bit integer
    U64(u64),
    /// Signed 8-bit integer
    I8(i8),
    /// Signed 16-bit integer
    I16(i16),
    /// Signed 32-bit integer
    I32(i32),
    /// Signed 64-bit integer
    I64(i64),
    /// Unix timestamp: the number of seconds since 1 January 1970
    Timestamp(i64),
    /// Floating point 32-bit
    F32(f32),
    /// Floating point 64-bit
    F64(f64),
    /// Geographic Point: a pair of latitude and longitude coordinates and a distance unit (kilometers, miles)
    Point(Point, DistanceUnit),
    /// No value
    #[default]
    None,
}

/// Facet field, with field name and a map of unique values and their count (number of times the specific value appears in the whole index).
#[derive(Deserialize, Serialize, Debug, Clone, Default)]
pub struct FacetField {
    /// Facet field name
    pub name: String,
    /// Vector of facet value names and their count
    /// The number of distinct string values and numerical ranges per facet field (cardinality) is limited to 65_536.
    /// Once that number is reached, the facet field is not updated anymore (no new values are added, no existing values are counted).
    pub values: IndexMap<String, (Vec<String>, usize)>,

    /// Minimum value of the facet field
    pub min: ValueType,
    /// Maximum value of the facet field
    pub max: ValueType,

    #[serde(skip)]
    pub(crate) offset: usize,
    #[serde(skip)]
    pub(crate) field_type: FieldType,
}

/// Facet field, with field name and a vector of unique values and their count (number of times the specific value appears in the whole index).
/// Facet field: a vector of unique values and their count (number of times the specific value appears in the whole index).
pub type Facet = Vec<(String, usize)>;

/// Shard wrapped in Arc and RwLock for concurrent read and write access.
pub type ShardArc = Arc<RwLock<Shard>>;

/// Index wrapped in Arc and RwLock for concurrent read and write access.
pub type IndexArc = Arc<RwLock<Index>>;

/// The shard object of the index. It contains all levels and all segments of the index.
/// It also contains all properties that control indexing and intersection.
pub struct Shard {
    /// Incompatible index  format change: new library can't open old format, and old library can't open new format
    pub index_format_version_major: u16,
    /// Backward compatible format change: new library can open old format, but old library can't open new format
    pub index_format_version_minor: u16,

    /// Number of indexed documents
    pub(crate) indexed_doc_count: usize,
    /// Number of comitted documents
    pub(crate) committed_doc_count: usize,
    /// The index countains indexed, but uncommitted documents. Documents will either committed automatically once the number exceeds 64K documents, or once commit is invoked manually.
    pub(crate) uncommitted: bool,

    /// Defines a field in index schema: field, stored, indexed , field_type, facet, boost.
    pub schema_map: HashMap<String, SchemaField>,
    /// List of stored fields in the index: get_document and highlighter work only with stored fields
    pub stored_field_names: Vec<String>,
    /// Specifies SimilarityType, TokenizerType and AccessType when creating an new index
    pub meta: IndexMetaObject,

    pub(crate) is_last_level_incomplete: bool,
    pub(crate) last_level_index_file_start_pos: u64,
    pub(crate) last_level_docstore_file_start_pos: u64,

    /// Number of allowed parallel indexed documents (default=available_parallelism). Can be used to detect wehen all indexing processes are finished.
    pub(crate) permits: Arc<Semaphore>,

    pub(crate) docstore_file: File,
    pub(crate) docstore_file_mmap: Mmap,

    pub(crate) delete_file: File,
    pub(crate) delete_hashset: AHashSet<usize>,

    pub(crate) index_file: File,
    pub(crate) index_path_string: String,
    pub(crate) index_file_mmap: Mmap,

    pub(crate) compressed_index_segment_block_buffer: Vec<u8>,
    pub(crate) compressed_docstore_segment_block_buffer: Vec<u8>,

    pub(crate) segment_number1: usize,
    pub(crate) segment_number_bits1: usize,

    pub(crate) document_length_normalized_average: f32,
    pub(crate) positions_sum_normalized: u64,

    pub(crate) level_index: Vec<LevelIndex>,
    pub(crate) segments_index: Vec<SegmentIndex>,
    pub(crate) segments_level0: Vec<SegmentLevel0>,

    pub(crate) enable_fallback: bool,
    pub(crate) enable_single_term_topk: bool,
    pub(crate) enable_search_quality_test: bool,
    pub(crate) enable_inter_query_threading: bool,
    pub(crate) enable_inter_query_threading_auto: bool,

    pub(crate) segment_number_mask1: u32,

    pub(crate) indexed_field_vec: Vec<IndexedField>,
    pub(crate) indexed_field_id_bits: usize,
    pub(crate) indexed_field_id_mask: usize,
    pub(crate) longest_field_id: usize,
    pub(crate) longest_field_auto: bool,
    pub(crate) indexed_schema_vec: Vec<SchemaField>,

    pub(crate) document_length_compressed_array: Vec<[u8; ROARING_BLOCK_SIZE]>,
    pub(crate) key_count_sum: u64,

    pub(crate) block_id: usize,
    pub(crate) strip_compressed_sum: u64,
    pub(crate) postings_buffer: Vec<u8>,
    pub(crate) postings_buffer_pointer: usize,

    pub(crate) size_compressed_positions_index: u64,
    pub(crate) size_compressed_docid_index: u64,

    pub(crate) postinglist_count: usize,
    pub(crate) docid_count: usize,
    pub(crate) position_count: usize,

    pub(crate) mute: bool,
    pub(crate) frequentword_results: AHashMap<String, ResultObject>,

    pub(crate) facets: Vec<FacetField>,
    pub(crate) facets_map: AHashMap<String, usize>,
    pub(crate) facets_size_sum: usize,
    pub(crate) facets_file: File,
    pub(crate) facets_file_mmap: MmapMut,
    pub(crate) bm25_component_cache: [f32; 256],

    pub(crate) string_set_to_single_term_id_vec: Vec<AHashMap<String, AHashSet<u32>>>,

    pub(crate) synonyms_map: AHashMap<u64, SynonymItem>,

    #[cfg(feature = "zh")]
    pub(crate) word_segmentation_option: Option<WordSegmentationTM>,

    pub(crate) shard_number: usize,
    pub(crate) index_option: Option<Arc<RwLock<Index>>>,
    pub(crate) stemmer: Option<Stemmer>,

    pub(crate) stop_words: AHashSet<String>,
    pub(crate) frequent_words: Vec<String>,
    pub(crate) frequent_hashset: AHashSet<u64>,
    pub(crate) key_head_size: usize,
    pub(crate) level_terms: AHashMap<u32, String>,
    pub(crate) level_completions: Arc<RwLock<AHashMap<Vec<String>, usize>>>,
}

/// The root object of the index. It contains all levels and all segments of the index.
/// It also contains all properties that control indexing and intersection.
pub struct Index {
    /// Incompatible index  format change: new library can't open old format, and old library can't open new format
    pub index_format_version_major: u16,
    /// Backward compatible format change: new library can open old format, but old library can't open new format
    pub index_format_version_minor: u16,

    /// Number of indexed documents
    pub(crate) indexed_doc_count: usize,

    /// Number of deleted documents
    pub(crate) deleted_doc_count: usize,

    /// Defines a field in index schema: field, stored, indexed , field_type, facet, boost.
    pub schema_map: HashMap<String, SchemaField>,
    /// List of stored fields in the index: get_document and highlighter work only with stored fields
    pub stored_field_names: Vec<String>,
    /// Specifies SimilarityType, TokenizerType and AccessType when creating an new index
    pub meta: IndexMetaObject,

    pub(crate) index_file: File,
    pub(crate) index_path_string: String,

    pub(crate) compressed_index_segment_block_buffer: Vec<u8>,

    pub(crate) segment_number1: usize,
    pub(crate) segment_number_mask1: u32,

    pub(crate) indexed_field_vec: Vec<IndexedField>,

    pub(crate) mute: bool,

    pub(crate) facets: Vec<FacetField>,

    pub(crate) synonyms_map: AHashMap<u64, SynonymItem>,

    pub(crate) shard_number: usize,
    pub(crate) shard_bits: usize,
    pub(crate) shard_vec: Vec<Arc<RwLock<Shard>>>,
    pub(crate) shard_queue: Arc<RwLock<VecDeque<usize>>>,

    pub(crate) max_dictionary_entries: usize,
    pub(crate) symspell_option: Option<Arc<RwLock<SymSpell>>>,

    pub(crate) max_completion_entries: usize,
    pub(crate) completion_option: Option<Arc<RwLock<PruningRadixTrie>>>,

    pub(crate) frequent_hashset: AHashSet<u64>,
}

///SynonymItem is a vector of tuples: (synonym term, (64-bit synonym term hash, 64-bit synonym term hash))
pub type SynonymItem = Vec<(String, (u64, u32))>;

/// Get the version of the SeekStorm search library
pub fn version() -> &'static str {
    VERSION
}

pub(crate) fn get_synonyms_map(
    synonyms: &[Synonym],
    segment_number_mask1: u32,
) -> AHashMap<u64, SynonymItem> {
    let mut synonyms_map: AHashMap<u64, SynonymItem> = AHashMap::new();
    for synonym in synonyms.iter() {
        if synonym.terms.len() > 1 {
            let mut hashes: Vec<(String, (u64, u32))> = Vec::new();
            for term in synonym.terms.iter() {
                let term_bytes = term.to_lowercase();
                hashes.push((
                    term.to_string(),
                    (
                        hash64(term_bytes.as_bytes()),
                        hash32(term_bytes.as_bytes()) & segment_number_mask1,
                    ),
                ));
            }
            if synonym.multiway {
                for (i, hash) in hashes.iter().enumerate() {
                    let new_synonyms = if i == 0 {
                        hashes[1..].to_vec()
                    } else if i == hashes.len() - 1 {
                        hashes[..hashes.len() - 1].to_vec()
                    } else {
                        [&hashes[..i], &hashes[(i + 1)..]].concat()
                    };

                    if let Some(item) = synonyms_map.get_mut(&hash.1.0) {
                        *item = item
                            .clone()
                            .into_iter()
                            .chain(new_synonyms.into_iter())
                            .collect::<HashMap<String, (u64, u32)>>()
                            .into_iter()
                            .collect();
                    } else {
                        synonyms_map.insert(hash.1.0, new_synonyms);
                    }
                }
            } else {
                synonyms_map.insert(hashes[0].1.0, hashes[1..].to_vec());
            }
        }
    }
    synonyms_map
}

/// N-gram indexing: n-grams are indexed in addition to single terms, for faster phrase search, at the cost of higher index size
/// Setting valid both for index time and query time. Any change requires reindexing.
/// bitwise OR flags:
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq, FromPrimitive)]
pub enum NgramSet {
    /// no n-grams, only single terms are indexed
    SingleTerm = 0b00000000,
    /// Ngram frequent frequent
    NgramFF = 0b00000001,
    /// Ngram frequent rare
    NgramFR = 0b00000010,
    /// Ngram rare frequent
    NgramRF = 0b00000100,
    /// Ngram frequent frequent frequent
    NgramFFF = 0b00001000,
    /// Ngram rare frequent frequent
    NgramRFF = 0b00010000,
    /// Ngram frequent frequent rare
    NgramFFR = 0b00100000,
    /// Ngram frequent rare frequent
    NgramFRF = 0b01000000,
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq, FromPrimitive, Default)]
pub(crate) enum NgramType {
    /// no n-grams, only single terms are indexed
    #[default]
    SingleTerm = 0,
    /// Ngram frequent frequent
    NgramFF = 1,
    /// Ngram frequent rare
    NgramFR = 2,
    /// Ngram rare frequent
    NgramRF = 3,
    /// Ngram frequent frequent frequent
    NgramFFF = 4,
    /// Ngram rare frequent frequent
    NgramRFF = 5,
    /// Ngram frequent frequent rare
    NgramFFR = 6,
    /// Ngram frequent rare frequent
    NgramFRF = 7,
}

/// Create index in RAM.
/// Inner data structures for create index and open_index
/// * `index_path` - index path.  
/// * `meta` - index meta object.  
/// * `schema` - schema.  
/// * `synonyms` - vector of synonyms.
/// * `segment_number_bits1` - number of index segments: e.g. 11 bits for 2048 segments.  
/// * `mute` - prevent emitting status messages (e.g. when using pipes for data interprocess communication).  
/// * `force_shard_number` - set number of shards manually or automatically.
///   - none: number of shards is set automatically = number of physical processor cores (default)
///   - small: slower indexing, higher latency, slightly higher throughput, faster realtime search, lower RAM consumption
///   - large: faster indexing, lower latency, slightly lower throughput, slower realtime search, higher RAM consumption
pub async fn create_index(
    index_path: &Path,
    meta: IndexMetaObject,
    schema: &Vec<SchemaField>,
    synonyms: &Vec<Synonym>,
    segment_number_bits1: usize,
    mute: bool,
    force_shard_number: Option<usize>,
) -> Result<IndexArc, String> {
    create_index_root(
        index_path,
        meta,
        schema,
        true,
        synonyms,
        segment_number_bits1,
        mute,
        force_shard_number,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn create_index_root(
    index_path: &Path,
    meta: IndexMetaObject,
    schema: &Vec<SchemaField>,
    serialize_schema: bool,
    synonyms: &Vec<Synonym>,
    segment_number_bits1: usize,
    mute: bool,
    force_shard_number: Option<usize>,
) -> Result<IndexArc, String> {
    let frequent_hashset: AHashSet<u64> = match &meta.frequent_words {
        FrequentwordType::None => AHashSet::new(),
        FrequentwordType::English => FREQUENT_EN.lines().map(|x| hash64(x.as_bytes())).collect(),
        FrequentwordType::German => FREQUENT_EN.lines().map(|x| hash64(x.as_bytes())).collect(),
        FrequentwordType::French => FREQUENT_FR.lines().map(|x| hash64(x.as_bytes())).collect(),
        FrequentwordType::Spanish => FREQUENT_ES.lines().map(|x| hash64(x.as_bytes())).collect(),
        FrequentwordType::Custom { terms } => terms.iter().map(|x| hash64(x.as_bytes())).collect(),
    };

    let segment_number1 = 1usize << segment_number_bits1;
    let segment_number_mask1 = (1u32 << segment_number_bits1) - 1;

    let index_path_buf = index_path.to_path_buf();
    let index_path_string = index_path_buf.to_str().unwrap();

    if !index_path.exists() {
        if !mute {
            println!("index path created: {} ", index_path_string);
        }
        fs::create_dir_all(index_path).unwrap();
    }

    let file_path = Path::new(index_path_string).join(FILE_PATH);
    if !file_path.exists() {
        if !mute {
            println!("index directory created: {} ", file_path.display());
        }
        fs::create_dir_all(file_path).unwrap();
    }

    match File::options()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(Path::new(index_path).join(INDEX_FILENAME))
    {
        Ok(index_file) => {
            let mut document_length_compressed_array: Vec<[u8; ROARING_BLOCK_SIZE]> = Vec::new();
            let mut indexed_field_vec: Vec<IndexedField> = Vec::new();
            let mut facets_vec: Vec<FacetField> = Vec::new();
            let mut facets_map: AHashMap<String, usize> = AHashMap::new();

            let mut schema_map: HashMap<String, SchemaField> = HashMap::new();
            let mut indexed_schema_vec: Vec<SchemaField> = Vec::new();
            let mut stored_field_names = Vec::new();
            let mut facets_size_sum = 0;
            let mut longest_field_id_option: Option<usize> = None;
            for (i, schema_field) in schema.iter().enumerate() {
                let mut schema_field_clone = schema_field.clone();
                schema_field_clone.indexed_field_id = indexed_field_vec.len();
                if schema_field.longest && schema_field.indexed {
                    longest_field_id_option = Some(schema_field_clone.indexed_field_id);
                }

                schema_field_clone.field_id = i;
                schema_map.insert(schema_field.field.clone(), schema_field_clone.clone());

                if schema_field.facet {
                    let facet_size = match schema_field.field_type {
                        FieldType::U8 => 1,
                        FieldType::U16 => 2,
                        FieldType::U32 => 4,
                        FieldType::U64 => 8,
                        FieldType::I8 => 1,
                        FieldType::I16 => 2,
                        FieldType::I32 => 4,
                        FieldType::I64 => 8,
                        FieldType::Timestamp => 8,
                        FieldType::F32 => 4,
                        FieldType::F64 => 8,
                        FieldType::String16 => 2,
                        FieldType::String32 => 4,
                        FieldType::StringSet16 => 2,
                        FieldType::StringSet32 => 4,
                        FieldType::Point => 8,
                        _ => 1,
                    };

                    facets_map.insert(schema_field.field.clone(), facets_vec.len());
                    facets_vec.push(FacetField {
                        name: schema_field.field.clone(),
                        values: IndexMap::new(),
                        min: ValueType::None,
                        max: ValueType::None,
                        offset: facets_size_sum,
                        field_type: schema_field.field_type.clone(),
                    });
                    facets_size_sum += facet_size;
                }

                if schema_field.indexed {
                    indexed_field_vec.push(IndexedField {
                        schema_field_name: schema_field.field.clone(),
                        is_longest_field: false,
                        field_length_sum: 0,
                        indexed_field_id: indexed_field_vec.len(),
                    });
                    indexed_schema_vec.push(schema_field_clone);
                    document_length_compressed_array.push([0; ROARING_BLOCK_SIZE]);
                }

                if schema_field.stored {
                    stored_field_names.push(schema_field.field.clone());
                }
            }

            if !facets_vec.is_empty()
                && let Ok(file) = File::open(Path::new(index_path).join(FACET_VALUES_FILENAME))
                && let Ok(facets) = serde_json::from_reader(BufReader::new(file))
            {
                let mut facets: Vec<FacetField> = facets;
                if facets_vec.len() == facets.len() {
                    for i in 0..facets.len() {
                        facets[i].offset = facets_vec[i].offset;
                        facets[i].field_type = facets_vec[i].field_type.clone();
                    }
                }
                facets_vec = facets;
            }

            let synonyms_map = get_synonyms_map(synonyms, segment_number_mask1);

            let shard_number = if let Some(shard_number) = force_shard_number {
                shard_number
            } else {
                num_cpus::get_physical()
            };
            let shard_bits = if serialize_schema {
                (usize::BITS - (shard_number - 1).leading_zeros()) as usize
            } else {
                0
            };

            let mut shard_vec: Vec<Arc<RwLock<Shard>>> = Vec::new();
            let mut shard_queue = VecDeque::<usize>::new();
            if serialize_schema {
                let mut result_object_list = Vec::new();
                let index_path_clone = Arc::new(index_path.to_path_buf());
                for i in 0..shard_number {
                    let index_path_clone2 = index_path_clone.clone();
                    let meta_clone = meta.clone();
                    let schema_clone = schema.clone();
                    result_object_list.push(tokio::spawn(async move {
                        let shard_path = index_path_clone2.join("shards").join(i.to_string());
                        let mut shard_meta = meta_clone.clone();
                        shard_meta.id = i as u64;
                        let mut shard = create_shard(
                            &shard_path,
                            &shard_meta,
                            &schema_clone,
                            serialize_schema,
                            &Vec::new(),
                            segment_number_bits1,
                            mute,
                            longest_field_id_option,
                        )
                        .unwrap();
                        shard.shard_number = shard_number;
                        let shard_arc = Arc::new(RwLock::new(shard));
                        (shard_arc, i)
                    }));
                }
                for result_object_shard in result_object_list {
                    let ro_shard = result_object_shard.await.unwrap();
                    shard_vec.push(ro_shard.0);
                    shard_queue.push_back(ro_shard.1);
                }
            }

            let shard_queue_arc = Arc::new(RwLock::new(shard_queue));

            let mut index = Index {
                index_format_version_major: INDEX_FORMAT_VERSION_MAJOR,
                index_format_version_minor: INDEX_FORMAT_VERSION_MINOR,

                index_file,
                index_path_string: index_path_string.to_owned(),
                stored_field_names,

                compressed_index_segment_block_buffer: vec![0; 10_000_000],
                indexed_doc_count: 0,
                deleted_doc_count: 0,
                segment_number1: 0,
                segment_number_mask1: 0,
                schema_map,
                indexed_field_vec,
                meta: meta.clone(),
                mute,
                facets: facets_vec,
                synonyms_map,

                shard_number,
                shard_bits,
                shard_vec,
                shard_queue: shard_queue_arc,

                max_dictionary_entries: if let Some(spelling_correction) = &meta.spelling_correction
                {
                    spelling_correction.max_dictionary_entries
                } else {
                    usize::MAX
                },

                symspell_option: if let Some(spelling_correction) = meta.spelling_correction {
                    Some(Arc::new(RwLock::new(SymSpell::new(
                        spelling_correction.max_dictionary_edit_distance,
                        spelling_correction.term_length_threshold,
                        7,
                        spelling_correction.count_threshold,
                    ))))
                } else {
                    None
                },

                max_completion_entries: if let Some(query_completion) = &meta.query_completion {
                    query_completion.max_completion_entries
                } else {
                    usize::MAX
                },

                completion_option: meta
                    .query_completion
                    .as_ref()
                    .map(|_query_completion| Arc::new(RwLock::new(PruningRadixTrie::new()))),

                frequent_hashset,
            };

            let file_len = index.index_file.metadata().unwrap().len();
            if file_len == 0 {
                write_u16(
                    INDEX_FORMAT_VERSION_MAJOR,
                    &mut index.compressed_index_segment_block_buffer,
                    0,
                );
                write_u16(
                    INDEX_FORMAT_VERSION_MINOR,
                    &mut index.compressed_index_segment_block_buffer,
                    2,
                );
                let _ = index.index_file.write(
                    &index.compressed_index_segment_block_buffer[0..INDEX_HEADER_SIZE as usize],
                );
            } else {
                let _ = index.index_file.read(
                    &mut index.compressed_index_segment_block_buffer[0..INDEX_HEADER_SIZE as usize],
                );
                index.index_format_version_major =
                    read_u16(&index.compressed_index_segment_block_buffer, 0);
                index.index_format_version_minor =
                    read_u16(&index.compressed_index_segment_block_buffer, 2);

                if INDEX_FORMAT_VERSION_MAJOR != index.index_format_version_major {
                    return Err("incompatible index format version ".to_string()
                        + &INDEX_FORMAT_VERSION_MAJOR.to_string()
                        + " "
                        + &index.index_format_version_major.to_string());
                };
            }

            index.segment_number1 = segment_number1;
            index.segment_number_mask1 = segment_number_mask1;

            if serialize_schema {
                serde_json::to_writer(
                    &File::create(Path::new(index_path).join(SCHEMA_FILENAME)).unwrap(),
                    &schema,
                )
                .unwrap();

                if !synonyms.is_empty() {
                    serde_json::to_writer(
                        &File::create(Path::new(index_path).join(SYNONYMS_FILENAME)).unwrap(),
                        &synonyms,
                    )
                    .unwrap();
                }

                serde_json::to_writer(
                    &File::create(Path::new(index_path).join(META_FILENAME)).unwrap(),
                    &index.meta,
                )
                .unwrap();
            }

            let index_arc = Arc::new(RwLock::new(index));

            if serialize_schema {
                for shard in index_arc.write().await.shard_vec.iter() {
                    shard.write().await.index_option = Some(index_arc.clone());
                }
            }

            Ok(index_arc)
        }
        Err(e) => {
            println!("file opening error");
            Err(e.to_string())
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn create_shard(
    index_path: &Path,
    meta: &IndexMetaObject,
    schema: &Vec<SchemaField>,
    serialize_schema: bool,
    synonyms: &Vec<Synonym>,
    segment_number_bits1: usize,
    mute: bool,
    longest_field_id_option: Option<usize>,
) -> Result<Shard, String> {
    let segment_number1 = 1usize << segment_number_bits1;
    let segment_number_mask1 = (1u32 << segment_number_bits1) - 1;

    let index_path_buf = index_path.to_path_buf();
    let index_path_string = index_path_buf.to_str().unwrap();

    if !index_path.exists() {
        fs::create_dir_all(index_path).unwrap();
    }

    let file_path = Path::new(index_path_string).join(FILE_PATH);
    if !file_path.exists() {
        fs::create_dir_all(file_path).unwrap();
    }

    match File::options()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(Path::new(index_path).join(INDEX_FILENAME))
    {
        Ok(index_file) => {
            let docstore_file = File::options()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(Path::new(index_path).join(DOCSTORE_FILENAME))
                .unwrap();

            let delete_file = File::options()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(Path::new(index_path).join(DELETE_FILENAME))
                .unwrap();

            let facets_file = File::options()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(Path::new(index_path).join(FACET_FILENAME))
                .unwrap();

            let mut document_length_compressed_array: Vec<[u8; ROARING_BLOCK_SIZE]> = Vec::new();
            let mut indexed_field_vec: Vec<IndexedField> = Vec::new();
            let mut facets_vec: Vec<FacetField> = Vec::new();
            let mut facets_map: AHashMap<String, usize> = AHashMap::new();

            let mut schema_map: HashMap<String, SchemaField> = HashMap::new();
            let mut indexed_schema_vec: Vec<SchemaField> = Vec::new();
            let mut stored_fields_flag = false;
            let mut stored_field_names = Vec::new();
            let mut facets_size_sum = 0;
            for (i, schema_field) in schema.iter().enumerate() {
                let mut schema_field_clone = schema_field.clone();
                schema_field_clone.indexed_field_id = indexed_field_vec.len();
                schema_field_clone.field_id = i;
                schema_map.insert(schema_field.field.clone(), schema_field_clone.clone());

                if schema_field.facet {
                    let facet_size = match schema_field.field_type {
                        FieldType::U8 => 1,
                        FieldType::U16 => 2,
                        FieldType::U32 => 4,
                        FieldType::U64 => 8,
                        FieldType::I8 => 1,
                        FieldType::I16 => 2,
                        FieldType::I32 => 4,
                        FieldType::I64 => 8,
                        FieldType::Timestamp => 8,
                        FieldType::F32 => 4,
                        FieldType::F64 => 8,
                        FieldType::String16 => 2,
                        FieldType::String32 => 4,
                        FieldType::StringSet16 => 2,
                        FieldType::StringSet32 => 4,
                        FieldType::Point => 8,
                        _ => 1,
                    };

                    facets_map.insert(schema_field.field.clone(), facets_vec.len());
                    facets_vec.push(FacetField {
                        name: schema_field.field.clone(),
                        values: IndexMap::new(),
                        min: ValueType::None,
                        max: ValueType::None,
                        offset: facets_size_sum,
                        field_type: schema_field.field_type.clone(),
                    });
                    facets_size_sum += facet_size;
                }

                if schema_field.indexed {
                    indexed_field_vec.push(IndexedField {
                        schema_field_name: schema_field.field.clone(),
                        is_longest_field: false,
                        field_length_sum: 0,
                        indexed_field_id: indexed_field_vec.len(),
                    });
                    indexed_schema_vec.push(schema_field_clone);
                    document_length_compressed_array.push([0; ROARING_BLOCK_SIZE]);
                }

                if schema_field.stored {
                    stored_fields_flag = true;
                    stored_field_names.push(schema_field.field.clone());
                }
            }

            let indexed_field_id_bits =
                (usize::BITS - (indexed_field_vec.len() - 1).leading_zeros()) as usize;

            let index_file_mmap;
            let docstore_file_mmap = if meta.access_type == AccessType::Mmap {
                index_file_mmap = unsafe { Mmap::map(&index_file).expect("Unable to create Mmap") };
                unsafe { Mmap::map(&docstore_file).expect("Unable to create Mmap") }
            } else {
                index_file_mmap = unsafe {
                    MmapOptions::new()
                        .len(0)
                        .map(&index_file)
                        .expect("Unable to create Mmap")
                };
                unsafe {
                    MmapOptions::new()
                        .len(0)
                        .map(&docstore_file)
                        .expect("Unable to create Mmap")
                }
            };

            if !facets_vec.is_empty()
                && let Ok(file) = File::open(Path::new(index_path).join(FACET_VALUES_FILENAME))
                && let Ok(facets) = serde_json::from_reader(BufReader::new(file))
            {
                let mut facets: Vec<FacetField> = facets;
                if facets_vec.len() == facets.len() {
                    for i in 0..facets.len() {
                        facets[i].offset = facets_vec[i].offset;
                        facets[i].field_type = facets_vec[i].field_type.clone();
                    }
                }
                facets_vec = facets;
            }

            let facets_file_mmap = if !facets_vec.is_empty() {
                if facets_file.metadata().unwrap().len() == 0 {
                    facets_file
                        .set_len((facets_size_sum * ROARING_BLOCK_SIZE) as u64)
                        .expect("Unable to set len");
                }

                unsafe { MmapMut::map_mut(&facets_file).expect("Unable to create Mmap") }
            } else {
                unsafe { MmapMut::map_mut(&facets_file).expect("Unable to create Mmap") }
            };

            let synonyms_map = get_synonyms_map(synonyms, segment_number_mask1);

            let facets_len = facets_vec.len();

            #[cfg(feature = "zh")]
            let word_segmentation_option = if meta.tokenizer == TokenizerType::UnicodeAlphanumericZH
            {
                let mut word_segmentation = WordSegmentationTM::new();
                word_segmentation.load_dictionary(0, 1, true);
                Some(word_segmentation)
            } else {
                None
            };

            let shard_number = 1;

            let stemmer = match meta.stemmer {
                StemmerType::Arabic => Some(Stemmer::create(Algorithm::Arabic)),
                StemmerType::Danish => Some(Stemmer::create(Algorithm::Danish)),
                StemmerType::Dutch => Some(Stemmer::create(Algorithm::Dutch)),
                StemmerType::English => Some(Stemmer::create(Algorithm::English)),
                StemmerType::Finnish => Some(Stemmer::create(Algorithm::Finnish)),
                StemmerType::French => Some(Stemmer::create(Algorithm::French)),
                StemmerType::German => Some(Stemmer::create(Algorithm::German)),
                StemmerType::Greek => Some(Stemmer::create(Algorithm::Greek)),
                StemmerType::Hungarian => Some(Stemmer::create(Algorithm::Hungarian)),
                StemmerType::Italian => Some(Stemmer::create(Algorithm::Italian)),
                StemmerType::Norwegian => Some(Stemmer::create(Algorithm::Norwegian)),
                StemmerType::Portuguese => Some(Stemmer::create(Algorithm::Portuguese)),
                StemmerType::Romanian => Some(Stemmer::create(Algorithm::Romanian)),
                StemmerType::Russian => Some(Stemmer::create(Algorithm::Russian)),
                StemmerType::Spanish => Some(Stemmer::create(Algorithm::Spanish)),
                StemmerType::Swedish => Some(Stemmer::create(Algorithm::Swedish)),
                StemmerType::Tamil => Some(Stemmer::create(Algorithm::Tamil)),
                StemmerType::Turkish => Some(Stemmer::create(Algorithm::Turkish)),
                _ => None,
            };

            let stop_words: AHashSet<String> = match &meta.stop_words {
                StopwordType::None => AHashSet::new(),
                StopwordType::English => FREQUENT_EN.lines().map(|x| x.to_string()).collect(),
                StopwordType::German => FREQUENT_DE.lines().map(|x| x.to_string()).collect(),
                StopwordType::French => FREQUENT_FR.lines().map(|x| x.to_string()).collect(),
                StopwordType::Spanish => FREQUENT_ES.lines().map(|x| x.to_string()).collect(),
                StopwordType::Custom { terms } => terms.iter().map(|x| x.to_string()).collect(),
            };

            let frequent_words: Vec<String> = match &meta.frequent_words {
                FrequentwordType::None => Vec::new(),
                FrequentwordType::English => {
                    let mut words: Vec<String> =
                        FREQUENT_EN.lines().map(|x| x.to_string()).collect();
                    words.sort_unstable();
                    words
                }
                FrequentwordType::German => {
                    let mut words: Vec<String> =
                        FREQUENT_DE.lines().map(|x| x.to_string()).collect();
                    words.sort_unstable();
                    words
                }
                FrequentwordType::French => {
                    let mut words: Vec<String> =
                        FREQUENT_FR.lines().map(|x| x.to_string()).collect();
                    words.sort_unstable();
                    words
                }
                FrequentwordType::Spanish => {
                    let mut words: Vec<String> =
                        FREQUENT_ES.lines().map(|x| x.to_string()).collect();
                    words.sort_unstable();
                    words
                }
                FrequentwordType::Custom { terms } => {
                    let mut words: Vec<String> = terms.iter().map(|x| x.to_string()).collect();
                    words.sort_unstable();
                    words
                }
            };

            let frequent_hashset: AHashSet<u64> = frequent_words
                .iter()
                .map(|x| hash64(x.as_bytes()))
                .collect();

            let mut index = Shard {
                index_format_version_major: INDEX_FORMAT_VERSION_MAJOR,
                index_format_version_minor: INDEX_FORMAT_VERSION_MINOR,
                docstore_file,
                delete_file,
                delete_hashset: AHashSet::new(),
                index_file,
                index_path_string: index_path_string.to_owned(),
                index_file_mmap,
                docstore_file_mmap,
                stored_field_names,
                compressed_index_segment_block_buffer: vec![0; 10_000_000],
                compressed_docstore_segment_block_buffer: if stored_fields_flag {
                    vec![0; ROARING_BLOCK_SIZE * 4]
                } else {
                    Vec::new()
                },
                document_length_normalized_average: 0.0,
                indexed_doc_count: 0,
                committed_doc_count: 0,
                is_last_level_incomplete: false,
                last_level_index_file_start_pos: 0,
                last_level_docstore_file_start_pos: 0,
                positions_sum_normalized: 0,
                segment_number1: 0,
                segment_number_bits1,
                segment_number_mask1: 0,
                level_index: Vec::new(),
                segments_index: Vec::new(),
                segments_level0: Vec::new(),
                uncommitted: false,
                enable_fallback: false,
                enable_single_term_topk: false,
                enable_search_quality_test: false,
                enable_inter_query_threading: false,
                enable_inter_query_threading_auto: false,
                schema_map,
                indexed_field_id_bits,
                indexed_field_id_mask: (1usize << indexed_field_id_bits) - 1,
                longest_field_id: longest_field_id_option.unwrap_or_default(),
                longest_field_auto: longest_field_id_option.is_none(),
                indexed_field_vec,
                indexed_schema_vec,
                meta: meta.clone(),
                document_length_compressed_array,
                key_count_sum: 0,

                block_id: 0,
                strip_compressed_sum: 0,
                postings_buffer: vec![0; POSTING_BUFFER_SIZE],
                postings_buffer_pointer: 0,

                docid_count: 0,
                size_compressed_docid_index: 0,
                size_compressed_positions_index: 0,
                position_count: 0,
                postinglist_count: 0,
                permits: Arc::new(Semaphore::new(1)),
                mute,
                frequentword_results: AHashMap::new(),
                facets: facets_vec,
                facets_map,
                facets_size_sum,
                facets_file,
                facets_file_mmap,
                string_set_to_single_term_id_vec: vec![AHashMap::new(); facets_len],
                bm25_component_cache: [0.0; 256],
                synonyms_map,
                #[cfg(feature = "zh")]
                word_segmentation_option,

                shard_number,
                index_option: None,
                stemmer,
                stop_words,
                frequent_words,
                frequent_hashset,
                key_head_size: if meta.ngram_indexing == 0 {
                    20
                } else if meta.ngram_indexing < 8 {
                    22
                } else {
                    23
                },
                level_terms: AHashMap::new(),
                level_completions: Arc::new(RwLock::new(AHashMap::with_capacity(200_000))),
            };

            let file_len = index.index_file.metadata().unwrap().len();
            if file_len == 0 {
                write_u16(
                    INDEX_FORMAT_VERSION_MAJOR,
                    &mut index.compressed_index_segment_block_buffer,
                    0,
                );
                write_u16(
                    INDEX_FORMAT_VERSION_MINOR,
                    &mut index.compressed_index_segment_block_buffer,
                    2,
                );
                let _ = index.index_file.write(
                    &index.compressed_index_segment_block_buffer[0..INDEX_HEADER_SIZE as usize],
                );
            } else {
                let _ = index.index_file.read(
                    &mut index.compressed_index_segment_block_buffer[0..INDEX_HEADER_SIZE as usize],
                );
                index.index_format_version_major =
                    read_u16(&index.compressed_index_segment_block_buffer, 0);
                index.index_format_version_minor =
                    read_u16(&index.compressed_index_segment_block_buffer, 2);

                if INDEX_FORMAT_VERSION_MAJOR != index.index_format_version_major {
                    return Err("incompatible index format version ".to_string()
                        + &INDEX_FORMAT_VERSION_MAJOR.to_string()
                        + " "
                        + &index.index_format_version_major.to_string());
                };
            }

            index.segment_number1 = segment_number1;
            index.segment_number_mask1 = segment_number_mask1;
            index.segments_level0 = vec![
                SegmentLevel0 {
                    segment: AHashMap::with_capacity(SEGMENT_KEY_CAPACITY),
                    ..Default::default()
                };
                index.segment_number1
            ];

            index.segments_index = Vec::new();
            for _i in 0..index.segment_number1 {
                index.segments_index.push(SegmentIndex {
                    byte_array_blocks: Vec::new(),
                    byte_array_blocks_pointer: Vec::new(),
                    segment: AHashMap::new(),
                });
            }

            if serialize_schema {
                serde_json::to_writer(
                    &File::create(Path::new(index_path).join(SCHEMA_FILENAME)).unwrap(),
                    &schema,
                )
                .unwrap();

                if !synonyms.is_empty() {
                    serde_json::to_writer(
                        &File::create(Path::new(index_path).join(SYNONYMS_FILENAME)).unwrap(),
                        &synonyms,
                    )
                    .unwrap();
                }

                serde_json::to_writer(
                    &File::create(Path::new(index_path).join(META_FILENAME)).unwrap(),
                    &index.meta,
                )
                .unwrap();
            }

            Ok(index)
        }
        Err(e) => {
            println!("file opening error");
            Err(e.to_string())
        }
    }
}

#[inline(always)]
pub(crate) fn get_document_length_compressed_mmap(
    index: &Shard,
    field_id: usize,
    block_id: usize,
    doc_id_block: usize,
) -> u8 {
    index.index_file_mmap[index.level_index[block_id].document_length_compressed_array_pointer
        + (field_id << 16)
        + doc_id_block]
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn get_max_score(
    index: &Shard,
    segment: &SegmentIndex,
    posting_count_ngram_1: u32,
    posting_count_ngram_2: u32,
    posting_count_ngram_3: u32,
    posting_count: u32,
    block_id: usize,
    max_docid: usize,
    max_p_docid: usize,
    pointer_pivot_p_docid: usize,
    compression_type_pointer: u32,
    ngram_type: &NgramType,
) -> f32 {
    let byte_array = if index.meta.access_type == AccessType::Mmap {
        &index.index_file_mmap[segment.byte_array_blocks_pointer[block_id].0
            ..segment.byte_array_blocks_pointer[block_id].0
                + segment.byte_array_blocks_pointer[block_id].1]
    } else {
        &segment.byte_array_blocks[block_id]
    };

    let mut bm25f = 0.0;

    let rank_position_pointer_range: u32 =
        compression_type_pointer & 0b0011_1111_1111_1111_1111_1111_1111_1111;

    let posting_pointer_size_sum;
    let rank_position_pointer;
    let posting_pointer_size;
    let embed_flag;
    if max_p_docid < pointer_pivot_p_docid {
        posting_pointer_size_sum = max_p_docid as u32 * 2;
        rank_position_pointer = read_u16(
            byte_array,
            rank_position_pointer_range as usize + posting_pointer_size_sum as usize,
        ) as u32;
        posting_pointer_size = 2;
        embed_flag = (rank_position_pointer & 0b10000000_00000000) != 0;
    } else {
        posting_pointer_size_sum = (max_p_docid as u32) * 3 - pointer_pivot_p_docid as u32;
        rank_position_pointer = read_u32(
            byte_array,
            rank_position_pointer_range as usize + posting_pointer_size_sum as usize,
        );
        posting_pointer_size = 3;
        embed_flag = (rank_position_pointer & 0b10000000_00000000_00000000) != 0;
    };

    let positions_pointer = if embed_flag {
        rank_position_pointer_range as usize + posting_pointer_size_sum as usize
    } else {
        let pointer_value = if posting_pointer_size == 2 {
            rank_position_pointer & 0b01111111_11111111
        } else {
            rank_position_pointer & 0b01111111_11111111_11111111
        } as usize;

        rank_position_pointer_range as usize - pointer_value
    };

    let mut field_vec: SmallVec<[(u16, usize); 2]> = SmallVec::new();
    let mut field_vec_ngram1 = SmallVec::new();
    let mut field_vec_ngram2 = SmallVec::new();
    let mut field_vec_ngram3 = SmallVec::new();

    decode_positions_commit(
        posting_pointer_size,
        embed_flag,
        byte_array,
        positions_pointer,
        ngram_type,
        index.indexed_field_vec.len(),
        index.indexed_field_id_bits,
        index.indexed_field_id_mask,
        index.longest_field_id as u16,
        &mut field_vec,
        &mut field_vec_ngram1,
        &mut field_vec_ngram2,
        &mut field_vec_ngram3,
    );

    if ngram_type == &NgramType::SingleTerm
        || index.meta.similarity == SimilarityType::Bm25fProximity
    {
        let idf = (((index.indexed_doc_count as f32 - posting_count as f32 + 0.5)
            / (posting_count as f32 + 0.5))
            + 1.0)
            .ln();

        for field in field_vec.iter() {
            let document_length_normalized = DOCUMENT_LENGTH_COMPRESSION[if index.meta.access_type
                == AccessType::Mmap
            {
                get_document_length_compressed_mmap(index, field.0 as usize, block_id, max_docid)
            } else {
                index.level_index[block_id].document_length_compressed_array[field.0 as usize]
                    [max_docid]
            } as usize] as f32;

            let document_length_quotient =
                document_length_normalized / index.document_length_normalized_average;

            let tf = field.1 as f32;

            let weight = index.indexed_schema_vec[field.0 as usize].boost;

            bm25f += weight
                * idf
                * ((tf * (K + 1.0) / (tf + (K * (1.0 - B + (B * document_length_quotient)))))
                    + SIGMA);
        }
    } else if ngram_type == &NgramType::NgramFF
        || ngram_type == &NgramType::NgramFR
        || ngram_type == &NgramType::NgramRF
    {
        let idf_ngram1 = (((index.indexed_doc_count as f32 - posting_count_ngram_1 as f32 + 0.5)
            / (posting_count_ngram_1 as f32 + 0.5))
            + 1.0)
            .ln();

        let idf_ngram2 = (((index.indexed_doc_count as f32 - posting_count_ngram_2 as f32 + 0.5)
            / (posting_count_ngram_2 as f32 + 0.5))
            + 1.0)
            .ln();

        for field in field_vec_ngram1.iter() {
            let document_length_normalized = DOCUMENT_LENGTH_COMPRESSION[if index.meta.access_type
                == AccessType::Mmap
            {
                get_document_length_compressed_mmap(index, field.0 as usize, block_id, max_docid)
            } else {
                index.level_index[block_id].document_length_compressed_array[field.0 as usize]
                    [max_docid]
            } as usize] as f32;

            let document_length_quotient =
                document_length_normalized / index.document_length_normalized_average;

            let tf_ngram1 = field.1 as f32;

            let weight = index.indexed_schema_vec[field.0 as usize].boost;

            bm25f += weight
                * idf_ngram1
                * ((tf_ngram1 * (K + 1.0)
                    / (tf_ngram1 + (K * (1.0 - B + (B * document_length_quotient)))))
                    + SIGMA);
        }

        for field in field_vec_ngram2.iter() {
            let document_length_normalized = DOCUMENT_LENGTH_COMPRESSION[if index.meta.access_type
                == AccessType::Mmap
            {
                get_document_length_compressed_mmap(index, field.0 as usize, block_id, max_docid)
            } else {
                index.level_index[block_id].document_length_compressed_array[field.0 as usize]
                    [max_docid]
            } as usize] as f32;

            let document_length_quotient =
                document_length_normalized / index.document_length_normalized_average;

            let tf_ngram2 = field.1 as f32;

            let weight = index.indexed_schema_vec[field.0 as usize].boost;

            bm25f += weight
                * idf_ngram2
                * ((tf_ngram2 * (K + 1.0)
                    / (tf_ngram2 + (K * (1.0 - B + (B * document_length_quotient)))))
                    + SIGMA);
        }
    } else {
        let idf_ngram1 = (((index.indexed_doc_count as f32 - posting_count_ngram_1 as f32 + 0.5)
            / (posting_count_ngram_1 as f32 + 0.5))
            + 1.0)
            .ln();

        let idf_ngram2 = (((index.indexed_doc_count as f32 - posting_count_ngram_2 as f32 + 0.5)
            / (posting_count_ngram_2 as f32 + 0.5))
            + 1.0)
            .ln();

        let idf_ngram3 = (((index.indexed_doc_count as f32 - posting_count_ngram_3 as f32 + 0.5)
            / (posting_count_ngram_3 as f32 + 0.5))
            + 1.0)
            .ln();

        for field in field_vec_ngram1.iter() {
            let document_length_normalized = DOCUMENT_LENGTH_COMPRESSION[if index.meta.access_type
                == AccessType::Mmap
            {
                get_document_length_compressed_mmap(index, field.0 as usize, block_id, max_docid)
            } else {
                index.level_index[block_id].document_length_compressed_array[field.0 as usize]
                    [max_docid]
            } as usize] as f32;

            let document_length_quotient =
                document_length_normalized / index.document_length_normalized_average;

            let tf_ngram1 = field.1 as f32;

            let weight = index.indexed_schema_vec[field.0 as usize].boost;

            bm25f += weight
                * idf_ngram1
                * ((tf_ngram1 * (K + 1.0)
                    / (tf_ngram1 + (K * (1.0 - B + (B * document_length_quotient)))))
                    + SIGMA);
        }

        for field in field_vec_ngram2.iter() {
            let document_length_normalized = DOCUMENT_LENGTH_COMPRESSION[if index.meta.access_type
                == AccessType::Mmap
            {
                get_document_length_compressed_mmap(index, field.0 as usize, block_id, max_docid)
            } else {
                index.level_index[block_id].document_length_compressed_array[field.0 as usize]
                    [max_docid]
            } as usize] as f32;

            let document_length_quotient =
                document_length_normalized / index.document_length_normalized_average;

            let tf_ngram2 = field.1 as f32;

            let weight = index.indexed_schema_vec[field.0 as usize].boost;

            bm25f += weight
                * idf_ngram2
                * ((tf_ngram2 * (K + 1.0)
                    / (tf_ngram2 + (K * (1.0 - B + (B * document_length_quotient)))))
                    + SIGMA);
        }

        for field in field_vec_ngram3.iter() {
            let document_length_normalized = DOCUMENT_LENGTH_COMPRESSION[if index.meta.access_type
                == AccessType::Mmap
            {
                get_document_length_compressed_mmap(index, field.0 as usize, block_id, max_docid)
            } else {
                index.level_index[block_id].document_length_compressed_array[field.0 as usize]
                    [max_docid]
            } as usize] as f32;

            let document_length_quotient =
                document_length_normalized / index.document_length_normalized_average;

            let tf_ngram3 = field.1 as f32;

            let weight = index.indexed_schema_vec[field.0 as usize].boost;

            bm25f += weight
                * idf_ngram3
                * ((tf_ngram3 * (K + 1.0)
                    / (tf_ngram3 + (K * (1.0 - B + (B * document_length_quotient)))))
                    + SIGMA);
        }
    }
    bm25f
}

pub(crate) fn update_list_max_impact_score(index: &mut Shard) {
    if index.meta.access_type == AccessType::Mmap {
        return;
    }

    for key0 in 0..index.segment_number1 {
        let keys: Vec<u64> = index.segments_index[key0].segment.keys().cloned().collect();
        for key in keys {
            let ngram_type = FromPrimitive::from_u64(key & 0b111).unwrap_or(NgramType::SingleTerm);

            let blocks_len = index.segments_index[key0].segment[&key].blocks.len();
            let mut max_list_score = 0.0;
            for block_index in 0..blocks_len {
                let segment = &index.segments_index[key0];
                let posting_list = &segment.segment[&key];
                let block = &posting_list.blocks[block_index];
                let max_block_score = get_max_score(
                    index,
                    segment,
                    posting_list.posting_count_ngram_1,
                    posting_list.posting_count_ngram_2,
                    posting_list.posting_count_ngram_3,
                    posting_list.posting_count,
                    block.block_id as usize,
                    block.max_docid as usize,
                    block.max_p_docid as usize,
                    block.pointer_pivot_p_docid as usize,
                    block.compression_type_pointer,
                    &ngram_type,
                );

                index.segments_index[key0]
                    .segment
                    .get_mut(&key)
                    .unwrap()
                    .blocks[block_index]
                    .max_block_score = max_block_score;
                max_list_score = f32::max(max_list_score, max_block_score);
            }
            index.segments_index[key0]
                .segment
                .get_mut(&key)
                .unwrap()
                .max_list_score = max_list_score;
        }
    }
}

/// Loads the index from disk into RAM or MMAP.
/// * `index_path` - index path.  
/// * `mute` - prevent emitting status messages (e.g. when using pipes for data interprocess communication).  
pub(crate) async fn open_shard(index_path: &Path, mute: bool) -> Result<ShardArc, String> {
    if !mute {
        println!("opening index ...");
    }

    let mut index_mmap_position = INDEX_HEADER_SIZE as usize;
    let mut docstore_mmap_position = 0;

    match File::open(Path::new(index_path).join(META_FILENAME)) {
        Ok(meta_file) => {
            let meta: IndexMetaObject = serde_json::from_reader(BufReader::new(meta_file)).unwrap();

            match File::open(Path::new(index_path).join(SCHEMA_FILENAME)) {
                Ok(schema_file) => {
                    let schema = serde_json::from_reader(BufReader::new(schema_file)).unwrap();

                    let synonyms = if let Ok(synonym_file) =
                        File::open(Path::new(index_path).join(SYNONYMS_FILENAME))
                    {
                        serde_json::from_reader(BufReader::new(synonym_file)).unwrap_or_default()
                    } else {
                        Vec::new()
                    };

                    match create_shard(index_path, &meta, &schema, false, &synonyms, 11, mute, None)
                    {
                        Ok(mut shard) => {
                            let mut block_count_sum = 0;

                            let is_mmap = shard.meta.access_type == AccessType::Mmap;

                            let file_len = if is_mmap {
                                shard.index_file_mmap.len() as u64
                            } else {
                                shard.index_file.metadata().unwrap().len()
                            };

                            while if is_mmap {
                                index_mmap_position as u64
                            } else {
                                shard.index_file.stream_position().unwrap()
                            } < file_len
                            {
                                let mut segment_head_vec: Vec<(u32, u32)> = Vec::new();
                                for key0 in 0..shard.segment_number1 {
                                    if key0 == 0 {
                                        shard.last_level_index_file_start_pos = if is_mmap {
                                            index_mmap_position as u64
                                        } else {
                                            shard.index_file.stream_position().unwrap()
                                        };

                                        shard.last_level_docstore_file_start_pos = if is_mmap {
                                            docstore_mmap_position as u64
                                        } else {
                                            shard.docstore_file.stream_position().unwrap()
                                        };

                                        if shard.level_index.is_empty() {
                                            let longest_field_id = if is_mmap {
                                                read_u16_ref(
                                                    &shard.index_file_mmap,
                                                    &mut index_mmap_position,
                                                )
                                                    as usize
                                            } else {
                                                let _ = shard.index_file.read(
                                                    &mut shard
                                                        .compressed_index_segment_block_buffer
                                                        [0..2],
                                                );
                                                read_u16(
                                                    &shard.compressed_index_segment_block_buffer,
                                                    0,
                                                )
                                                    as usize
                                            };

                                            for indexed_field in shard.indexed_field_vec.iter_mut()
                                            {
                                                indexed_field.is_longest_field = indexed_field
                                                    .indexed_field_id
                                                    == longest_field_id;

                                                if indexed_field.is_longest_field {
                                                    shard.longest_field_id = longest_field_id
                                                }
                                            }
                                        }

                                        let mut document_length_compressed_array_vec: Vec<
                                            [u8; ROARING_BLOCK_SIZE],
                                        > = Vec::new();

                                        let document_length_compressed_array_pointer = if is_mmap {
                                            index_mmap_position
                                        } else {
                                            shard.index_file.stream_position().unwrap() as usize
                                        };

                                        for _i in 0..shard.indexed_field_vec.len() {
                                            if is_mmap {
                                                index_mmap_position += ROARING_BLOCK_SIZE;
                                            } else {
                                                let mut document_length_compressed_array_item =
                                                    [0u8; ROARING_BLOCK_SIZE];

                                                let _ = shard.index_file.read(
                                                    &mut document_length_compressed_array_item,
                                                );
                                                document_length_compressed_array_vec
                                                    .push(document_length_compressed_array_item);
                                            }
                                        }

                                        let mut docstore_pointer_docs: Vec<u8> = Vec::new();

                                        let mut docstore_pointer_docs_pointer = 0;
                                        if !shard.stored_field_names.is_empty() {
                                            if is_mmap {
                                                let docstore_pointer_docs_size = read_u32_ref(
                                                    &shard.docstore_file_mmap,
                                                    &mut docstore_mmap_position,
                                                )
                                                    as usize;
                                                docstore_pointer_docs_pointer =
                                                    docstore_mmap_position;
                                                docstore_mmap_position +=
                                                    docstore_pointer_docs_size;
                                            } else {
                                                let _ = shard.docstore_file.read(
                                                    &mut shard
                                                        .compressed_index_segment_block_buffer
                                                        [0..4],
                                                );

                                                let docstore_pointer_docs_size = read_u32(
                                                    &shard.compressed_index_segment_block_buffer,
                                                    0,
                                                )
                                                    as usize;

                                                docstore_pointer_docs_pointer =
                                                    shard.docstore_file.stream_position().unwrap()
                                                        as usize;
                                                docstore_pointer_docs =
                                                    vec![0; docstore_pointer_docs_size];
                                                let _ = shard
                                                    .docstore_file
                                                    .read(&mut docstore_pointer_docs);
                                            }
                                        }

                                        if is_mmap {
                                            shard.indexed_doc_count = read_u64_ref(
                                                &shard.index_file_mmap,
                                                &mut index_mmap_position,
                                            )
                                                as usize;
                                            shard.positions_sum_normalized = read_u64_ref(
                                                &shard.index_file_mmap,
                                                &mut index_mmap_position,
                                            );

                                            for _key0 in 0..shard.segment_number1 {
                                                let block_length = read_u32_ref(
                                                    &shard.index_file_mmap,
                                                    &mut index_mmap_position,
                                                );
                                                let key_count = read_u32_ref(
                                                    &shard.index_file_mmap,
                                                    &mut index_mmap_position,
                                                );

                                                segment_head_vec.push((block_length, key_count));
                                            }
                                        } else {
                                            let _ = shard.index_file.read(
                                                &mut shard.compressed_index_segment_block_buffer
                                                    [0..16],
                                            );

                                            shard.indexed_doc_count = read_u64(
                                                &shard.compressed_index_segment_block_buffer,
                                                0,
                                            )
                                                as usize;

                                            shard.positions_sum_normalized = read_u64(
                                                &shard.compressed_index_segment_block_buffer,
                                                8,
                                            );

                                            for _key0 in 0..shard.segment_number1 {
                                                let _ = shard.index_file.read(
                                                    &mut shard
                                                        .compressed_index_segment_block_buffer
                                                        [0..8],
                                                );

                                                let block_length = read_u32(
                                                    &shard.compressed_index_segment_block_buffer,
                                                    0,
                                                );
                                                let key_count = read_u32(
                                                    &shard.compressed_index_segment_block_buffer,
                                                    4,
                                                );
                                                segment_head_vec.push((block_length, key_count));
                                            }
                                        }

                                        shard.document_length_normalized_average =
                                            shard.positions_sum_normalized as f32
                                                / shard.indexed_doc_count as f32;

                                        shard.level_index.push(LevelIndex {
                                            document_length_compressed_array:
                                                document_length_compressed_array_vec,
                                            docstore_pointer_docs,
                                            docstore_pointer_docs_pointer,
                                            document_length_compressed_array_pointer,
                                        });
                                    }

                                    let block_length = segment_head_vec[key0].0;
                                    let key_count = segment_head_vec[key0].1;

                                    let block_id =
                                        (block_count_sum >> shard.segment_number_bits1) as u32;
                                    block_count_sum += 1;

                                    let key_body_pointer_write_start: u32 =
                                        key_count * shard.key_head_size as u32;

                                    if is_mmap {
                                        index_mmap_position +=
                                            key_count as usize * shard.key_head_size;
                                        shard.segments_index[key0].byte_array_blocks_pointer.push(
                                            (
                                                index_mmap_position,
                                                (block_length - key_body_pointer_write_start)
                                                    as usize,
                                                key_count,
                                            ),
                                        );

                                        index_mmap_position +=
                                            (block_length - key_body_pointer_write_start) as usize;
                                    } else {
                                        let _ = shard.index_file.read(
                                            &mut shard.compressed_index_segment_block_buffer
                                                [0..(key_count as usize * shard.key_head_size)],
                                        );
                                        let compressed_index_segment_block_buffer = &shard
                                            .compressed_index_segment_block_buffer
                                            [0..(key_count as usize * shard.key_head_size)];

                                        let mut block_array: Vec<u8> = vec![
                                            0;
                                            (block_length - key_body_pointer_write_start)
                                                as usize
                                        ];

                                        let _ = shard.index_file.read(&mut block_array);
                                        shard.segments_index[key0]
                                            .byte_array_blocks
                                            .push(block_array);

                                        let mut read_pointer = 0;

                                        let mut posting_count_previous = 0;
                                        let mut pointer_pivot_p_docid_previous = 0;
                                        let mut compression_type_pointer_previous = 0;

                                        for key_index in 0..key_count {
                                            let key_hash = read_u64_ref(
                                                compressed_index_segment_block_buffer,
                                                &mut read_pointer,
                                            );

                                            let posting_count = read_u16_ref(
                                                compressed_index_segment_block_buffer,
                                                &mut read_pointer,
                                            );

                                            let max_docid = read_u16_ref(
                                                compressed_index_segment_block_buffer,
                                                &mut read_pointer,
                                            );

                                            let max_p_docid = read_u16_ref(
                                                compressed_index_segment_block_buffer,
                                                &mut read_pointer,
                                            );

                                            let mut posting_count_ngram_1 = 0;
                                            let mut posting_count_ngram_2 = 0;
                                            let mut posting_count_ngram_3 = 0;
                                            match shard.key_head_size {
                                                20 => {}
                                                22 => {
                                                    let posting_count_ngram_1_compressed =
                                                        read_u8_ref(
                                                            compressed_index_segment_block_buffer,
                                                            &mut read_pointer,
                                                        );
                                                    posting_count_ngram_1 =
                                                        DOCUMENT_LENGTH_COMPRESSION
                                                            [posting_count_ngram_1_compressed
                                                                as usize];

                                                    let posting_count_ngram_2_compressed =
                                                        read_u8_ref(
                                                            compressed_index_segment_block_buffer,
                                                            &mut read_pointer,
                                                        );
                                                    posting_count_ngram_2 =
                                                        DOCUMENT_LENGTH_COMPRESSION
                                                            [posting_count_ngram_2_compressed
                                                                as usize];
                                                }
                                                _ => {
                                                    let posting_count_ngram_1_compressed =
                                                        read_u8_ref(
                                                            compressed_index_segment_block_buffer,
                                                            &mut read_pointer,
                                                        );
                                                    posting_count_ngram_1 =
                                                        DOCUMENT_LENGTH_COMPRESSION
                                                            [posting_count_ngram_1_compressed
                                                                as usize];

                                                    let posting_count_ngram_2_compressed =
                                                        read_u8_ref(
                                                            compressed_index_segment_block_buffer,
                                                            &mut read_pointer,
                                                        );
                                                    posting_count_ngram_2 =
                                                        DOCUMENT_LENGTH_COMPRESSION
                                                            [posting_count_ngram_2_compressed
                                                                as usize];

                                                    let posting_count_ngram_3_compressed =
                                                        read_u8_ref(
                                                            compressed_index_segment_block_buffer,
                                                            &mut read_pointer,
                                                        );
                                                    posting_count_ngram_3 =
                                                        DOCUMENT_LENGTH_COMPRESSION
                                                            [posting_count_ngram_3_compressed
                                                                as usize];
                                                }
                                            }

                                            let pointer_pivot_p_docid = read_u16_ref(
                                                compressed_index_segment_block_buffer,
                                                &mut read_pointer,
                                            );

                                            let compression_type_pointer = read_u32_ref(
                                                compressed_index_segment_block_buffer,
                                                &mut read_pointer,
                                            );

                                            if let Some(value) = shard.segments_index[key0]
                                                .segment
                                                .get_mut(&key_hash)
                                            {
                                                value.posting_count += posting_count as u32 + 1;

                                                value.blocks.push(BlockObjectIndex {
                                                    max_block_score: 0.0,
                                                    block_id,
                                                    posting_count,
                                                    max_docid,
                                                    max_p_docid,
                                                    pointer_pivot_p_docid,
                                                    compression_type_pointer,
                                                });
                                            } else {
                                                let value = PostingListObjectIndex {
                                                    posting_count: posting_count as u32 + 1,
                                                    posting_count_ngram_1,
                                                    posting_count_ngram_2,
                                                    posting_count_ngram_3,
                                                    max_list_score: 0.0,
                                                    position_range_previous: 0,
                                                    blocks: vec![BlockObjectIndex {
                                                        max_block_score: 0.0,
                                                        block_id,
                                                        posting_count,
                                                        max_docid,
                                                        max_p_docid,
                                                        pointer_pivot_p_docid,
                                                        compression_type_pointer,
                                                    }],
                                                    ..Default::default()
                                                };
                                                shard.segments_index[key0]
                                                    .segment
                                                    .insert(key_hash, value);
                                            };

                                            if !shard
                                                .indexed_doc_count
                                                .is_multiple_of(ROARING_BLOCK_SIZE)
                                                && block_id as usize
                                                    == shard.indexed_doc_count / ROARING_BLOCK_SIZE
                                                && shard.meta.access_type == AccessType::Ram
                                            {
                                                let position_range_previous = if key_index == 0 {
                                                    0
                                                } else {
                                                    let posting_pointer_size_sum_previous =
                                                        pointer_pivot_p_docid_previous as usize * 2
                                                            + if (pointer_pivot_p_docid_previous
                                                                as usize)
                                                                < posting_count_previous
                                                            {
                                                                (posting_count_previous
                                                                    - pointer_pivot_p_docid_previous
                                                                        as usize)
                                                                    * 3
                                                            } else {
                                                                0
                                                            };

                                                    let rank_position_pointer_range_previous= compression_type_pointer_previous & 0b0011_1111_1111_1111_1111_1111_1111_1111;
                                                    let compression_type_previous: CompressionType =
                                                        FromPrimitive::from_i32(
                                                            (compression_type_pointer_previous
                                                                >> 30)
                                                                as i32,
                                                        )
                                                        .unwrap();

                                                    let compressed_docid_previous =
                                                        match compression_type_previous {
                                                            CompressionType::Array => {
                                                                posting_count_previous * 2
                                                            }
                                                            CompressionType::Bitmap => 8192,
                                                            CompressionType::Rle => {
                                                                let byte_array_docid = &shard
                                                                    .segments_index[key0]
                                                                    .byte_array_blocks
                                                                    [block_id as usize];
                                                                4 * read_u16( byte_array_docid, rank_position_pointer_range_previous as usize +posting_pointer_size_sum_previous) as usize + 2
                                                            }
                                                            _ => 0,
                                                        };

                                                    rank_position_pointer_range_previous
                                                        + (posting_pointer_size_sum_previous
                                                            + compressed_docid_previous)
                                                            as u32
                                                };

                                                let plo = shard.segments_index[key0]
                                                    .segment
                                                    .get_mut(&key_hash)
                                                    .unwrap();

                                                plo.position_range_previous =
                                                    position_range_previous;

                                                posting_count_previous = posting_count as usize + 1;
                                                pointer_pivot_p_docid_previous =
                                                    pointer_pivot_p_docid;
                                                compression_type_pointer_previous =
                                                    compression_type_pointer;
                                            };
                                        }
                                    }
                                }
                            }

                            shard.committed_doc_count = shard.indexed_doc_count;
                            shard.is_last_level_incomplete =
                                !shard.committed_doc_count.is_multiple_of(ROARING_BLOCK_SIZE);

                            for (i, component) in shard.bm25_component_cache.iter_mut().enumerate()
                            {
                                let document_length_quotient = DOCUMENT_LENGTH_COMPRESSION[i]
                                    as f32
                                    / shard.document_length_normalized_average;
                                *component = K * (1.0 - B + B * document_length_quotient);
                            }

                            shard.string_set_to_single_term_id();

                            update_list_max_impact_score(&mut shard);

                            let mut reader = BufReader::with_capacity(8192, &shard.delete_file);
                            while let Ok(buffer) = reader.fill_buf() {
                                let length = buffer.len();

                                if length == 0 {
                                    break;
                                }

                                for i in (0..length).step_by(8) {
                                    let docid = read_u64(buffer, i);
                                    shard.delete_hashset.insert(docid as usize);
                                }

                                reader.consume(length);
                            }

                            let shard_arc = Arc::new(RwLock::new(shard));

                            warmup(&shard_arc).await;
                            Ok(shard_arc.clone())
                        }
                        Err(err) => Err(err.to_string()),
                    }
                }
                Err(err) => Err(err.to_string()),
            }
        }
        Err(err) => Err(err.to_string()),
    }
}

/// Loads the index from disk into RAM or MMAP.
/// * `index_path` - index path.  
/// * `mute` - prevent emitting status messages (e.g. when using pipes for data interprocess communication).  
pub async fn open_index(index_path: &Path, mute: bool) -> Result<IndexArc, String> {
    if !mute {
        println!("opening index ...");
    }

    let start_time = Instant::now();

    match File::open(Path::new(index_path).join(META_FILENAME)) {
        Ok(meta_file) => {
            let meta: IndexMetaObject = serde_json::from_reader(BufReader::new(meta_file)).unwrap();

            match File::open(Path::new(index_path).join(SCHEMA_FILENAME)) {
                Ok(schema_file) => {
                    let schema = serde_json::from_reader(BufReader::new(schema_file)).unwrap();

                    let synonyms = if let Ok(synonym_file) =
                        File::open(Path::new(index_path).join(SYNONYMS_FILENAME))
                    {
                        serde_json::from_reader(BufReader::new(synonym_file)).unwrap_or_default()
                    } else {
                        Vec::new()
                    };

                    match create_index_root(
                        index_path, meta, &schema, false, &synonyms, 11, false, None,
                    )
                    .await
                    {
                        Ok(index_arc) => {
                            let lock = Arc::into_inner(index_arc).unwrap();
                            let index = RwLock::into_inner(lock);

                            let index_arc = Arc::new(RwLock::new(index));

                            if let Some(symspell) =
                                &mut index_arc.read().await.symspell_option.as_ref()
                            {
                                let dictionary_path =
                                    Path::new(&index_arc.read().await.index_path_string)
                                        .join(DICTIONARY_FILENAME);
                                let _ = symspell.write().await.load_dictionary(
                                    &dictionary_path,
                                    0,
                                    1,
                                    " ",
                                );
                            }

                            if let Some(completion_option) =
                                &mut index_arc.read().await.completion_option.as_ref()
                            {
                                let _ = completion_option.write().await.load_completions(
                                    &Path::new(&index_arc.read().await.index_path_string)
                                        .join(COMPLETIONS_FILENAME),
                                    0,
                                    1,
                                    ":",
                                );
                            }

                            let mut level_count = 0;

                            let mut shard_vec: Vec<Arc<RwLock<Shard>>> = Vec::new();
                            let mut shard_queue = VecDeque::<usize>::new();

                            let paths: Vec<_> = fs::read_dir(index_path.join("shards"))
                                .unwrap()
                                .filter_map(Result::ok)
                                .collect();
                            let mut shard_handle_vec = Vec::new();
                            let index_path_clone = Arc::new(index_path.to_path_buf());
                            for i in 0..paths.len() {
                                let index_path_clone2 = index_path_clone.clone();
                                shard_handle_vec.push(tokio::spawn(async move {
                                    let path = index_path_clone2.join("shards").join(i.to_string());

                                    open_shard(&path, true).await.unwrap()
                                }));
                            }

                            for shard_handle in shard_handle_vec {
                                let shard_arc = shard_handle.await.unwrap();
                                shard_arc.write().await.index_option = Some(index_arc.clone());
                                index_arc.write().await.indexed_doc_count +=
                                    shard_arc.read().await.indexed_doc_count;
                                index_arc.write().await.deleted_doc_count +=
                                    shard_arc.read().await.delete_hashset.len();
                                level_count += shard_arc.read().await.level_index.len();
                                let shard_id = shard_arc.read().await.meta.id;
                                shard_queue.push_back(shard_id as usize);
                                shard_vec.push(shard_arc);
                            }

                            index_arc.write().await.shard_number = shard_vec.len();
                            index_arc.write().await.shard_bits =
                                (usize::BITS - (shard_vec.len() - 1).leading_zeros()) as usize;

                            for shard in shard_vec.iter() {
                                shard.write().await.shard_number = shard_vec.len();
                            }

                            index_arc.write().await.shard_vec = shard_vec;
                            index_arc.write().await.shard_queue =
                                Arc::new(RwLock::new(shard_queue));

                            let elapsed_time = start_time.elapsed().as_nanos();

                            if !mute {
                                let index_ref = index_arc.read().await;
                                println!(
                                    "{} name {} id {} version {} {} shards {} ngrams {:08b} level {} fields {} {} facets {} docs {} deleted {} segments {} dictionary {} {} completions {} time {} s",
                                    INDEX_FILENAME,
                                    index_ref.meta.name,
                                    index_ref.meta.id,
                                    index_ref.index_format_version_major.to_string()
                                        + "."
                                        + &index_ref.index_format_version_minor.to_string(),
                                    INDEX_FORMAT_VERSION_MAJOR.to_string()
                                        + "."
                                        + &INDEX_FORMAT_VERSION_MINOR.to_string(),
                                    index_ref.shard_count().await,
                                    index_ref.meta.ngram_indexing,
                                    level_count,
                                    index_ref.indexed_field_vec.len(),
                                    index_ref.schema_map.len(),
                                    index_ref.facets.len(),
                                    index_ref.indexed_doc_count.to_formatted_string(&Locale::en),
                                    index_ref.deleted_doc_count.to_formatted_string(&Locale::en),
                                    index_ref.segment_number1,
                                    if let Some(symspell) = index_ref.symspell_option.as_ref() {
                                        symspell
                                            .read()
                                            .await
                                            .get_dictionary_size()
                                            .to_formatted_string(&Locale::en)
                                    } else {
                                        "None".to_string()
                                    },
                                    if let Some(symspell) = index_ref.symspell_option.as_ref() {
                                        symspell
                                            .read()
                                            .await
                                            .get_candidates_size()
                                            .to_formatted_string(&Locale::en)
                                    } else {
                                        "None".to_string()
                                    },
                                    if let Some(completions) = index_ref.completion_option.as_ref()
                                    {
                                        completions
                                            .read()
                                            .await
                                            .len()
                                            .to_formatted_string(&Locale::en)
                                    } else {
                                        "None".to_string()
                                    },
                                    elapsed_time / 1_000_000_000
                                );
                            }

                            Ok(index_arc.clone())
                        }
                        Err(err) => Err(err.to_string()),
                    }
                }
                Err(err) => Err(err.to_string()),
            }
        }
        Err(err) => Err(err.to_string()),
    }
}

pub(crate) async fn warmup(shard_object_arc: &ShardArc) {
    shard_object_arc.write().await.frequentword_results.clear();
    let mut query_facets: Vec<QueryFacet> = Vec::new();
    for facet in shard_object_arc.read().await.facets.iter() {
        match facet.field_type {
            FieldType::String16 => query_facets.push(QueryFacet::String16 {
                field: facet.name.clone(),
                prefix: "".into(),
                length: u16::MAX,
            }),
            FieldType::String32 => query_facets.push(QueryFacet::String32 {
                field: facet.name.clone(),
                prefix: "".into(),
                length: u32::MAX,
            }),
            FieldType::StringSet16 => query_facets.push(QueryFacet::StringSet16 {
                field: facet.name.clone(),
                prefix: "".into(),
                length: u16::MAX,
            }),
            FieldType::StringSet32 => query_facets.push(QueryFacet::StringSet32 {
                field: facet.name.clone(),
                prefix: "".into(),
                length: u32::MAX,
            }),
            _ => {}
        }
    }

    let frequent_words = shard_object_arc.read().await.frequent_words.clone();
    for frequentword in frequent_words.iter() {
        let results_list = shard_object_arc
            .search_shard(
                frequentword.to_owned(),
                QueryType::Union,
                0,
                1000,
                ResultType::TopkCount,
                false,
                Vec::new(),
                query_facets.clone(),
                Vec::new(),
                Vec::new(),
            )
            .await;

        let mut index_mut = shard_object_arc.write().await;
        index_mut
            .frequentword_results
            .insert(frequentword.to_string(), results_list);
    }
}

#[derive(Default, Debug, Deserialize, Serialize, Clone)]
pub(crate) struct TermObject {
    pub key_hash: u64,
    pub key0: u32,
    pub term: String,

    pub ngram_type: NgramType,

    pub term_ngram_2: String,
    pub term_ngram_1: String,
    pub term_ngram_0: String,
    pub field_vec_ngram1: Vec<(usize, u32)>,
    pub field_vec_ngram2: Vec<(usize, u32)>,
    pub field_vec_ngram3: Vec<(usize, u32)>,

    pub field_positions_vec: Vec<Vec<u16>>,
}

#[derive(Default, Debug, Serialize, Deserialize, Clone)]
pub(crate) struct NonUniqueTermObject {
    pub term: String,
    pub ngram_type: NgramType,

    pub term_ngram_2: String,
    pub term_ngram_1: String,
    pub term_ngram_0: String,
    pub op: QueryType,
}

#[cfg(not(all(target_feature = "aes", target_feature = "sse2")))]
use ahash::RandomState;
#[cfg(not(all(target_feature = "aes", target_feature = "sse2")))]
use std::sync::LazyLock;

#[cfg(not(all(target_feature = "aes", target_feature = "sse2")))]
pub static HASHER_32: LazyLock<RandomState> =
    LazyLock::new(|| RandomState::with_seeds(805272099, 242851902, 646123436, 591410655));

#[cfg(not(all(target_feature = "aes", target_feature = "sse2")))]
pub static HASHER_64: LazyLock<RandomState> =
    LazyLock::new(|| RandomState::with_seeds(808259318, 750368348, 84901999, 789810389));

#[inline]
#[cfg(all(target_feature = "aes", target_feature = "sse2"))]
pub(crate) fn hash32(term_bytes: &[u8]) -> u32 {
    gxhash32(term_bytes, 1234)
}

#[inline]
#[cfg(all(target_feature = "aes", target_feature = "sse2"))]
pub(crate) fn hash64(term_bytes: &[u8]) -> u64 {
    gxhash64(term_bytes, 1234) & 0b1111111111111111111111111111111111111111111111111111111111111000
}

#[inline]
#[cfg(not(all(target_feature = "aes", target_feature = "sse2")))]
pub(crate) fn hash32(term_bytes: &[u8]) -> u32 {
    HASHER_32.hash_one(term_bytes) as u32
}

#[inline]
#[cfg(not(all(target_feature = "aes", target_feature = "sse2")))]
pub(crate) fn hash64(term_bytes: &[u8]) -> u64 {
    HASHER_64.hash_one(term_bytes)
        & 0b1111111111111111111111111111111111111111111111111111111111111000
}

static FREQUENT_EN: &str = include_str!("../../assets/dictionaries/frequent_en.txt");
static FREQUENT_DE: &str = include_str!("../../assets/dictionaries/frequent_de.txt");
static FREQUENT_FR: &str = include_str!("../../assets/dictionaries/frequent_fr.txt");
static FREQUENT_ES: &str = include_str!("../../assets/dictionaries/frequent_es.txt");

pub(crate) const NUM_FREE_VALUES: u32 = 24;

/// Compress an u32 to a byte, preserving 4 significant bits.
/// used for compressing n-gram frequent_term positions_count and doc/field length
/// Ported from Lucene SmallFloat.java https://github.com/apache/lucene/blob/main/lucene/core/src/java/org/apache/lucene/util/SmallFloat.java
pub(crate) fn int_to_byte4(i: u32) -> u8 {
    if i < NUM_FREE_VALUES {
        i as u8
    } else {
        let ii = i - NUM_FREE_VALUES;
        let num_bits = 32 - ii.leading_zeros();
        if num_bits < 4 {
            (NUM_FREE_VALUES + ii) as u8
        } else {
            let shift = num_bits - 4;
            (NUM_FREE_VALUES + (((ii >> shift) & 0x07) | (shift + 1) << 3)) as u8
        }
    }
}

/// Decompress a byte that has been compressed with intToByte4(int), to an u32
/// used for pre-calculating DOCUMENT_LENGTH_COMPRESSION table. Decompressing n-gram frequent_term positions_count and doc/field length via table lookup.
/// Ported from Lucene SmallFloat.java https://github.com/apache/lucene/blob/main/lucene/core/src/java/org/apache/lucene/util/SmallFloat.java
pub(crate) const fn byte4_to_int(b: u8) -> u32 {
    if (b as u32) < NUM_FREE_VALUES {
        b as u32
    } else {
        let i = b as u32 - NUM_FREE_VALUES;
        let bits = i & 0x07;
        let shift = i >> 3;
        if shift == 0 {
            NUM_FREE_VALUES + bits
        } else {
            NUM_FREE_VALUES + ((bits | 0x08) << (shift - 1))
        }
    }
}

/// Pre-calculated DOCUMENT_LENGTH_COMPRESSION table for fast lookup.
pub(crate) const DOCUMENT_LENGTH_COMPRESSION: [u32; 256] = {
    let mut k2 = [0; 256];
    let mut i = 0usize;
    while i < 256 {
        k2[i] = byte4_to_int(i as u8);
        i += 1;
    }
    k2
};

impl Shard {
    pub(crate) fn string_set_to_single_term_id(&mut self) {
        for (i, facet) in self.facets.iter().enumerate() {
            if facet.field_type == FieldType::StringSet16
                || facet.field_type == FieldType::StringSet32
            {
                for (idx, value) in facet.values.iter().enumerate() {
                    for term in value.1.0.iter() {
                        self.string_set_to_single_term_id_vec[i]
                            .entry(term.to_string())
                            .or_insert(AHashSet::from_iter(vec![idx as u32]))
                            .insert(idx as u32);
                    }
                }
            }
        }
    }

    /// Reset shard to empty, while maintaining schema
    async fn clear_shard(&mut self) {
        let permit = self.permits.clone().acquire_owned().await.unwrap();

        self.level_terms.clear();

        let mut mmap_options = MmapOptions::new();
        let mmap: MmapMut = mmap_options.len(4).map_anon().unwrap();
        self.index_file_mmap = mmap
            .make_read_only()
            .expect("Unable to make Mmap read-only");

        let _ = self.index_file.rewind();
        if let Err(e) = self.index_file.set_len(0) {
            println!(
                "Unable to index_file.set_len in clear_index {} {} {:?}",
                self.index_path_string, self.indexed_doc_count, e
            )
        };

        if !self.compressed_docstore_segment_block_buffer.is_empty() {
            self.compressed_docstore_segment_block_buffer = vec![0; ROARING_BLOCK_SIZE * 4];
        };

        write_u16(
            INDEX_FORMAT_VERSION_MAJOR,
            &mut self.compressed_index_segment_block_buffer,
            0,
        );
        write_u16(
            INDEX_FORMAT_VERSION_MINOR,
            &mut self.compressed_index_segment_block_buffer,
            2,
        );

        let _ = self
            .index_file
            .write(&self.compressed_index_segment_block_buffer[0..INDEX_HEADER_SIZE as usize]);
        let _ = self.index_file.flush();

        self.index_file_mmap =
            unsafe { Mmap::map(&self.index_file).expect("Unable to create Mmap") };

        self.docstore_file_mmap = unsafe {
            MmapOptions::new()
                .len(0)
                .map(&self.docstore_file)
                .expect("Unable to create Mmap")
        };

        let _ = self.docstore_file.rewind();
        if let Err(e) = self.docstore_file.set_len(0) {
            println!("Unable to docstore_file.set_len in clear_index {:?}", e)
        };
        let _ = self.docstore_file.flush();

        let _ = self.delete_file.rewind();
        if let Err(e) = self.delete_file.set_len(0) {
            println!("Unable to delete_file.set_len in clear_index {:?}", e)
        };
        let _ = self.delete_file.flush();
        self.delete_hashset.clear();

        self.facets_file_mmap = unsafe {
            MmapOptions::new()
                .len(0)
                .map_mut(&self.facets_file)
                .expect("Unable to create Mmap")
        };
        let _ = self.facets_file.rewind();
        if let Err(e) = self
            .facets_file
            .set_len((self.facets_size_sum * ROARING_BLOCK_SIZE) as u64)
        {
            println!("Unable to facets_file.set_len in clear_index {:?}", e)
        };
        let _ = self.facets_file.flush();

        self.facets_file_mmap =
            unsafe { MmapMut::map_mut(&self.facets_file).expect("Unable to create Mmap") };
        let index_path = Path::new(&self.index_path_string);
        let _ = fs::remove_file(index_path.join(FACET_VALUES_FILENAME));
        for facet in self.facets.iter_mut() {
            facet.values.clear();
            facet.min = ValueType::None;
            facet.max = ValueType::None;
        }

        if !self.stored_field_names.is_empty() && self.meta.access_type == AccessType::Mmap {
            self.docstore_file_mmap =
                unsafe { Mmap::map(&self.docstore_file).expect("Unable to create Mmap") };
        }

        self.document_length_normalized_average = 0.0;
        self.indexed_doc_count = 0;
        self.committed_doc_count = 0;
        self.positions_sum_normalized = 0;

        self.level_index = Vec::new();

        for segment in self.segments_index.iter_mut() {
            segment.byte_array_blocks.clear();
            segment.byte_array_blocks_pointer.clear();
            segment.segment.clear();
        }

        for segment in self.segments_level0.iter_mut() {
            segment.segment.clear();
        }

        self.key_count_sum = 0;
        self.block_id = 0;
        self.strip_compressed_sum = 0;
        self.postings_buffer_pointer = 0;
        self.docid_count = 0;
        self.size_compressed_docid_index = 0;
        self.size_compressed_positions_index = 0;
        self.position_count = 0;
        self.postinglist_count = 0;

        self.is_last_level_incomplete = false;

        drop(permit);
    }

    pub(crate) fn get_index_string_facets_shard(
        &self,
        query_facets: Vec<QueryFacet>,
    ) -> Option<AHashMap<String, Facet>> {
        if self.facets.is_empty() {
            return None;
        }

        let mut result_query_facets = Vec::new();
        if !query_facets.is_empty() {
            result_query_facets = vec![ResultFacet::default(); self.facets.len()];
            for query_facet in query_facets.iter() {
                match &query_facet {
                    QueryFacet::String16 {
                        field,
                        prefix,
                        length,
                    } => {
                        if let Some(idx) = self.facets_map.get(field)
                            && self.facets[*idx].field_type == FieldType::String16
                        {
                            result_query_facets[*idx] = ResultFacet {
                                field: field.clone(),
                                prefix: prefix.clone(),
                                length: *length as u32,
                                ..Default::default()
                            }
                        }
                    }
                    QueryFacet::StringSet16 {
                        field,
                        prefix,
                        length,
                    } => {
                        if let Some(idx) = self.facets_map.get(field)
                            && self.facets[*idx].field_type == FieldType::StringSet16
                        {
                            result_query_facets[*idx] = ResultFacet {
                                field: field.clone(),
                                prefix: prefix.clone(),
                                length: *length as u32,
                                ..Default::default()
                            }
                        }
                    }

                    QueryFacet::String32 {
                        field,
                        prefix,
                        length,
                    } => {
                        if let Some(idx) = self.facets_map.get(field)
                            && self.facets[*idx].field_type == FieldType::String32
                        {
                            result_query_facets[*idx] = ResultFacet {
                                field: field.clone(),
                                prefix: prefix.clone(),
                                length: *length,
                                ..Default::default()
                            }
                        }
                    }
                    QueryFacet::StringSet32 {
                        field,
                        prefix,
                        length,
                    } => {
                        if let Some(idx) = self.facets_map.get(field)
                            && self.facets[*idx].field_type == FieldType::StringSet32
                        {
                            result_query_facets[*idx] = ResultFacet {
                                field: field.clone(),
                                prefix: prefix.clone(),
                                length: *length,
                                ..Default::default()
                            }
                        }
                    }

                    _ => {}
                };
            }
        }

        let mut facets: AHashMap<String, Facet> = AHashMap::new();
        for (i, facet) in result_query_facets.iter().enumerate() {
            if facet.length == 0 || self.facets[i].values.is_empty() {
                continue;
            }

            if self.facets[i].field_type == FieldType::StringSet16
                || self.facets[i].field_type == FieldType::StringSet32
            {
                let mut hash_map: AHashMap<String, usize> = AHashMap::new();
                for value in self.facets[i].values.iter() {
                    for term in value.1.0.iter() {
                        *hash_map.entry(term.clone()).or_insert(0) += value.1.1;
                    }
                }

                let v = hash_map
                    .iter()
                    .sorted_unstable_by(|a, b| b.1.cmp(a.1))
                    .map(|(a, c)| (a.to_string(), *c))
                    .filter(|(a, _c)| facet.prefix.is_empty() || a.starts_with(&facet.prefix))
                    .take(facet.length as usize)
                    .collect::<Vec<_>>();

                if !v.is_empty() {
                    facets.insert(facet.field.clone(), v);
                }
            } else {
                let v = self.facets[i]
                    .values
                    .iter()
                    .sorted_unstable_by(|a, b| b.1.cmp(a.1))
                    .map(|(a, c)| (a.to_string(), c.1))
                    .filter(|(a, _c)| facet.prefix.is_empty() || a.starts_with(&facet.prefix))
                    .take(facet.length as usize)
                    .collect::<Vec<_>>();

                if !v.is_empty() {
                    facets.insert(facet.field.clone(), v);
                }
            }
        }

        Some(facets)
    }
}

impl Index {
    /// Current document count: indexed document count - deleted document count
    pub async fn current_doc_count(&self) -> usize {
        let mut current_doc_count = 0;
        for shard in self.shard_vec.iter() {
            current_doc_count +=
                shard.read().await.indexed_doc_count - shard.read().await.delete_hashset.len();
        }
        current_doc_count
    }

    /// are there uncommited documents?
    pub async fn uncommitted_doc_count(&self) -> usize {
        let mut uncommitted_doc_count = 0;
        for shard in self.shard_vec.iter() {
            uncommitted_doc_count +=
                shard.read().await.indexed_doc_count - shard.read().await.committed_doc_count;
        }
        uncommitted_doc_count
    }

    /// Get number of indexed documents.
    pub async fn committed_doc_count(&self) -> usize {
        let mut committed_doc_count = 0;
        for shard in self.shard_vec.iter() {
            committed_doc_count += shard.read().await.committed_doc_count;
        }
        committed_doc_count
    }

    /// Get number of indexed documents.
    pub async fn indexed_doc_count(&self) -> usize {
        let mut indexed_doc_count = 0;
        for shard in self.shard_vec.iter() {
            indexed_doc_count += shard.read().await.indexed_doc_count;
        }
        indexed_doc_count
    }

    /// Get number of index levels. One index level comprises 64K documents.
    pub async fn level_count(&self) -> usize {
        let mut level_count = 0;
        for shard in self.shard_vec.iter() {
            level_count += shard.read().await.level_index.len();
        }
        level_count
    }

    /// Get number of index shards.
    pub async fn shard_count(&self) -> usize {
        self.shard_number
    }

    /// Get number of facets defined in the index schema.
    pub fn facets_count(&self) -> usize {
        self.facets.len()
    }

    /// get_index_facets_minmax: return map of numeric facet fields, each with field name and min/max values.
    pub async fn index_facets_minmax(&self) -> HashMap<String, MinMaxFieldJson> {
        let mut facets_minmax: HashMap<String, MinMaxFieldJson> = HashMap::new();
        for shard in self.shard_vec.iter() {
            for facet in shard.read().await.facets.iter() {
                match (&facet.min, &facet.max) {
                    (ValueType::U8(min), ValueType::U8(max)) => {
                        if let Some(item) = facets_minmax.get_mut(&facet.name) {
                            *item = MinMaxFieldJson {
                                min: (*min.min(&(item.min.as_u64().unwrap() as u8))).into(),
                                max: (*max.min(&(item.max.as_u64().unwrap() as u8))).into(),
                            }
                        } else {
                            facets_minmax.insert(
                                facet.name.clone(),
                                MinMaxFieldJson {
                                    min: (*min).into(),
                                    max: (*max).into(),
                                },
                            );
                        }
                    }
                    (ValueType::U16(min), ValueType::U16(max)) => {
                        if let Some(item) = facets_minmax.get_mut(&facet.name) {
                            *item = MinMaxFieldJson {
                                min: (*min.min(&(item.min.as_u64().unwrap() as u16))).into(),
                                max: (*max.min(&(item.max.as_u64().unwrap() as u16))).into(),
                            }
                        } else {
                            facets_minmax.insert(
                                facet.name.clone(),
                                MinMaxFieldJson {
                                    min: (*min).into(),
                                    max: (*max).into(),
                                },
                            );
                        }
                    }
                    (ValueType::U32(min), ValueType::U32(max)) => {
                        if let Some(item) = facets_minmax.get_mut(&facet.name) {
                            *item = MinMaxFieldJson {
                                min: (*min.min(&(item.min.as_u64().unwrap() as u32))).into(),
                                max: (*max.min(&(item.max.as_u64().unwrap() as u32))).into(),
                            }
                        } else {
                            facets_minmax.insert(
                                facet.name.clone(),
                                MinMaxFieldJson {
                                    min: (*min).into(),
                                    max: (*max).into(),
                                },
                            );
                        }
                    }
                    (ValueType::U64(min), ValueType::U64(max)) => {
                        if let Some(item) = facets_minmax.get_mut(&facet.name) {
                            *item = MinMaxFieldJson {
                                min: (*min.min(&(item.min.as_u64().unwrap()))).into(),
                                max: (*max.min(&(item.max.as_u64().unwrap()))).into(),
                            }
                        } else {
                            facets_minmax.insert(
                                facet.name.clone(),
                                MinMaxFieldJson {
                                    min: (*min).into(),
                                    max: (*max).into(),
                                },
                            );
                        }
                    }
                    (ValueType::I8(min), ValueType::I8(max)) => {
                        if let Some(item) = facets_minmax.get_mut(&facet.name) {
                            *item = MinMaxFieldJson {
                                min: (*min.min(&(item.min.as_i64().unwrap() as i8))).into(),
                                max: (*max.min(&(item.max.as_i64().unwrap() as i8))).into(),
                            }
                        } else {
                            facets_minmax.insert(
                                facet.name.clone(),
                                MinMaxFieldJson {
                                    min: (*min).into(),
                                    max: (*max).into(),
                                },
                            );
                        }
                    }
                    (ValueType::I16(min), ValueType::I16(max)) => {
                        if let Some(item) = facets_minmax.get_mut(&facet.name) {
                            *item = MinMaxFieldJson {
                                min: (*min.min(&(item.min.as_i64().unwrap() as i16))).into(),
                                max: (*max.min(&(item.max.as_i64().unwrap() as i16))).into(),
                            }
                        } else {
                            facets_minmax.insert(
                                facet.name.clone(),
                                MinMaxFieldJson {
                                    min: (*min).into(),
                                    max: (*max).into(),
                                },
                            );
                        }
                    }
                    (ValueType::I32(min), ValueType::I32(max)) => {
                        if let Some(item) = facets_minmax.get_mut(&facet.name) {
                            *item = MinMaxFieldJson {
                                min: (*min.min(&(item.min.as_i64().unwrap() as i32))).into(),
                                max: (*max.min(&(item.max.as_i64().unwrap() as i32))).into(),
                            }
                        } else {
                            facets_minmax.insert(
                                facet.name.clone(),
                                MinMaxFieldJson {
                                    min: (*min).into(),
                                    max: (*max).into(),
                                },
                            );
                        }
                    }
                    (ValueType::I64(min), ValueType::I64(max)) => {
                        if let Some(item) = facets_minmax.get_mut(&facet.name) {
                            *item = MinMaxFieldJson {
                                min: (*min.min(&(item.min.as_i64().unwrap()))).into(),
                                max: (*max.min(&(item.max.as_i64().unwrap()))).into(),
                            }
                        } else {
                            facets_minmax.insert(
                                facet.name.clone(),
                                MinMaxFieldJson {
                                    min: (*min).into(),
                                    max: (*max).into(),
                                },
                            );
                        }
                    }
                    (ValueType::Timestamp(min), ValueType::Timestamp(max)) => {
                        if let Some(item) = facets_minmax.get_mut(&facet.name) {
                            *item = MinMaxFieldJson {
                                min: (*min.min(&(item.min.as_i64().unwrap()))).into(),
                                max: (*max.min(&(item.max.as_i64().unwrap()))).into(),
                            }
                        } else {
                            facets_minmax.insert(
                                facet.name.clone(),
                                MinMaxFieldJson {
                                    min: (*min).into(),
                                    max: (*max).into(),
                                },
                            );
                        }
                    }
                    (ValueType::F32(min), ValueType::F32(max)) => {
                        if let Some(item) = facets_minmax.get_mut(&facet.name) {
                            *item = MinMaxFieldJson {
                                min: min.min(item.min.as_f64().unwrap() as f32).into(),
                                max: max.min(item.max.as_f64().unwrap() as f32).into(),
                            }
                        } else {
                            facets_minmax.insert(
                                facet.name.clone(),
                                MinMaxFieldJson {
                                    min: (*min).into(),
                                    max: (*max).into(),
                                },
                            );
                        }
                    }
                    (ValueType::F64(min), ValueType::F64(max)) => {
                        if let Some(item) = facets_minmax.get_mut(&facet.name) {
                            *item = MinMaxFieldJson {
                                min: min.min(item.min.as_f64().unwrap()).into(),
                                max: max.min(item.max.as_f64().unwrap()).into(),
                            }
                        } else {
                            facets_minmax.insert(
                                facet.name.clone(),
                                MinMaxFieldJson {
                                    min: (*min).into(),
                                    max: (*max).into(),
                                },
                            );
                        }
                    }
                    _ => {}
                }
            }
        }
        facets_minmax
    }

    /// get_index_string_facets: list of string facet fields, each with field name and a map of unique values and their count (number of times the specific value appears in the whole index).
    /// values are sorted by their occurrence count within all indexed documents in descending order
    /// * `query_facets`: Must be set if facet fields should be returned in get_index_facets. If set to Vec::new() then no facet fields are returned.
    ///   The prefix property of a QueryFacet allows to filter the returned facet values to those matching a given prefix, if there are too many distinct values per facet field.
    ///   The length property of a QueryFacet allows limiting the number of returned distinct values per facet field, if there are too many distinct values.  The QueryFacet can be used to improve the usability in an UI.
    ///   If the length property of a QueryFacet is set to 0 then no facet values for that facet are returned.
    ///   The facet values are sorted by the frequency of the appearance of the value within the indexed documents matching the query in descending order.
    ///   Example: query_facets = vec![QueryFacet::String16 {field: "language".to_string(),prefix: "ger".to_string(),length: 5},QueryFacet::String16 {field: "brand".to_string(),prefix: "a".to_string(),length: 5}];
    pub async fn get_index_string_facets(
        &self,
        query_facets: Vec<QueryFacet>,
    ) -> Option<AHashMap<String, Facet>> {
        if self.facets.is_empty() {
            return None;
        }

        let mut result: AHashMap<String, Facet> = AHashMap::new();

        let mut result_facets: AHashMap<String, (AHashMap<String, usize>, u32)> = AHashMap::new();
        for query_facet in query_facets.iter() {
            match query_facet {
                QueryFacet::String16 {
                    field,
                    prefix: _,
                    length,
                } => {
                    result_facets.insert(field.into(), (AHashMap::new(), *length as u32));
                }
                QueryFacet::StringSet16 {
                    field,
                    prefix: _,
                    length,
                } => {
                    result_facets.insert(field.into(), (AHashMap::new(), *length as u32));
                }

                QueryFacet::String32 {
                    field,
                    prefix: _,
                    length,
                } => {
                    result_facets.insert(field.into(), (AHashMap::new(), *length));
                }
                QueryFacet::StringSet32 {
                    field,
                    prefix: _,
                    length,
                } => {
                    result_facets.insert(field.into(), (AHashMap::new(), *length));
                }

                _ => {}
            }
        }

        for shard_arc in self.shard_vec.iter() {
            let shard = shard_arc.read().await;
            if !shard.facets.is_empty() {
                for facet in shard.facets.iter() {
                    if let Some(existing) = result_facets.get_mut(&facet.name) {
                        for (key, value) in facet.values.iter() {
                            *existing.0.entry(key.clone()).or_insert(0) += value.1;
                        }
                    };
                }
            }
        }

        for (key, value) in result_facets.iter_mut() {
            let sum = value
                .0
                .iter()
                .sorted_unstable_by(|a, b| b.1.cmp(a.1))
                .map(|(a, c)| (a.clone(), *c))
                .take(value.1 as usize)
                .collect::<Vec<_>>();
            result.insert(key.clone(), sum);
        }

        Some(result)
    }

    /// Reset index to empty, while maintaining schema
    pub async fn clear_index(&mut self) {
        let index_path = Path::new(&self.index_path_string);
        let _ = fs::remove_file(index_path.join(DICTIONARY_FILENAME));
        if let Some(spelling_correction) = self.meta.spelling_correction.as_ref() {
            self.symspell_option = Some(Arc::new(RwLock::new(SymSpell::new(
                spelling_correction.max_dictionary_edit_distance,
                spelling_correction.term_length_threshold.clone(),
                7,
                spelling_correction.count_threshold,
            ))));
        }

        let _ = fs::remove_file(index_path.join(COMPLETIONS_FILENAME));
        if let Some(_query_completion) = self.meta.query_completion.as_ref() {
            self.completion_option = Some(Arc::new(RwLock::new(PruningRadixTrie::new())));
        }

        let mut result_object_list = Vec::new();
        for shard in self.shard_vec.iter() {
            let shard_clone = shard.clone();
            result_object_list.push(tokio::spawn(async move {
                shard_clone.write().await.clear_shard().await;
            }));
        }
        future::join_all(result_object_list).await;
    }

    /// Delete index from disc and ram
    pub fn delete_index(&mut self) {
        let index_path = Path::new(&self.index_path_string);

        let _ = fs::remove_file(index_path.join(DICTIONARY_FILENAME));
        let _ = fs::remove_file(index_path.join(COMPLETIONS_FILENAME));

        let _ = fs::remove_file(index_path.join(INDEX_FILENAME));
        let _ = fs::remove_file(index_path.join(SCHEMA_FILENAME));
        let _ = fs::remove_file(index_path.join(META_FILENAME));
        let _ = fs::remove_file(index_path.join(DELETE_FILENAME));
        let _ = fs::remove_file(index_path.join(FACET_FILENAME));
        let _ = fs::remove_file(index_path.join(FACET_VALUES_FILENAME));
        let _ = fs::remove_dir(index_path);
    }

    /// Get synonyms from index
    pub fn get_synonyms(&self) -> Result<Vec<Synonym>, String> {
        if let Ok(synonym_file) =
            File::open(Path::new(&self.index_path_string).join(SYNONYMS_FILENAME))
        {
            if let Ok(synonyms) = serde_json::from_reader(BufReader::new(synonym_file)) {
                Ok(synonyms)
            } else {
                Err("not found".into())
            }
        } else {
            Err("not found".into())
        }
    }

    /// Set/replace/overwrite synonyms in index
    /// Affects only subsequently indexed documents
    pub fn set_synonyms(&mut self, synonyms: &Vec<Synonym>) -> Result<usize, String> {
        serde_json::to_writer(
            &File::create(Path::new(&self.index_path_string).join(SYNONYMS_FILENAME)).unwrap(),
            &synonyms,
        )
        .unwrap();

        self.synonyms_map = get_synonyms_map(synonyms, self.segment_number_mask1);
        Ok(synonyms.len())
    }

    /// Add/append/update/merge synonyms in index
    /// Affects only subsequently indexed documents
    pub fn add_synonyms(&mut self, synonyms: &[Synonym]) -> Result<usize, String> {
        let mut merged_synonyms = if let Ok(synonym_file) =
            File::open(Path::new(&self.index_path_string).join(SYNONYMS_FILENAME))
        {
            serde_json::from_reader(BufReader::new(synonym_file)).unwrap_or_default()
        } else {
            Vec::new()
        };

        merged_synonyms.extend(synonyms.iter().cloned());

        serde_json::to_writer(
            &File::create(Path::new(&self.index_path_string).join(SYNONYMS_FILENAME)).unwrap(),
            &merged_synonyms,
        )
        .unwrap();

        self.synonyms_map = get_synonyms_map(&merged_synonyms, self.segment_number_mask1);
        Ok(merged_synonyms.len())
    }
}

/// Remove index from RAM (Reverse of open_index)
#[allow(async_fn_in_trait)]
pub trait Close {
    /// Remove index from RAM (Reverse of open_index)
    async fn close(&self);
}

/// Remove index from RAM (Reverse of open_index)
impl Close for IndexArc {
    /// Remove index from RAM (Reverse of open_index)
    async fn close(&self) {
        self.commit().await;

        if let Some(completion_option) = &self.read().await.completion_option.as_ref() {
            let trie = completion_option.read().await;
            let completions_path =
                Path::new(&self.read().await.index_path_string).join(COMPLETIONS_FILENAME);

            _ = trie.save_completions(&completions_path, ":");
        }

        if let Some(symspell) = &mut self.read().await.symspell_option.as_ref() {
            let dictionary_path =
                Path::new(&self.read().await.index_path_string).join(DICTIONARY_FILENAME);
            let _ = symspell.read().await.save_dictionary(&dictionary_path, " ");
        }

        let mut result_object_list = Vec::new();
        for shard in self.read().await.shard_vec.iter() {
            let shard_clone = shard.clone();
            result_object_list.push(tokio::spawn(async move {
                let mut mmap_options = MmapOptions::new();
                let mmap: MmapMut = mmap_options.len(4).map_anon().unwrap();
                shard_clone.write().await.index_file_mmap = mmap
                    .make_read_only()
                    .expect("Unable to make Mmap read-only");

                let mut mmap_options = MmapOptions::new();
                let mmap: MmapMut = mmap_options.len(4).map_anon().unwrap();
                shard_clone.write().await.docstore_file_mmap = mmap
                    .make_read_only()
                    .expect("Unable to make Mmap read-only");
            }));
        }
        future::join_all(result_object_list).await;
    }
}

/// Delete document from index by document id
#[allow(async_fn_in_trait)]
pub trait DeleteDocument {
    /// Delete document from index by document id
    async fn delete_document(&self, docid: u64);
}

/// Delete document from index by document id
/// Document ID can by obtained by search.
/// Immediately effective, indpendent of commit.
/// Index space used by deleted documents is not reclaimed (until compaction is implemented), but result_count_total is updated.
/// By manually deleting the delete.bin file the deleted documents can be recovered (until compaction).
/// Deleted documents impact performance, especially but not limited to counting (Count, TopKCount). They also increase the size of the index (until compaction is implemented).
/// For minimal query latency delete index and reindexing documents is preferred over deleting documents (until compaction is implemented).
/// BM25 scores are not updated (until compaction is implemented), but the impact is minimal.
impl DeleteDocument for IndexArc {
    async fn delete_document(&self, docid: u64) {
        let index_ref = self.read().await;
        let shard_id = docid & ((1 << index_ref.shard_bits) - 1);
        let doc_id = docid >> index_ref.shard_bits;
        let mut shard_mut = index_ref.shard_vec[shard_id as usize].write().await;

        if doc_id as usize >= shard_mut.indexed_doc_count {
            return;
        }
        if shard_mut.delete_hashset.insert(doc_id as usize) {
            let mut buffer: [u8; 8] = [0; 8];
            write_u64(doc_id, &mut buffer, 0);
            let _ = shard_mut.delete_file.write(&buffer);
            let _ = shard_mut.delete_file.flush();
        }
    }
}

/// Delete documents from index by document id
#[allow(async_fn_in_trait)]
pub trait DeleteDocuments {
    /// Delete documents from index by document id
    async fn delete_documents(&self, docid_vec: Vec<u64>);
}

/// Delete documents from index by document id
/// Document ID can by obtained by search.
/// Immediately effective, indpendent of commit.
/// Index space used by deleted documents is not reclaimed (until compaction is implemented), but result_count_total is updated.
/// By manually deleting the delete.bin file the deleted documents can be recovered (until compaction).
/// Deleted documents impact performance, especially but not limited to counting (Count, TopKCount). They also increase the size of the index (until compaction is implemented).
/// For minimal query latency delete index and reindexing documents is preferred over deleting documents (until compaction is implemented).
/// BM25 scores are not updated (until compaction is implemented), but the impact is minimal.
impl DeleteDocuments for IndexArc {
    async fn delete_documents(&self, docid_vec: Vec<u64>) {
        for docid in docid_vec {
            self.delete_document(docid).await;
        }
    }
}

/// Delete documents from index by query
/// Delete and search have identical parameters.
/// It is recommended to test with search prior to delete to verify that only those documents are returned that you really want to delete.
#[allow(clippy::too_many_arguments)]
#[allow(async_fn_in_trait)]
pub trait DeleteDocumentsByQuery {
    /// Delete documents from index by query
    /// Delete and search have identical parameters.
    /// It is recommended to test with search prior to delete to verify that only those documents are returned that you really want to delete.
    async fn delete_documents_by_query(
        &self,
        query_string: String,
        query_type_default: QueryType,
        offset: usize,
        length: usize,
        include_uncommited: bool,
        field_filter: Vec<String>,
        facet_filter: Vec<FacetFilter>,
        result_sort: Vec<ResultSort>,
    );
}

/// Delete documents from index by query
/// Delete and search have identical parameters.
/// It is recommended to test with search prior to delete to verify that only those documents are returned that you really want to delete.
impl DeleteDocumentsByQuery for IndexArc {
    async fn delete_documents_by_query(
        &self,
        query_string: String,
        query_type_default: QueryType,
        offset: usize,
        length: usize,
        include_uncommited: bool,
        field_filter: Vec<String>,
        facet_filter: Vec<FacetFilter>,
        result_sort: Vec<ResultSort>,
    ) {
        let rlo = self
            .search(
                query_string.to_owned(),
                query_type_default,
                offset,
                length,
                ResultType::Topk,
                include_uncommited,
                field_filter,
                Vec::new(),
                facet_filter,
                result_sort,
                QueryRewriting::SearchOnly,
            )
            .await;

        let document_id_vec: Vec<u64> = rlo
            .results
            .iter()
            .map(|result| result.doc_id as u64)
            .collect();
        self.delete_documents(document_id_vec).await;
    }
}

/// Update document in index
/// Update_document is a combination of delete_document and index_document.
/// All current limitations of delete_document apply.
#[allow(async_fn_in_trait)]
pub trait UpdateDocument {
    /// Update document in index
    /// Update_document is a combination of delete_document and index_document.
    /// All current limitations of delete_document apply.
    async fn update_document(&self, id_document: (u64, Document));
}

/// Update document in index
/// Update_document is a combination of delete_document and index_document.
/// All current limitations of delete_document apply.
impl UpdateDocument for IndexArc {
    async fn update_document(&self, id_document: (u64, Document)) {
        self.delete_document(id_document.0).await;
        self.index_document(id_document.1, FileType::None).await;
    }
}

/// Update documents in index
/// Update_document is a combination of delete_document and index_document.
/// All current limitations of delete_document apply.
#[allow(async_fn_in_trait)]
pub trait UpdateDocuments {
    /// Update documents in index
    /// Update_document is a combination of delete_document and index_document.
    /// All current limitations of delete_document apply.
    async fn update_documents(&self, id_document_vec: Vec<(u64, Document)>);
}

/// Update documents in index
/// Update_document is a combination of delete_document and index_document.
/// All current limitations of delete_document apply.
impl UpdateDocuments for IndexArc {
    async fn update_documents(&self, id_document_vec: Vec<(u64, Document)>) {
        let (docid_vec, document_vec): (Vec<_>, Vec<_>) = id_document_vec.into_iter().unzip();
        self.delete_documents(docid_vec).await;
        self.index_documents(document_vec).await;
    }
}

/// Indexes a list of documents
#[allow(async_fn_in_trait)]
pub trait IndexDocuments {
    /// Indexes a list of documents
    /// May block, if the threshold of documents indexed in parallel is exceeded.
    async fn index_documents(&self, document_vec: Vec<Document>);
}

impl IndexDocuments for IndexArc {
    /// Index list of documents (bulk)
    /// May block, if the threshold of documents indexed in parallel is exceeded.
    async fn index_documents(&self, document_vec: Vec<Document>) {
        for document in document_vec {
            self.index_document(document, FileType::None).await;
        }
    }
}

/// Indexes a single document
#[allow(async_fn_in_trait)]
pub trait IndexDocument {
    /// Indexes a single document
    /// May block, if the threshold of documents indexed in parallel is exceeded.
    async fn index_document(&self, document: Document, file: FileType);
}

impl IndexDocument for IndexArc {
    /// Index document
    /// May block, if the threshold of documents indexed in parallel is exceeded.
    async fn index_document(&self, document: Document, file: FileType) {
        while self.read().await.shard_queue.read().await.is_empty() {
            hint::spin_loop();
        }

        let index_arc_clone = self.clone();
        let index_id = self
            .read()
            .await
            .shard_queue
            .write()
            .await
            .pop_front()
            .unwrap();
        let index_shard = self.read().await.shard_vec[index_id].clone();

        let permit = index_shard
            .read()
            .await
            .permits
            .clone()
            .acquire_owned()
            .await
            .unwrap();

        tokio::spawn(async move {
            let index_id2 = index_id;
            index_shard.index_document_shard(document, file).await;

            index_arc_clone
                .read()
                .await
                .shard_queue
                .write()
                .await
                .push_back(index_id2);
            drop(permit);
        });
    }
}

/// Indexes a single document
#[allow(async_fn_in_trait)]
pub(crate) trait IndexDocumentShard {
    /// Indexes a single document
    /// May block, if the threshold of documents indexed in parallel is exceeded.
    async fn index_document_shard(&self, document: Document, file: FileType);
}

impl IndexDocumentShard for ShardArc {
    /// Index document
    /// May block, if the threshold of documents indexed in parallel is exceeded.
    async fn index_document_shard(&self, document: Document, file: FileType) {
        let shard_arc_clone = self.clone();
        let index_ref = self.read().await;
        let schema = index_ref.indexed_schema_vec.clone();
        let ngram_indexing = index_ref.meta.ngram_indexing;
        let indexed_field_vec_len = index_ref.indexed_field_vec.len();
        let tokenizer_type = index_ref.meta.tokenizer;
        let segment_number_mask1 = index_ref.segment_number_mask1;
        drop(index_ref);

        let token_per_field_max: u32 = u16::MAX as u32;
        let mut unique_terms: AHashMap<String, TermObject> = AHashMap::new();
        let mut field_vec: Vec<(usize, u8, u32, u32)> = Vec::new();
        let shard_ref2 = shard_arc_clone.read().await;

        for schema_field in schema.iter() {
            if !schema_field.indexed {
                continue;
            }

            let field_name = &schema_field.field;

            if let Some(field_value) = document.get(field_name) {
                let mut non_unique_terms: Vec<NonUniqueTermObject> = Vec::new();
                let mut nonunique_terms_count = 0u32;

                let text = match schema_field.field_type {
                    FieldType::Text | FieldType::String16 | FieldType::String32 => {
                        serde_json::from_value::<String>(field_value.clone())
                            .unwrap_or(field_value.to_string())
                    }
                    _ => field_value.to_string(),
                };

                let mut query_type_mut = QueryType::Union;

                tokenizer(
                    &shard_ref2,
                    &text,
                    &mut unique_terms,
                    &mut non_unique_terms,
                    tokenizer_type,
                    segment_number_mask1,
                    &mut nonunique_terms_count,
                    token_per_field_max,
                    MAX_POSITIONS_PER_TERM,
                    false,
                    &mut query_type_mut,
                    ngram_indexing,
                    schema_field.indexed_field_id,
                    indexed_field_vec_len,
                )
                .await;

                let document_length_compressed: u8 = int_to_byte4(nonunique_terms_count);
                let document_length_normalized: u32 =
                    DOCUMENT_LENGTH_COMPRESSION[document_length_compressed as usize];
                field_vec.push((
                    schema_field.indexed_field_id,
                    document_length_compressed,
                    document_length_normalized,
                    nonunique_terms_count,
                ));
            }
        }
        drop(shard_ref2);

        let ngrams: Vec<String> = unique_terms
            .iter()
            .filter(|term| term.1.ngram_type != NgramType::SingleTerm)
            .map(|term| term.1.term.clone())
            .collect();

        for term in ngrams.iter() {
            let ngram = unique_terms.get(term).unwrap();

            match ngram.ngram_type {
                NgramType::SingleTerm => {}
                NgramType::NgramFF | NgramType::NgramFR | NgramType::NgramRF => {
                    let term_ngram1 = ngram.term_ngram_1.clone();
                    let term_ngram2 = ngram.term_ngram_0.clone();

                    for indexed_field_id in 0..indexed_field_vec_len {
                        let positions_count_ngram1 =
                            unique_terms[&term_ngram1].field_positions_vec[indexed_field_id].len();
                        let positions_count_ngram2 =
                            unique_terms[&term_ngram2].field_positions_vec[indexed_field_id].len();
                        let ngram = unique_terms.get_mut(term).unwrap();

                        if positions_count_ngram1 > 0 {
                            ngram
                                .field_vec_ngram1
                                .push((indexed_field_id, positions_count_ngram1 as u32));
                        }
                        if positions_count_ngram2 > 0 {
                            ngram
                                .field_vec_ngram2
                                .push((indexed_field_id, positions_count_ngram2 as u32));
                        }
                    }
                }
                _ => {
                    let term_ngram1 = ngram.term_ngram_2.clone();
                    let term_ngram2 = ngram.term_ngram_1.clone();
                    let term_ngram3 = ngram.term_ngram_0.clone();

                    for indexed_field_id in 0..indexed_field_vec_len {
                        let positions_count_ngram1 =
                            unique_terms[&term_ngram1].field_positions_vec[indexed_field_id].len();
                        let positions_count_ngram2 =
                            unique_terms[&term_ngram2].field_positions_vec[indexed_field_id].len();
                        let positions_count_ngram3 =
                            unique_terms[&term_ngram3].field_positions_vec[indexed_field_id].len();
                        let ngram = unique_terms.get_mut(term).unwrap();

                        if positions_count_ngram1 > 0 {
                            ngram
                                .field_vec_ngram1
                                .push((indexed_field_id, positions_count_ngram1 as u32));
                        }
                        if positions_count_ngram2 > 0 {
                            ngram
                                .field_vec_ngram2
                                .push((indexed_field_id, positions_count_ngram2 as u32));
                        }
                        if positions_count_ngram3 > 0 {
                            ngram
                                .field_vec_ngram3
                                .push((indexed_field_id, positions_count_ngram3 as u32));
                        }
                    }
                }
            }
        }

        let document_item = DocumentItem {
            document,
            unique_terms,
            field_vec,
        };

        shard_arc_clone.index_document_2(document_item, file).await;
    }
}

#[allow(async_fn_in_trait)]
pub(crate) trait IndexDocument2 {
    async fn index_document_2(&self, document_item: DocumentItem, file: FileType);
}

impl IndexDocument2 for ShardArc {
    async fn index_document_2(&self, document_item: DocumentItem, file: FileType) {
        let mut shard_mut = self.write().await;

        let doc_id: usize = shard_mut.indexed_doc_count;
        shard_mut.indexed_doc_count += 1;

        let do_commit = shard_mut.block_id != doc_id >> 16;
        if do_commit {
            shard_mut.commit(doc_id).await;

            shard_mut.block_id = doc_id >> 16;
        }

        if !shard_mut.facets.is_empty() {
            let facets_size_sum = shard_mut.facets_size_sum;
            for i in 0..shard_mut.facets.len() {
                let facet = &mut shard_mut.facets[i];
                if let Some(field_value) = document_item.document.get(&facet.name) {
                    let address = (facets_size_sum * doc_id) + facet.offset;

                    match facet.field_type {
                        FieldType::U8 => {
                            let value = field_value.as_u64().unwrap_or_default() as u8;
                            match (&facet.min, &facet.max) {
                                (ValueType::U8(min), ValueType::U8(max)) => {
                                    if value < *min {
                                        facet.min = ValueType::U8(value);
                                    }
                                    if value > *max {
                                        facet.max = ValueType::U8(value);
                                    }
                                }
                                (ValueType::None, ValueType::None) => {
                                    facet.min = ValueType::U8(value);
                                    facet.max = ValueType::U8(value);
                                }
                                _ => {}
                            }
                            shard_mut.facets_file_mmap[address] = value
                        }
                        FieldType::U16 => {
                            let value = field_value.as_u64().unwrap_or_default() as u16;
                            match (&facet.min, &facet.max) {
                                (ValueType::U16(min), ValueType::U16(max)) => {
                                    if value < *min {
                                        facet.min = ValueType::U16(value);
                                    }
                                    if value > *max {
                                        facet.max = ValueType::U16(value);
                                    }
                                }
                                (ValueType::None, ValueType::None) => {
                                    facet.min = ValueType::U16(value);
                                    facet.max = ValueType::U16(value);
                                }
                                _ => {}
                            }
                            write_u16(value, &mut shard_mut.facets_file_mmap, address)
                        }
                        FieldType::U32 => {
                            let value = field_value.as_u64().unwrap_or_default() as u32;
                            match (&facet.min, &facet.max) {
                                (ValueType::U32(min), ValueType::U32(max)) => {
                                    if value < *min {
                                        facet.min = ValueType::U32(value);
                                    }
                                    if value > *max {
                                        facet.max = ValueType::U32(value);
                                    }
                                }
                                (ValueType::None, ValueType::None) => {
                                    facet.min = ValueType::U32(value);
                                    facet.max = ValueType::U32(value);
                                }
                                _ => {}
                            }
                            write_u32(value, &mut shard_mut.facets_file_mmap, address)
                        }
                        FieldType::U64 => {
                            let value = field_value.as_u64().unwrap_or_default();
                            match (&facet.min, &facet.max) {
                                (ValueType::U64(min), ValueType::U64(max)) => {
                                    if value < *min {
                                        facet.min = ValueType::U64(value);
                                    }
                                    if value > *max {
                                        facet.max = ValueType::U64(value);
                                    }
                                }
                                (ValueType::None, ValueType::None) => {
                                    facet.min = ValueType::U64(value);
                                    facet.max = ValueType::U64(value);
                                }
                                _ => {}
                            }
                            write_u64(value, &mut shard_mut.facets_file_mmap, address)
                        }
                        FieldType::I8 => {
                            let value = field_value.as_i64().unwrap_or_default() as i8;
                            match (&facet.min, &facet.max) {
                                (ValueType::I8(min), ValueType::I8(max)) => {
                                    if value < *min {
                                        facet.min = ValueType::I8(value);
                                    }
                                    if value > *max {
                                        facet.max = ValueType::I8(value);
                                    }
                                }
                                (ValueType::None, ValueType::None) => {
                                    facet.min = ValueType::I8(value);
                                    facet.max = ValueType::I8(value);
                                }
                                _ => {}
                            }
                            write_i8(value, &mut shard_mut.facets_file_mmap, address)
                        }
                        FieldType::I16 => {
                            let value = field_value.as_i64().unwrap_or_default() as i16;
                            match (&facet.min, &facet.max) {
                                (ValueType::I16(min), ValueType::I16(max)) => {
                                    if value < *min {
                                        facet.min = ValueType::I16(value);
                                    }
                                    if value > *max {
                                        facet.max = ValueType::I16(value);
                                    }
                                }
                                (ValueType::None, ValueType::None) => {
                                    facet.min = ValueType::I16(value);
                                    facet.max = ValueType::I16(value);
                                }
                                _ => {}
                            }
                            write_i16(value, &mut shard_mut.facets_file_mmap, address)
                        }
                        FieldType::I32 => {
                            let value = field_value.as_i64().unwrap_or_default() as i32;
                            match (&facet.min, &facet.max) {
                                (ValueType::I32(min), ValueType::I32(max)) => {
                                    if value < *min {
                                        facet.min = ValueType::I32(value);
                                    }
                                    if value > *max {
                                        facet.max = ValueType::I32(value);
                                    }
                                }
                                (ValueType::None, ValueType::None) => {
                                    facet.min = ValueType::I32(value);
                                    facet.max = ValueType::I32(value);
                                }
                                _ => {}
                            }
                            write_i32(value, &mut shard_mut.facets_file_mmap, address)
                        }
                        FieldType::I64 => {
                            let value = field_value.as_i64().unwrap_or_default();
                            match (&facet.min, &facet.max) {
                                (ValueType::I64(min), ValueType::I64(max)) => {
                                    if value < *min {
                                        facet.min = ValueType::I64(value);
                                    }
                                    if value > *max {
                                        facet.max = ValueType::I64(value);
                                    }
                                }
                                (ValueType::None, ValueType::None) => {
                                    facet.min = ValueType::I64(value);
                                    facet.max = ValueType::I64(value);
                                }
                                _ => {}
                            }
                            write_i64(value, &mut shard_mut.facets_file_mmap, address)
                        }
                        FieldType::Timestamp => {
                            let value = field_value.as_i64().unwrap_or_default();
                            match (&facet.min, &facet.max) {
                                (ValueType::Timestamp(min), ValueType::Timestamp(max)) => {
                                    if value < *min {
                                        facet.min = ValueType::Timestamp(value);
                                    }
                                    if value > *max {
                                        facet.max = ValueType::Timestamp(value);
                                    }
                                }
                                (ValueType::None, ValueType::None) => {
                                    facet.min = ValueType::Timestamp(value);
                                    facet.max = ValueType::Timestamp(value);
                                }
                                _ => {}
                            }

                            write_i64(value, &mut shard_mut.facets_file_mmap, address);
                        }
                        FieldType::F32 => {
                            let value = field_value.as_f64().unwrap_or_default() as f32;
                            match (&facet.min, &facet.max) {
                                (ValueType::F32(min), ValueType::F32(max)) => {
                                    if value < *min {
                                        facet.min = ValueType::F32(value);
                                    }
                                    if value > *max {
                                        facet.max = ValueType::F32(value);
                                    }
                                }
                                (ValueType::None, ValueType::None) => {
                                    facet.min = ValueType::F32(value);
                                    facet.max = ValueType::F32(value);
                                }
                                _ => {}
                            }

                            write_f32(value, &mut shard_mut.facets_file_mmap, address)
                        }
                        FieldType::F64 => {
                            let value = field_value.as_f64().unwrap_or_default();
                            match (&facet.min, &facet.max) {
                                (ValueType::F64(min), ValueType::F64(max)) => {
                                    if value < *min {
                                        facet.min = ValueType::F64(value);
                                    }
                                    if value > *max {
                                        facet.max = ValueType::F64(value);
                                    }
                                }
                                (ValueType::None, ValueType::None) => {
                                    facet.min = ValueType::F64(value);
                                    facet.max = ValueType::F64(value);
                                }
                                _ => {}
                            }

                            write_f64(value, &mut shard_mut.facets_file_mmap, address)
                        }
                        FieldType::String16 => {
                            if facet.values.len() < u16::MAX as usize {
                                let key = serde_json::from_value::<String>(field_value.clone())
                                    .unwrap_or(field_value.to_string());

                                let key_string = key.clone();
                                let key = vec![key];

                                facet.values.entry(key_string.clone()).or_insert((key, 0)).1 += 1;

                                let facet_value_id =
                                    facet.values.get_index_of(&key_string).unwrap() as u16;
                                write_u16(facet_value_id, &mut shard_mut.facets_file_mmap, address)
                            }
                        }

                        FieldType::StringSet16 => {
                            if facet.values.len() < u16::MAX as usize {
                                let mut key: Vec<String> =
                                    serde_json::from_value(field_value.clone()).unwrap();
                                key.sort();

                                let key_string = key.join("_");
                                facet.values.entry(key_string.clone()).or_insert((key, 0)).1 += 1;

                                let facet_value_id =
                                    facet.values.get_index_of(&key_string).unwrap() as u16;
                                write_u16(facet_value_id, &mut shard_mut.facets_file_mmap, address)
                            }
                        }

                        FieldType::String32 => {
                            if facet.values.len() < u32::MAX as usize {
                                let key = serde_json::from_value::<String>(field_value.clone())
                                    .unwrap_or(field_value.to_string());

                                let key_string = key.clone();
                                let key = vec![key];

                                facet.values.entry(key_string.clone()).or_insert((key, 0)).1 += 1;

                                let facet_value_id =
                                    facet.values.get_index_of(&key_string).unwrap() as u32;
                                write_u32(facet_value_id, &mut shard_mut.facets_file_mmap, address)
                            }
                        }

                        FieldType::StringSet32 => {
                            if facet.values.len() < u32::MAX as usize {
                                let mut key: Vec<String> =
                                    serde_json::from_value(field_value.clone()).unwrap();
                                key.sort();

                                let key_string = key.join("_");
                                facet.values.entry(key_string.clone()).or_insert((key, 0)).1 += 1;

                                let facet_value_id =
                                    facet.values.get_index_of(&key_string).unwrap() as u32;
                                write_u32(facet_value_id, &mut shard_mut.facets_file_mmap, address)
                            }
                        }

                        FieldType::Point => {
                            if let Ok(point) = serde_json::from_value::<Point>(field_value.clone())
                                && point.len() == 2
                            {
                                if point[0] >= -90.0
                                    && point[0] <= 90.0
                                    && point[1] >= -180.0
                                    && point[1] <= 180.0
                                {
                                    let morton_code = encode_morton_2_d(&point);
                                    write_u64(morton_code, &mut shard_mut.facets_file_mmap, address)
                                } else {
                                    println!(
                                        "outside valid coordinate range: {} {}",
                                        point[0], point[1]
                                    );
                                }
                            }
                        }

                        _ => {}
                    };
                }
            }
        }

        if !shard_mut.uncommitted {
            if shard_mut.segments_level0[0].positions_compressed.is_empty() {
                for strip0 in shard_mut.segments_level0.iter_mut() {
                    strip0.positions_compressed = vec![0; MAX_POSITIONS_PER_TERM * 2];
                }
            }
            shard_mut.uncommitted = true;
        }

        let mut longest_field_id: usize = 0;
        let mut longest_field_length: u32 = 0;
        for value in document_item.field_vec {
            if doc_id == 0 && value.3 > longest_field_length {
                longest_field_id = value.0;
                longest_field_length = value.3;
            }

            shard_mut.document_length_compressed_array[value.0][doc_id & 0b11111111_11111111] =
                value.1;
            shard_mut.positions_sum_normalized += value.2 as u64;
            shard_mut.indexed_field_vec[value.0].field_length_sum += value.2 as usize;
        }

        if doc_id == 0 {
            if !shard_mut.longest_field_auto {
                longest_field_id = shard_mut.longest_field_id;
            }
            shard_mut.longest_field_id = longest_field_id;
            shard_mut.indexed_field_vec[longest_field_id].is_longest_field = true;
            if shard_mut.indexed_field_vec.len() > 1 {
                println!(
                    "detect longest field id {} name {} length {}",
                    longest_field_id,
                    shard_mut.indexed_field_vec[longest_field_id].schema_field_name,
                    longest_field_length
                );
            }
        }

        let mut unique_terms = document_item.unique_terms;
        if !shard_mut.synonyms_map.is_empty() {
            let unique_terms_clone = unique_terms.clone();
            for term in unique_terms_clone.iter() {
                if term.1.ngram_type == NgramType::SingleTerm {
                    let synonym = shard_mut.synonyms_map.get(&term.1.key_hash).cloned();
                    if let Some(synonym) = synonym {
                        for synonym_term in synonym {
                            let mut term_clone = term.1.clone();
                            term_clone.key_hash = synonym_term.1.0;
                            term_clone.key0 = synonym_term.1.1;
                            term_clone.term = synonym_term.0.clone();

                            if let Some(existing) = unique_terms.get_mut(&synonym_term.0) {
                                existing
                                    .field_positions_vec
                                    .iter_mut()
                                    .zip(term_clone.field_positions_vec.iter())
                                    .for_each(|(x1, x2)| {
                                        x1.extend_from_slice(x2);
                                        x1.sort_unstable();
                                    });
                            } else {
                                unique_terms.insert(synonym_term.0.clone(), term_clone);
                            };
                        }
                    }
                }
            }
        }

        for term in unique_terms {
            shard_mut.index_posting(term.1, doc_id, false, 0, 0, 0);
        }

        match file {
            FileType::Path(file_path) => {
                if let Err(e) = shard_mut.copy_file(&file_path, doc_id) {
                    println!("can't copy PDF {} {}", file_path.display(), e);
                }
            }

            FileType::Bytes(file_path, file_bytes) => {
                if let Err(e) = shard_mut.write_file(&file_bytes, doc_id) {
                    println!("can't copy PDF {} {}", file_path.display(), e);
                }
            }

            _ => {}
        }

        if !shard_mut.stored_field_names.is_empty() {
            shard_mut.store_document(doc_id, document_item.document);
        }

        if do_commit {
            drop(shard_mut);
            warmup(self).await;
        }
    }
}

pub(crate) struct DocumentItem {
    pub document: Document,
    pub unique_terms: AHashMap<String, TermObject>,
    pub field_vec: Vec<(usize, u8, u32, u32)>,
}
