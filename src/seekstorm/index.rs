use add_result::decode_positions_commit;
use ahash::{AHashMap, AHashSet, HashSet, RandomState};
use derivative::Derivative;
use indexmap::IndexMap;
use itertools::Itertools;
use lazy_static::lazy_static;
use memmap2::{Mmap, MmapMut, MmapOptions};
use num::FromPrimitive;
use num_derive::FromPrimitive;

use num_format::{Locale, ToFormattedString};

use search::{decode_posting_list_object, QueryType, Search};
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use std::{
    collections::HashMap,
    fs::{self, File},
    io::{BufRead, BufReader, Read, Seek, Write},
    path::Path,
    sync::Arc,
    thread::available_parallelism,
    time::Instant,
};
use tokio::sync::{RwLock, Semaphore};
use utils::{read_u32, write_u16};

use crate::{
    add_result::{self, B, DOCUMENT_LENGTH_COMPRESSION, K, SIGMA},
    commit::KEY_HEAD_SIZE,
    geo_search::encode_morton_2_d,
    search::{self, FacetFilter, Point, QueryFacet, Ranges, ResultObject, ResultSort, ResultType},
    tokenizer::tokenizer,
    utils::{
        self, read_u16, read_u16_ref, read_u32_ref, read_u64, read_u64_ref, read_u8_ref, write_f32,
        write_f64, write_i16, write_i32, write_i64, write_i8, write_u32, write_u64,
    },
};

pub(crate) const INDEX_FILENAME: &str = "index.bin";
pub(crate) const DOCSTORE_FILENAME: &str = "docstore.bin";
pub(crate) const DELETE_FILENAME: &str = "delete.bin";
pub(crate) const SCHEMA_FILENAME: &str = "schema.json";
pub(crate) const META_FILENAME: &str = "index.json";
pub(crate) const FACET_FILENAME: &str = "facet.bin";
pub(crate) const FACET_VALUES_FILENAME: &str = "facet.json";

pub(crate) const VERSION: &str = env!("CARGO_PKG_VERSION");

const INDEX_HEADER_SIZE: u64 = 4;
/// Incompatible index  format change: new library can't open old format, and old library can't open new format
pub const INDEX_FORMAT_VERSION_MAJOR: u16 = 3;
/// Backward compatible format change: new library can open old format, but old library can't open new format
pub const INDEX_FORMAT_VERSION_MINOR: u16 = 2;

/// Maximum processed positions per term per document: default=65_536. E.g. 65,536 * 'the' per document, exceeding positions are ignored for search.
pub const MAX_POSITIONS_PER_TERM: usize = 65_536;
pub(crate) const STOP_BIT: u8 = 0b10000000;
pub(crate) const FIELD_STOP_BIT_1: u8 = 0b0010_0000;
pub(crate) const FIELD_STOP_BIT_2: u8 = 0b0100_0000;
/// maximum number of documents per block
pub const ROARING_BLOCK_SIZE: usize = 65_536;

pub(crate) const SPEEDUP_FLAG: bool = true;
pub(crate) const SORT_FLAG: bool = true;
pub(crate) const BIGRAM_FLAG: bool = true;

/// A document is a flattened, single level of key-value pairs, where key is an arbitrary string, and value represents any valid JSON value.
pub type Document = HashMap<String, serde_json::Value>;

/// Defines where the index resides during search: Ram (the complete index is preloaded to Ram when opening the index) or Mmap (the index is accessed via memory-mapped files). See architecture.md for details.
#[derive(Derivative, Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
pub enum AccessType {
    Ram = 0,
    Mmap = 1,
}

/// Similarity type defines the scoring and ranking of the search results: Bm25f or Bm25fProximity (considers term proximity, e.g. for implicit phrase search with improved relevancy)
#[derive(Derivative, Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
pub enum SimilarityType {
    Bm25f = 0,
    Bm25fProximity = 1,
}

/// Defines tokenizer behavior: AsciiAlphabetic (for benchmark compatibility) or UnicodeAlphanumeric (all Unicode alphanumeric chars are recognized as token)
#[derive(Derivative, Debug, Deserialize, Serialize, Clone, PartialEq, Eq, Copy)]
pub enum TokenizerType {
    AsciiAlphabetic = 0,
    UnicodeAlphanumeric = 1,
}

pub(crate) struct LevelIndex {
    pub document_length_compressed_array: Vec<[u8; ROARING_BLOCK_SIZE]>,

    pub docstore_pointer_docs: Vec<u8>,
    pub docstore_pointer_docs_pointer: usize,
    pub document_length_compressed_array_pointer: usize,
}

/// Posting lists are divided into blocks of a doc id range of 65.536 (16 bit).
/// Each block can be compressed with a different method.
#[derive(Default, Debug, Deserialize, Serialize, Derivative, Clone)]
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
    pub bigram_term_index1: u8,
    pub bigram_term_index2: u8,
    pub max_list_score: f32,
    pub blocks: Vec<BlockObjectIndex>,

    pub position_range_previous: u32,
}

#[derive(Default, Debug, Deserialize, Serialize, Derivative, Clone)]
pub(crate) struct PostingListObject0 {
    pub pointer_first: usize,
    pub pointer_last: usize,
    pub posting_count: usize,

    pub max_block_score: f32,
    pub max_docid: u16,
    pub max_p_docid: u16,
    pub is_bigram: bool,
    pub term_bigram1: String,
    pub term_bigram2: String,
    pub posting_count_bigram1: usize,
    pub posting_count_bigram2: usize,
    pub position_count: usize,
    pub pointer_pivot_p_docid: u16,
    pub size_compressed_positions_key: usize,
    pub docid_delta_max: u16,
    pub docid_old: u16,
    pub compression_type_pointer: u32,
}

/// Type of posting list compression.
#[derive(Default, Debug, Deserialize, Serialize, Derivative, Clone, PartialEq, FromPrimitive)]
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
    pub idf_bigram1: f32,
    pub idf_bigram2: f32,
    pub tf_bigram1: u32,
    pub tf_bigram2: u32,
    pub is_bigram: bool,

    pub end_flag: bool,
    pub end_flag_block: bool,
    pub is_embedded: bool,
    pub embedded_positions: [u32; 4],
    pub field_vec: SmallVec<[(u16, usize); 2]>,
    pub field_vec_bigram1: SmallVec<[(u16, usize); 2]>,
    pub field_vec_bigram2: SmallVec<[(u16, usize); 2]>,
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
            idf_bigram1: 0.0,
            idf_bigram2: 0.0,
            is_bigram: false,
            is_embedded: false,
            embedded_positions: [0; 4],
            field_vec: SmallVec::new(),
            tf_bigram1: 0,
            tf_bigram2: 0,
            field_vec_bigram1: SmallVec::new(),
            field_vec_bigram2: SmallVec::new(),

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
#[derive(Default, Debug, Derivative, Clone)]
pub(crate) struct SegmentLevel0 {
    pub segment: AHashMap<u64, PostingListObject0>,
    pub positions_compressed: Vec<u8>,
}

/// In the index schema the type for every field of the document is defined.
#[derive(Derivative, Debug, Deserialize, Serialize, Clone, PartialEq, Eq, Default)]
pub enum FieldType {
    U8,
    U16,
    U32,
    U64,
    I8,
    I16,
    I32,
    I64,
    F32,
    F64,
    Bool,
    #[default]
    String,
    StringSet,
    /// Point is a geographic field type: A Vec<f64> with two coordinate values (latitude and longitude) are internally encoded into a single u64 value (Morton code).
    /// Morton codes enable efficient range queries.
    /// Latitude and longitude are a pair of numbers (coordinates) used to describe a position on the plane of a geographic coordinate system.
    /// The numbers are in decimal degrees format and range from -90 to 90 for latitude and -180 to 180 for longitude.
    /// Coordinates are internally stored as u64 morton code: both f64 values are multiplied by 10_000_000, converted to i32 and bitwise interleaved into a single u64 morton code
    /// The conversion between longitude/latitude coordinates and Morton code is lossy due to rounding errors.
    Point,
    Text,
    Bytes,
    Json,
    Date,
}

/// Defines a field in index schema: field_name, field_stored, field_indexed , field_type, field_boost.
#[derive(Debug, Clone, Deserialize, Serialize)]
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
    /// both numerical and string fields can be indexed (field_indexed=true) and stored (field_stored=true) in the json document,
    /// but with field_facet=true they are additionally stored in a binary format, for fast faceting and sorting without docstore access (decompression, deserialization)
    #[serde(skip_serializing_if = "is_default_bool")]
    #[serde(default = "default_false")]
    pub facet: bool,

    /// optional custom weight factor for Bm25 ranking
    #[serde(skip_serializing_if = "is_default_f32")]
    #[serde(default = "default_1")]
    pub boost: f32,

    #[serde(skip)]
    pub(crate) indexed_field_id: usize,
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

/// Specifies SimilarityType, TokenizerType and AccessType when creating an new index
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct IndexMetaObject {
    #[serde(skip)]
    pub id: u64,
    pub name: String,

    pub similarity: SimilarityType,

    pub tokenizer: TokenizerType,

    pub access_type: AccessType,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct ResultFacet {
    pub field: String,
    pub values: AHashMap<u16, usize>,
    pub prefix: String,
    pub length: u16,
    pub ranges: Ranges,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub enum DistanceUnit {
    Kilometers,
    Miles,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DistanceField {
    pub field: String,
    pub distance: String,
    pub base: Point,
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

/// Facet field, with field name and a map of unique values and their count (number of times the specific value appears in the whole index).
#[derive(Deserialize, Serialize, Debug, Clone, Default)]
pub struct FacetField {
    /// Facet field name
    pub name: String,
    /// Vector of facet value names and their count
    pub values: IndexMap<String, (Vec<String>, usize)>,
    #[serde(skip)]
    pub(crate) offset: usize,
    #[serde(skip)]
    pub(crate) field_type: FieldType,
}

/// Facet field, with field name and a vector of unique values and their count (number of times the specific value appears in the whole index).
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct Facet {
    /// Facet field name
    pub field: String,
    /// Vector of facet value names and their count
    /// The number of distinct string values and numerical ranges per facet field (cardinality) is limited to 65_536.
    /// Once that number is reached, the facet field is not updated anymore (no new values are added, no existing values are counted).
    pub values: Vec<(String, usize)>,
}

/// Index wrapped in Arc and RwLock for concurrent read and write access.
pub type IndexArc = Arc<RwLock<Index>>;

/// The root object of the index. It contains all levels and all segments of the index.
/// It also contains all properties that control indexing and intersection.
pub struct Index {
    /// Incompatible index  format change: new library can't open old format, and old library can't open new format
    pub index_format_version_major: u16,
    /// Backward compatible format change: new library can open old format, but old library can't open new format
    pub index_format_version_minor: u16,
    /// List of stored fields in the index: get_document and highlighter work only with stored fields
    pub stored_field_names: Vec<String>,

    /// Number of indexed documents
    pub indexed_doc_count: usize,
    /// Number of comitted documents
    pub committed_doc_count: usize,
    pub(crate) is_last_level_incomplete: bool,
    pub(crate) last_level_index_file_start_pos: u64,
    pub(crate) last_level_docstore_file_start_pos: u64,
    /// Number of allowed parallel indexed documents (default=available_parallelism). Can be used to detect wehen all indexing processes are finished.
    pub permits: Arc<Semaphore>,
    /// Defines a field in index schema: field_name, field_stored, field_indexed , field_type, field_boost.
    pub schema_map: HashMap<String, SchemaField>,
    /// Specifies SimilarityType, TokenizerType and AccessType when creating an new index
    pub meta: IndexMetaObject,

    pub(crate) hasher_32: RandomState,
    pub(crate) hasher_64: RandomState,

    pub(crate) stopword_posting_counts: [u32; STOPWORDS.len()],

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
    /// The index countains indexed, but uncommitted documents. Documents will either committed automatically once the number exceeds 64K documents, or once commit is invoked manually.
    pub uncommitted: bool,

    pub(crate) enable_bigram: bool,
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
    pub(crate) stopword_results: AHashMap<String, ResultObject>,

    pub(crate) facets: Vec<FacetField>,
    pub(crate) facets_map: AHashMap<String, usize>,
    pub(crate) facets_size_sum: usize,
    pub(crate) facets_file: File,
    pub(crate) facets_file_mmap: MmapMut,
    pub(crate) bm25_component_cache: [f32; 256],

    pub(crate) string_set_to_single_term_id_vec: Vec<AHashMap<String, AHashSet<u16>>>,
}

/// Get the version of the SeekStorm search library
pub fn version() -> &'static str {
    VERSION
}

/// Create index in RAM.
/// Inner data structures for create index and open_index
/// * `index_path` - index path.  
/// * `meta` - index meta object.  
/// * `schema` - schema.  
/// * `serialize_schema` - serialize schema.  
/// * `segment_number_bits1` - number of index segments: e.g. 11 bits for 2048 segments.  
/// * `mute` - prevent emitting status messages (e.g. when using pipes for data interprocess communication).  
pub fn create_index(
    index_path: &Path,
    meta: IndexMetaObject,
    schema: &Vec<SchemaField>,
    serialize_schema: bool,
    segment_number_bits1: usize,
    mute: bool,
) -> Result<Index, String> {
    let index_path_buf = index_path.to_path_buf();
    let index_path_string = index_path_buf.to_str().unwrap();

    if !index_path.exists() {
        if !mute {
            println!("index path created: {} ", index_path_string);
        }
        fs::create_dir_all(index_path).unwrap();
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
            for schema_field in schema.iter() {
                let mut schema_field_clone = schema_field.clone();
                schema_field_clone.indexed_field_id = indexed_field_vec.len();
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
                        FieldType::F32 => 4,
                        FieldType::F64 => 8,
                        FieldType::String => 2,
                        FieldType::StringSet => 2,
                        FieldType::Point => 8,
                        _ => 1,
                    };

                    facets_map.insert(schema_field.field.clone(), facets_vec.len());
                    facets_vec.push(FacetField {
                        name: schema_field.field.clone(),
                        values: IndexMap::new(),
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
                (u64::BITS - (indexed_field_vec.len() - 1).leading_zeros()) as usize;

            let hasher_32 = RandomState::with_seeds(805272099, 242851902, 646123436, 591410655);
            let hasher_64 = RandomState::with_seeds(808259318, 750368348, 84901999, 789810389);

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

            if !facets_vec.is_empty() {
                if let Ok(file) = File::open(Path::new(index_path).join(FACET_VALUES_FILENAME)) {
                    if let Ok(facets) = serde_json::from_reader(BufReader::new(file)) {
                        let mut facets: Vec<FacetField> = facets;
                        if facets_vec.len() == facets.len() {
                            for i in 0..facets.len() {
                                facets[i].offset = facets_vec[i].offset;
                                facets[i].field_type = facets_vec[i].field_type.clone();
                            }
                        }
                        facets_vec = facets;
                    }
                }
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

            let facets_len = facets_vec.len();

            let mut index = Index {
                index_format_version_major: INDEX_FORMAT_VERSION_MAJOR,
                index_format_version_minor: INDEX_FORMAT_VERSION_MINOR,
                hasher_32,
                hasher_64,
                stopword_posting_counts: [0; STOPWORDS.len()],
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
                enable_bigram: BIGRAM_FLAG,
                enable_fallback: false,
                enable_single_term_topk: false,
                enable_search_quality_test: false,
                enable_inter_query_threading: false,
                enable_inter_query_threading_auto: false,
                schema_map,
                indexed_field_id_bits,
                indexed_field_id_mask: (1usize << indexed_field_id_bits) - 1,
                longest_field_id: 0,
                indexed_field_vec,
                indexed_schema_vec,
                meta,
                document_length_compressed_array,
                key_count_sum: 0,

                block_id: 0,
                strip_compressed_sum: 0,
                postings_buffer: vec![0; 400_000_000],
                postings_buffer_pointer: 0,

                docid_count: 0,
                size_compressed_docid_index: 0,
                size_compressed_positions_index: 0,
                position_count: 0,
                postinglist_count: 0,
                permits: Arc::new(Semaphore::new(available_parallelism().unwrap().get())),
                mute,
                stopword_results: AHashMap::new(),
                facets: facets_vec,
                facets_map,
                facets_size_sum,
                facets_file,
                facets_file_mmap,
                string_set_to_single_term_id_vec: vec![AHashMap::new(); facets_len],
                bm25_component_cache: [0.0; 256],
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

            index.segment_number1 = 1usize << index.segment_number_bits1;
            index.segment_number_mask1 = (1u32 << index.segment_number_bits1) - 1;
            index.segments_level0 = vec![
                SegmentLevel0 {
                    ..Default::default()
                };
                index.segment_number1 as usize
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
    index: &Index,
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
    index: &Index,
    segment: &SegmentIndex,
    bigram_term_index1: u8,
    bigram_term_index2: u8,
    posting_count: u32,
    block_id: usize,
    max_docid: usize,
    max_p_docid: usize,
    pointer_pivot_p_docid: usize,
    compression_type_pointer: u32,
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
    let mut field_vec_bigram1 = SmallVec::new();
    let mut field_vec_bigram2 = SmallVec::new();

    decode_positions_commit(
        posting_pointer_size,
        embed_flag,
        byte_array,
        positions_pointer,
        bigram_term_index1 < 255,
        index.indexed_field_vec.len(),
        index.indexed_field_id_bits,
        index.indexed_field_id_mask,
        index.longest_field_id as u16,
        &mut field_vec,
        &mut field_vec_bigram1,
        &mut field_vec_bigram2,
    );

    if bigram_term_index1 == 255 || index.meta.similarity == SimilarityType::Bm25fProximity {
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
    } else {
        let posting_count1 = index.stopword_posting_counts[bigram_term_index1 as usize];
        let posting_count2 = index.stopword_posting_counts[bigram_term_index2 as usize];

        let idf_bigram1 = (((index.indexed_doc_count as f32 - posting_count1 as f32 + 0.5)
            / (posting_count1 as f32 + 0.5))
            + 1.0)
            .ln();

        let idf_bigram2 = (((index.indexed_doc_count as f32 - posting_count2 as f32 + 0.5)
            / (posting_count2 as f32 + 0.5))
            + 1.0)
            .ln();

        for field in field_vec_bigram1.iter() {
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

            let tf_bigram1 = field.1 as f32;

            let weight = index.indexed_schema_vec[field.0 as usize].boost;

            bm25f += weight
                * idf_bigram1
                * ((tf_bigram1 * (K + 1.0)
                    / (tf_bigram1 + (K * (1.0 - B + (B * document_length_quotient)))))
                    + SIGMA);
        }

        for field in field_vec_bigram2.iter() {
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

            let tf_bigram2 = field.1 as f32;

            let weight = index.indexed_schema_vec[field.0 as usize].boost;

            bm25f += weight
                * idf_bigram2
                * ((tf_bigram2 * (K + 1.0)
                    / (tf_bigram2 + (K * (1.0 - B + (B * document_length_quotient)))))
                    + SIGMA);
        }
    }
    bm25f
}

pub(crate) fn update_stopwords_posting_counts(
    index: &mut Index,
    update_last_block_with_level0: bool,
) {
    for (i, stopword) in STOPWORDS.iter().enumerate() {
        let index_ref = &*index;

        let term_bytes = stopword.as_bytes();
        let key0 = HASHER_32.hash_one(term_bytes) as u32 & index_ref.segment_number_mask1;
        let key_hash = HASHER_64.hash_one(term_bytes);

        index.stopword_posting_counts[i] = if index.meta.access_type == AccessType::Mmap {
            let plo_option = decode_posting_list_object(
                &index_ref.segments_index[key0 as usize],
                index_ref,
                key_hash,
                false,
            );
            if let Some(plo) = plo_option {
                plo.posting_count
            } else {
                0
            }
        } else if let Some(plo) = index_ref.segments_index[key0 as usize]
            .segment
            .get(&key_hash)
        {
            plo.posting_count
        } else {
            0
        };

        if update_last_block_with_level0 {
            if let Some(x) = index.segments_level0[key0 as usize].segment.get(&key_hash) {
                index.stopword_posting_counts[i] += x.posting_count as u32;
            }
        }
    }
}

pub(crate) fn update_list_max_impact_score(index: &mut Index) {
    update_stopwords_posting_counts(index, false);

    if index.meta.access_type == AccessType::Mmap {
        return;
    }

    for key0 in 0..index.segment_number1 {
        let keys: Vec<u64> = index.segments_index[key0].segment.keys().cloned().collect();
        for key in keys {
            let blocks_len = index.segments_index[key0].segment[&key].blocks.len();
            let mut max_list_score = 0.0;
            for block_index in 0..blocks_len {
                let segment = &index.segments_index[key0];
                let posting_list = &segment.segment[&key];
                let block = &posting_list.blocks[block_index];
                let max_block_score = get_max_score(
                    index,
                    segment,
                    posting_list.bigram_term_index1,
                    posting_list.bigram_term_index2,
                    posting_list.posting_count,
                    block.block_id as usize,
                    block.max_docid as usize,
                    block.max_p_docid as usize,
                    block.pointer_pivot_p_docid as usize,
                    block.compression_type_pointer,
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

/// Loads the index from disk into RAM.
/// * `index_path` - index path.  
/// * `mute` - prevent emitting status messages (e.g. when using pipes for data interprocess communication).  
pub async fn open_index(index_path: &Path, mute: bool) -> Result<IndexArc, String> {
    if !mute {
        println!("opening index ...");
    }

    let start_time = Instant::now();

    let mut index_mmap_position = INDEX_HEADER_SIZE as usize;
    let mut docstore_mmap_position = 0;

    match File::open(Path::new(index_path).join(META_FILENAME)) {
        Ok(file) => {
            let meta = serde_json::from_reader(BufReader::new(file)).unwrap();

            match File::open(Path::new(index_path).join(SCHEMA_FILENAME)) {
                Ok(file) => {
                    let schema = serde_json::from_reader(BufReader::new(file)).unwrap();

                    match create_index(index_path, meta, &schema, false, 11, false) {
                        Ok(mut index) => {
                            let mut block_count_sum = 0;

                            let is_mmap = index.meta.access_type == AccessType::Mmap;

                            let file_len = if is_mmap {
                                index.index_file_mmap.len() as u64
                            } else {
                                index.index_file.metadata().unwrap().len()
                            };

                            while if is_mmap {
                                index_mmap_position as u64
                            } else {
                                index.index_file.stream_position().unwrap()
                            } < file_len
                            {
                                let mut segment_head_vec: Vec<(u32, u32)> = Vec::new();
                                for key0 in 0..index.segment_number1 {
                                    if key0 == 0 {
                                        index.last_level_index_file_start_pos = if is_mmap {
                                            index_mmap_position as u64
                                        } else {
                                            index.index_file.stream_position().unwrap()
                                        };

                                        index.last_level_docstore_file_start_pos = if is_mmap {
                                            docstore_mmap_position as u64
                                        } else {
                                            index.docstore_file.stream_position().unwrap()
                                        };

                                        if index.level_index.is_empty() {
                                            let longest_field_id = if is_mmap {
                                                read_u16_ref(
                                                    &index.index_file_mmap,
                                                    &mut index_mmap_position,
                                                )
                                                    as usize
                                            } else {
                                                let _ = index.index_file.read(
                                                    &mut index
                                                        .compressed_index_segment_block_buffer
                                                        [0..2],
                                                );
                                                read_u16(
                                                    &index.compressed_index_segment_block_buffer,
                                                    0,
                                                )
                                                    as usize
                                            };

                                            for indexed_field in index.indexed_field_vec.iter_mut()
                                            {
                                                indexed_field.is_longest_field = indexed_field
                                                    .indexed_field_id
                                                    == longest_field_id;

                                                if indexed_field.is_longest_field {
                                                    index.longest_field_id = longest_field_id
                                                }
                                            }
                                        }

                                        let mut document_length_compressed_array_vec: Vec<
                                            [u8; ROARING_BLOCK_SIZE],
                                        > = Vec::new();

                                        let document_length_compressed_array_pointer = if is_mmap {
                                            index_mmap_position
                                        } else {
                                            index.index_file.stream_position().unwrap() as usize
                                        };

                                        for _i in 0..index.indexed_field_vec.len() {
                                            if is_mmap {
                                                index_mmap_position += ROARING_BLOCK_SIZE;
                                            } else {
                                                let mut document_length_compressed_array_item =
                                                    [0u8; ROARING_BLOCK_SIZE];

                                                let _ = index.index_file.read(
                                                    &mut document_length_compressed_array_item,
                                                );
                                                document_length_compressed_array_vec
                                                    .push(document_length_compressed_array_item);
                                            }
                                        }

                                        let mut docstore_pointer_docs: Vec<u8> = Vec::new();

                                        let mut docstore_pointer_docs_pointer = 0;
                                        if !index.stored_field_names.is_empty() {
                                            if is_mmap {
                                                let docstore_pointer_docs_size = read_u32_ref(
                                                    &index.docstore_file_mmap,
                                                    &mut docstore_mmap_position,
                                                )
                                                    as usize;
                                                docstore_pointer_docs_pointer =
                                                    docstore_mmap_position;
                                                docstore_mmap_position +=
                                                    docstore_pointer_docs_size;
                                            } else {
                                                let _ = index.docstore_file.read(
                                                    &mut index
                                                        .compressed_index_segment_block_buffer
                                                        [0..4],
                                                );

                                                let docstore_pointer_docs_size = read_u32(
                                                    &index.compressed_index_segment_block_buffer,
                                                    0,
                                                )
                                                    as usize;

                                                docstore_pointer_docs_pointer =
                                                    index.docstore_file.stream_position().unwrap()
                                                        as usize;
                                                docstore_pointer_docs =
                                                    vec![0; docstore_pointer_docs_size];
                                                let _ = index
                                                    .docstore_file
                                                    .read(&mut docstore_pointer_docs);
                                            }
                                        }

                                        if is_mmap {
                                            index.indexed_doc_count = read_u64_ref(
                                                &index.index_file_mmap,
                                                &mut index_mmap_position,
                                            )
                                                as usize;
                                            index.positions_sum_normalized = read_u64_ref(
                                                &index.index_file_mmap,
                                                &mut index_mmap_position,
                                            );

                                            for _key0 in 0..index.segment_number1 {
                                                let block_length = read_u32_ref(
                                                    &index.index_file_mmap,
                                                    &mut index_mmap_position,
                                                );
                                                let key_count = read_u32_ref(
                                                    &index.index_file_mmap,
                                                    &mut index_mmap_position,
                                                );

                                                segment_head_vec.push((block_length, key_count));
                                            }
                                        } else {
                                            let _ = index.index_file.read(
                                                &mut index.compressed_index_segment_block_buffer
                                                    [0..16],
                                            );

                                            index.indexed_doc_count = read_u64(
                                                &index.compressed_index_segment_block_buffer,
                                                0,
                                            )
                                                as usize;

                                            index.positions_sum_normalized = read_u64(
                                                &index.compressed_index_segment_block_buffer,
                                                8,
                                            );

                                            for _key0 in 0..index.segment_number1 {
                                                let _ = index.index_file.read(
                                                    &mut index
                                                        .compressed_index_segment_block_buffer
                                                        [0..8],
                                                );

                                                let block_length = read_u32(
                                                    &index.compressed_index_segment_block_buffer,
                                                    0,
                                                );
                                                let key_count = read_u32(
                                                    &index.compressed_index_segment_block_buffer,
                                                    4,
                                                );
                                                segment_head_vec.push((block_length, key_count));
                                            }
                                        }

                                        index.document_length_normalized_average =
                                            index.positions_sum_normalized as f32
                                                / index.indexed_doc_count as f32;

                                        index.level_index.push(LevelIndex {
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
                                        (block_count_sum >> index.segment_number_bits1) as u32;
                                    block_count_sum += 1;

                                    let key_body_pointer_write_start: u32 =
                                        key_count * KEY_HEAD_SIZE as u32;

                                    if is_mmap {
                                        index_mmap_position += key_count as usize * KEY_HEAD_SIZE;
                                        index.segments_index[key0].byte_array_blocks_pointer.push(
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
                                        let _ = index.index_file.read(
                                            &mut index.compressed_index_segment_block_buffer
                                                [0..(key_count as usize * KEY_HEAD_SIZE)],
                                        );
                                        let compressed_index_segment_block_buffer = &index
                                            .compressed_index_segment_block_buffer
                                            [0..(key_count as usize * KEY_HEAD_SIZE)];

                                        let mut block_array: Vec<u8> = vec![
                                            0;
                                            (block_length - key_body_pointer_write_start)
                                                as usize
                                        ];

                                        let _ = index.index_file.read(&mut block_array);
                                        index.segments_index[key0]
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

                                            let bigram_term_index1 = read_u8_ref(
                                                compressed_index_segment_block_buffer,
                                                &mut read_pointer,
                                            );

                                            let bigram_term_index2 = read_u8_ref(
                                                compressed_index_segment_block_buffer,
                                                &mut read_pointer,
                                            );

                                            let pointer_pivot_p_docid = read_u16_ref(
                                                compressed_index_segment_block_buffer,
                                                &mut read_pointer,
                                            );

                                            let compression_type_pointer = read_u32_ref(
                                                compressed_index_segment_block_buffer,
                                                &mut read_pointer,
                                            );

                                            if let Some(value) = index.segments_index[key0]
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
                                                    bigram_term_index1,
                                                    bigram_term_index2,
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
                                                };
                                                index.segments_index[key0]
                                                    .segment
                                                    .insert(key_hash, value);
                                            };

                                            if index.indexed_doc_count % ROARING_BLOCK_SIZE > 0
                                                && block_id as usize
                                                    == index.indexed_doc_count / ROARING_BLOCK_SIZE
                                                && index.meta.access_type == AccessType::Ram
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
                                                                let byte_array_docid = &index
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

                                                let plo = index.segments_index[key0]
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

                            index.committed_doc_count = index.indexed_doc_count;
                            index.is_last_level_incomplete =
                                index.committed_doc_count % ROARING_BLOCK_SIZE > 0;

                            for (i, component) in index.bm25_component_cache.iter_mut().enumerate()
                            {
                                let document_length_quotient = DOCUMENT_LENGTH_COMPRESSION[i]
                                    as f32
                                    / index.document_length_normalized_average;
                                *component = K * (1.0 - B + B * document_length_quotient);
                            }

                            index.string_set_to_single_term_id();

                            update_list_max_impact_score(&mut index);

                            let mut reader = BufReader::with_capacity(8192, &index.delete_file);
                            loop {
                                let Ok(buffer) = reader.fill_buf() else { break };

                                let length = buffer.len();

                                if length == 0 {
                                    break;
                                }

                                for i in (0..length).step_by(8) {
                                    let docid = read_u64(buffer, i);
                                    index.delete_hashset.insert(docid as usize);
                                }

                                reader.consume(length);
                            }

                            let elapsed_time = start_time.elapsed().as_nanos();

                            if !mute {
                                println!(
                        "{} name {} id {} version {} {} level {} fields {} {} facets {} docs {} deleted {} segments {} time {} s",
                        INDEX_FILENAME,
                        index.meta.name,
                        index.meta.id,
                        index.index_format_version_major.to_string() + "." + &index.index_format_version_minor.to_string(),
                        INDEX_FORMAT_VERSION_MAJOR.to_string() + "." + &INDEX_FORMAT_VERSION_MINOR.to_string(),

                        index.level_index.len(),

                        index.indexed_field_vec.len(),
                        index.schema_map.len(),
                        index.facets.len(),

                        index.indexed_doc_count.to_formatted_string(&Locale::en),
                        index.delete_hashset.len().to_formatted_string(&Locale::en),
                        index.segment_number1,
                        elapsed_time/1_000_000_000
                    );
                            }
                            let index_arc = Arc::new(RwLock::new(index));
                            warmup(&index_arc).await;
                            Ok(index_arc)
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

pub(crate) async fn warmup(index_object_arc: &IndexArc) {
    index_object_arc.write().await.stopword_results.clear();
    let mut query_facets: Vec<QueryFacet> = Vec::new();
    for facet in index_object_arc.read().await.facets.iter() {
        match facet.field_type {
            FieldType::String => query_facets.push(QueryFacet::String {
                field: facet.name.clone(),
                prefix: "".into(),
                length: u16::MAX,
            }),
            FieldType::StringSet => query_facets.push(QueryFacet::StringSet {
                field: facet.name.clone(),
                prefix: "".into(),
                length: u16::MAX,
            }),
            _ => {}
        }
    }

    for stopword in STOPWORDS {
        let results_list = index_object_arc
            .search(
                stopword.to_owned(),
                QueryType::Union,
                0,
                1000,
                search::ResultType::TopkCount,
                false,
                Vec::new(),
                query_facets.clone(),
                Vec::new(),
                Vec::new(),
            )
            .await;

        let mut index_mut = index_object_arc.write().await;
        index_mut
            .stopword_results
            .insert(stopword.to_string(), results_list);
    }
}

#[derive(Default, Debug, Deserialize, Serialize, Derivative, Clone)]
pub(crate) struct TermObject {
    pub key_hash: u64,
    pub key0: u32,
    pub term: String,

    pub is_bigram: bool,
    pub term_bigram1: String,
    pub term_bigram2: String,
    pub field_vec_bigram1: Vec<(usize, u32)>,
    pub field_vec_bigram2: Vec<(usize, u32)>,

    pub field_positions_vec: Vec<Vec<u16>>,
}

#[derive(Default, Debug, Serialize, Deserialize, Clone)]
pub(crate) struct NonUniqueTermObject {
    pub term: String,
    pub term_bigram1: String,
    pub term_bigram2: String,
    pub is_bigram: bool,
    pub op: QueryType,
}

lazy_static! {
    pub(crate) static ref HASHER_32: RandomState =
        RandomState::with_seeds(805272099, 242851902, 646123436, 591410655);
    pub(crate) static ref HASHER_64: RandomState =
        RandomState::with_seeds(808259318, 750368348, 84901999, 789810389);
    pub(crate) static ref STOPWORD_HASHSET: HashSet<u64> = STOPWORDS
        .iter()
        .map(|&x| HASHER_64.hash_one(x.as_bytes()))
        .collect();
}

pub(crate) const STOPWORDS: [&str; 40] = [
    "a", "all", "an", "and", "are", "as", "at", "be", "but", "by", "for", "if", "in", "into", "is",
    "it", "most", "new", "no", "not", "of", "on", "only", "or", "r", "such", "that", "the",
    "their", "then", "there", "these", "they", "this", "to", "up", "was", "who", "will", "with",
];

/// Compress termFrequency : 90 -> 88, 96 = index of previous smaller number
pub(crate) fn norm_frequency(term_frequency: u32) -> u8 {
    match DOCUMENT_LENGTH_COMPRESSION.binary_search(&term_frequency) {
        Ok(term_frequency_compressed) => term_frequency_compressed as u8,
        Err(term_frequency_compressed2) => term_frequency_compressed2 as u8 - 1,
    }
}

impl Index {
    /// Get number of index levels. One index level comprises 64K documents.
    pub fn level_count(index: &Index) -> usize {
        index.level_index.len()
    }

    /// Get number of facets defined in the index schema.
    pub fn get_facets_count(&self) -> usize {
        self.facets.len()
    }

    /// get_index_string_facets: list of string facet fields, each with field name and a map of unique values and their count (number of times the specific value appears in the whole index).
    /// values are sorted by their occurrence count within all indexed documents in descending order
    /// * `query_facets`: Must be set if facet fields should be returned in get_index_facets. If set to Vec::new() then no facet fields are returned.
    ///    The prefix property of a QueryFacet allows to filter the returned facet values to those matching a given prefix, if there are too many distinct values per facet field.
    ///    The length property of a QueryFacet allows limiting the number of returned distinct values per facet field, if there are too many distinct values.  The QueryFacet can be used to improve the usability in an UI.
    ///    If the length property of a QueryFacet is set to 0 then no facet values for that facet are returned.
    ///    The facet values are sorted by the frequency of the appearance of the value within the indexed documents matching the query in descending order.
    ///    Example: query_facets = vec![QueryFacet::String {field: "language".to_string(),prefix: "ger".to_string(),length: 5},QueryFacet::String {field: "brand".to_string(),prefix: "a".to_string(),length: 5}];
    pub fn get_index_string_facets(&self, query_facets: Vec<QueryFacet>) -> Option<Vec<Facet>> {
        if self.facets.is_empty() {
            return None;
        }

        let mut result_query_facets = Vec::new();
        if !query_facets.is_empty() {
            result_query_facets = vec![ResultFacet::default(); self.facets.len()];
            for query_facet in query_facets.iter() {
                match &query_facet {
                    QueryFacet::String {
                        field,
                        prefix,
                        length,
                    } => {
                        if let Some(idx) = self.facets_map.get(field) {
                            if self.facets[*idx].field_type == FieldType::String {
                                result_query_facets[*idx] = ResultFacet {
                                    field: field.clone(),
                                    prefix: prefix.clone(),
                                    length: *length,
                                    ..Default::default()
                                }
                            }
                        }
                    }
                    QueryFacet::StringSet {
                        field,
                        prefix,
                        length,
                    } => {
                        if let Some(idx) = self.facets_map.get(field) {
                            if self.facets[*idx].field_type == FieldType::StringSet {
                                result_query_facets[*idx] = ResultFacet {
                                    field: field.clone(),
                                    prefix: prefix.clone(),
                                    length: *length,
                                    ..Default::default()
                                }
                            }
                        }
                    }
                    _ => {}
                };
            }
        }

        let mut facets: Vec<Facet> = Vec::new();
        for (i, facet) in result_query_facets.iter().enumerate() {
            if facet.length == 0 || self.facets[i].values.is_empty() {
                continue;
            }

            if self.facets[i].field_type == FieldType::StringSet {
                let mut hash_map: AHashMap<String, usize> = AHashMap::new();
                for value in self.facets[i].values.iter() {
                    for term in value.1 .0.iter() {
                        *hash_map.entry(term.clone()).or_insert(0) += value.1 .1;
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
                    facets.push(Facet {
                        field: facet.field.clone(),
                        values: v,
                    });
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
                    facets.push(Facet {
                        field: facet.field.clone(),
                        values: v,
                    });
                }
            }
        }

        Some(facets)
    }

    pub fn string_set_to_single_term_id(&mut self) {
        for (i, facet) in self.facets.iter().enumerate() {
            if facet.field_type == FieldType::StringSet {
                for (idx, value) in facet.values.iter().enumerate() {
                    for term in value.1 .0.iter() {
                        self.string_set_to_single_term_id_vec[i]
                            .entry(term.to_string())
                            .or_insert(AHashSet::from_iter(vec![idx as u16]))
                            .insert(idx as u16);
                    }
                }
            }
        }
    }

    /// Reset index to empty, while maintaining schema
    pub fn clear_index(&mut self) {
        let _ = self.index_file.rewind();
        let _ = self.index_file.set_len(0);
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

        self.index_file_mmap =
            unsafe { Mmap::map(&self.index_file).expect("Unable to create Mmap") };

        let _ = self.docstore_file.rewind();
        let _ = self.docstore_file.set_len(0);

        let _ = self.delete_file.rewind();
        let _ = self.delete_file.set_len(0);
        self.delete_hashset.clear();

        let _ = self.facets_file.rewind();
        let _ = self
            .facets_file
            .set_len((self.facets_size_sum * ROARING_BLOCK_SIZE) as u64);
        self.facets_file_mmap =
            unsafe { MmapMut::map_mut(&self.facets_file).expect("Unable to create Mmap") };
        let index_path = Path::new(&self.index_path_string);
        let _ = fs::remove_file(index_path.join(FACET_VALUES_FILENAME));
        self.facets.clear();

        if !self.stored_field_names.is_empty() && self.meta.access_type == AccessType::Mmap {
            self.docstore_file_mmap =
                unsafe { Mmap::map(&self.docstore_file).expect("Unable to create Mmap") };
        }

        self.document_length_normalized_average = 0.0;
        self.indexed_doc_count = 0;
        self.positions_sum_normalized = 0;
        self.segment_number1 = 0;

        self.level_index = Vec::new();
        self.segments_index = Vec::new();
        self.segments_level0 = Vec::new();

        self.key_count_sum = 0;
        self.block_id = 0;
        self.strip_compressed_sum = 0;
        self.postings_buffer_pointer = 0;
        self.docid_count = 0;
        self.size_compressed_docid_index = 0;
        self.size_compressed_positions_index = 0;

        self.position_count = 0;
        self.postinglist_count = 0;
    }

    /// Delete index from disc and ram
    pub fn delete_index(&mut self) {
        let index_path = Path::new(&self.index_path_string);
        let _ = fs::remove_file(index_path.join(INDEX_FILENAME));
        let _ = fs::remove_file(index_path.join(SCHEMA_FILENAME));
        let _ = fs::remove_file(index_path.join(META_FILENAME));
        let _ = fs::remove_file(index_path.join(DELETE_FILENAME));
        let _ = fs::remove_file(index_path.join(FACET_FILENAME));
        let _ = fs::remove_file(index_path.join(FACET_VALUES_FILENAME));
        let _ = fs::remove_dir(index_path);
    }

    /// Remove index from RAM (Reverse of open_index)
    pub fn close_index(&mut self) {
        self.commit(self.indexed_doc_count);
    }
}

/// Delete document from index by document id
#[allow(async_fn_in_trait)]
pub trait DeleteDocument {
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
        let mut index_mut = self.write().await;
        if docid as usize >= index_mut.indexed_doc_count {
            return;
        }
        if index_mut.delete_hashset.insert(docid as usize) {
            let mut buffer: [u8; 8] = [0; 8];
            write_u64(docid, &mut buffer, 0);
            let _ = index_mut.delete_file.write(&buffer);
            let _ = index_mut.delete_file.flush();
        }
    }
}

/// Delete documents from index by document id
#[allow(async_fn_in_trait)]
pub trait DeleteDocuments {
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
        let mut index_mut = self.write().await;
        let mut buffer: [u8; 8] = [0; 8];
        for docid in docid_vec {
            if docid as usize >= index_mut.indexed_doc_count {
                continue;
            }
            if index_mut.delete_hashset.insert(docid as usize) {
                write_u64(docid, &mut buffer, 0);
                let _ = index_mut.delete_file.write(&buffer);
            }
        }
        let _ = index_mut.delete_file.flush();
    }
}

/// Delete documents from index by query
/// Delete and search have identical parameters.
/// It is recommended to test with search prior to delete to verify that only those documents are returned that you really want to delete.
#[allow(clippy::too_many_arguments)]
#[allow(async_fn_in_trait)]
pub trait DeleteDocumentsByQuery {
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
    async fn update_document(&self, id_document: (u64, Document));
}

/// Update document in index
/// Update_document is a combination of delete_document and index_document.
/// All current limitations of delete_document apply.
impl UpdateDocument for IndexArc {
    async fn update_document(&self, id_document: (u64, Document)) {
        self.delete_document(id_document.0).await;
        self.index_document(id_document.1).await;
    }
}

/// Update documents in index
/// Update_document is a combination of delete_document and index_document.
/// All current limitations of delete_document apply.
#[allow(async_fn_in_trait)]
pub trait UpdateDocuments {
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
    async fn index_documents(&self, document_vec: Vec<Document>);
}

impl IndexDocuments for IndexArc {
    /// Index document
    async fn index_documents(&self, document_vec: Vec<Document>) {
        for document in document_vec {
            self.index_document(document).await;
        }
    }
}

/// Indexes as single document
#[allow(async_fn_in_trait)]
pub trait IndexDocument {
    async fn index_document(&self, document: Document);
}

impl IndexDocument for IndexArc {
    /// Index document
    /// May block, if the threshold of documents indexed in parallel is exceeded.
    async fn index_document(&self, document: Document) {
        let index_arc_clone = self.clone();
        let index_ref = self.read().await;
        let schema = index_ref.indexed_schema_vec.clone();
        let enable_bigram = index_ref.enable_bigram;
        let indexed_field_vec_len = index_ref.indexed_field_vec.len();
        let tokenizer_type = index_ref.meta.tokenizer;
        let segment_number_mask1 = index_ref.segment_number_mask1;
        let index_permits = index_ref.permits.clone();
        drop(index_ref);

        let permit_thread = index_permits.clone().acquire_owned().await.unwrap();

        tokio::spawn(async move {
            let token_per_field_max: u32 = u16::MAX as u32;
            let mut unique_terms: AHashMap<String, TermObject> = AHashMap::new();
            let mut field_vec: Vec<(usize, u8, u32, u32)> = Vec::new();

            for schema_field in schema.iter() {
                let field_name = &schema_field.field;

                if let Some(field_value) = document.get(field_name) {
                    let mut non_unique_terms: Vec<NonUniqueTermObject> = Vec::new();
                    let mut nonunique_terms_count = 0u32;

                    let text = match schema_field.field_type {
                        FieldType::Text | FieldType::String => {
                            serde_json::from_str(&field_value.to_string())
                                .unwrap_or(field_value.to_string())
                                .to_string()
                        }
                        _ => field_value.to_string(),
                    };

                    let mut query_type_mut = QueryType::Union;

                    tokenizer(
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
                        enable_bigram,
                        schema_field.indexed_field_id,
                        indexed_field_vec_len,
                    );

                    let document_length_compressed: u8 = norm_frequency(nonunique_terms_count);
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

            let bigrams: Vec<String> = unique_terms
                .iter()
                .filter(|term| term.1.is_bigram)
                .map(|term| term.1.term.clone())
                .collect();

            for term in bigrams.iter() {
                let bigram = unique_terms.get(term).unwrap();
                let term_bigram1 = bigram.term_bigram1.clone();
                let term_bigram2 = bigram.term_bigram2.clone();

                for indexed_field_id in 0..indexed_field_vec_len {
                    let positions_count_bigram1 =
                        unique_terms[&term_bigram1].field_positions_vec[indexed_field_id].len();
                    let positions_count_bigram2 =
                        unique_terms[&term_bigram2].field_positions_vec[indexed_field_id].len();
                    let bigram = unique_terms.get_mut(term).unwrap();

                    if positions_count_bigram1 > 0 {
                        bigram
                            .field_vec_bigram1
                            .push((indexed_field_id, positions_count_bigram1 as u32));
                    }
                    if positions_count_bigram2 > 0 {
                        bigram
                            .field_vec_bigram2
                            .push((indexed_field_id, positions_count_bigram2 as u32));
                    }
                }
            }

            let document_item = DocumentItem {
                document,
                unique_terms,
                field_vec,
            };

            index_arc_clone.index_document_2(document_item).await;

            drop(permit_thread);
        });
    }
}

#[allow(async_fn_in_trait)]
pub(crate) trait IndexDocument2 {
    async fn index_document_2(&self, document_item: DocumentItem);
}

impl IndexDocument2 for IndexArc {
    async fn index_document_2(&self, document_item: DocumentItem) {
        let mut index_mut = self.write().await;

        let doc_id: usize = index_mut.indexed_doc_count;
        index_mut.indexed_doc_count += 1;

        let do_commit = index_mut.block_id != doc_id >> 16;
        if do_commit {
            index_mut.commit(doc_id);

            index_mut.block_id = doc_id >> 16;
        }

        if !index_mut.facets.is_empty() {
            let facets_size_sum = index_mut.facets_size_sum;
            for i in 0..index_mut.facets.len() {
                let facet = &mut index_mut.facets[i];
                if let Some(field_value) = document_item.document.get(&facet.name) {
                    let address = (facets_size_sum * doc_id) + facet.offset;

                    match facet.field_type {
                        FieldType::U8 => {
                            index_mut.facets_file_mmap[address] =
                                field_value.as_u64().unwrap() as u8
                        }
                        FieldType::U16 => write_u16(
                            field_value.as_u64().unwrap_or_default() as u16,
                            &mut index_mut.facets_file_mmap,
                            address,
                        ),
                        FieldType::U32 => write_u32(
                            field_value.as_u64().unwrap_or_default() as u32,
                            &mut index_mut.facets_file_mmap,
                            address,
                        ),
                        FieldType::U64 => write_u64(
                            field_value.as_u64().unwrap_or_default(),
                            &mut index_mut.facets_file_mmap,
                            address,
                        ),
                        FieldType::I8 => write_i8(
                            field_value.as_i64().unwrap() as i8,
                            &mut index_mut.facets_file_mmap,
                            address,
                        ),
                        FieldType::I16 => write_i16(
                            field_value.as_i64().unwrap_or_default() as i16,
                            &mut index_mut.facets_file_mmap,
                            address,
                        ),
                        FieldType::I32 => write_i32(
                            field_value.as_i64().unwrap_or_default() as i32,
                            &mut index_mut.facets_file_mmap,
                            address,
                        ),
                        FieldType::I64 => write_i64(
                            field_value.as_i64().unwrap_or_default(),
                            &mut index_mut.facets_file_mmap,
                            address,
                        ),
                        FieldType::F32 => write_f32(
                            field_value.as_f64().unwrap_or_default() as f32,
                            &mut index_mut.facets_file_mmap,
                            address,
                        ),
                        FieldType::F64 => write_f64(
                            field_value.as_f64().unwrap_or_default(),
                            &mut index_mut.facets_file_mmap,
                            address,
                        ),
                        FieldType::String => {
                            if facet.values.len() < u16::MAX as usize {
                                let key = serde_json::from_str(&field_value.to_string())
                                    .unwrap_or(field_value.to_string())
                                    .to_string();

                                let key_string = key.clone();
                                let key = vec![key];

                                facet.values.entry(key_string.clone()).or_insert((key, 0)).1 += 1;

                                let facet_value_id =
                                    facet.values.get_index_of(&key_string).unwrap() as u16;
                                write_u16(facet_value_id, &mut index_mut.facets_file_mmap, address)
                            }
                        }

                        FieldType::StringSet => {
                            if facet.values.len() < u16::MAX as usize {
                                let mut key: Vec<String> =
                                    serde_json::from_value(field_value.clone()).unwrap();
                                key.sort();

                                let key_string = key.join("_");
                                facet.values.entry(key_string.clone()).or_insert((key, 0)).1 += 1;

                                let facet_value_id =
                                    facet.values.get_index_of(&key_string).unwrap() as u16;
                                write_u16(facet_value_id, &mut index_mut.facets_file_mmap, address)
                            }
                        }
                        FieldType::Point => {
                            if let Ok(point) = serde_json::from_value::<Point>(field_value.clone())
                            {
                                if point.len() == 2 {
                                    if point[0] >= -90.0
                                        && point[0] <= 90.0
                                        && point[1] >= -180.0
                                        && point[1] <= 180.0
                                    {
                                        let morton_code = encode_morton_2_d(&point);
                                        write_u64(
                                            morton_code,
                                            &mut index_mut.facets_file_mmap,
                                            address,
                                        )
                                    } else {
                                        println!(
                                            "outside valid coordinate range: {} {}",
                                            point[0], point[1]
                                        );
                                    }
                                }
                            }
                        }

                        _ => {}
                    };
                }
            }
        }

        if !index_mut.uncommitted {
            for strip0 in index_mut.segments_level0.iter_mut() {
                strip0.positions_compressed = vec![0; MAX_POSITIONS_PER_TERM * 2];
            }
            index_mut.uncommitted = true;
        }

        let mut longest_field_id: usize = 0;
        let mut longest_field_length: u32 = 0;
        for value in document_item.field_vec {
            if doc_id == 0 && value.3 > longest_field_length {
                longest_field_id = value.0;
                longest_field_length = value.3;
            }

            index_mut.document_length_compressed_array[value.0][doc_id & 0b11111111_11111111] =
                value.1;
            index_mut.positions_sum_normalized += value.2 as u64;
            index_mut.indexed_field_vec[value.0].field_length_sum += value.2 as usize;
        }

        if doc_id == 0 {
            index_mut.longest_field_id = longest_field_id;
            index_mut.indexed_field_vec[longest_field_id].is_longest_field = true;
            if index_mut.indexed_field_vec.len() > 1 {
                println!(
                    "detect longest field id {} name {} length {}",
                    longest_field_id,
                    index_mut.indexed_field_vec[longest_field_id].schema_field_name,
                    longest_field_length
                );
            }
        }

        for term in document_item.unique_terms {
            index_mut.index_posting(term.1, doc_id, false);
        }

        if !index_mut.stored_field_names.is_empty() {
            index_mut.store_document(doc_id, document_item.document);
        }

        if do_commit {
            drop(index_mut);
            warmup(self).await;
        }
    }
}

pub(crate) struct DocumentItem {
    pub document: Document,
    pub unique_terms: AHashMap<String, TermObject>,
    pub field_vec: Vec<(usize, u8, u32, u32)>,
}
