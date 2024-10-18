use crate::commit::KEY_HEAD_SIZE;
use crate::geo_search::{decode_morton_2_d, point_distance_to_morton_range};
use crate::index::{DistanceUnit, Facet, FieldType, ResultFacet};
use crate::min_heap::Result;
use crate::tokenizer::tokenizer;
use crate::union::{union_docid_2, union_docid_3};
use crate::utils::{
    read_f32, read_f64, read_i16, read_i32, read_i64, read_i8, read_u16, read_u32, read_u64,
    read_u8,
};
use crate::{
    index::{
        get_max_score, AccessType, BlockObjectIndex, Index, IndexArc,
        NonUniquePostingListObjectQuery, NonUniqueTermObject, PostingListObjectIndex,
        PostingListObjectQuery, QueueObject, SegmentIndex, SimilarityType, TermObject, DUMMY_VEC,
        DUMMY_VEC_8, MAX_POSITIONS_PER_TERM, SPEEDUP_FLAG,
    },
    intersection::intersection_blockid,
    min_heap::MinHeap,
    single::single_blockid,
    union::union_blockid,
};

use ahash::{AHashMap, AHashSet};
use derivative::Derivative;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use std::mem;
use std::ops::Range;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

/// Specifies the default QueryType: The following query types are supported: **Union** (OR, disjunction), **Intersection** (AND, conjunction), **Phrase** (""), **Not** (-).
/// The default QueryType is superseded if the query parser detects that a different query type is specified within the query string (+ - "").
#[derive(Default, PartialEq, Clone, Debug, Serialize, Deserialize)]
pub enum QueryType {
    #[default]
    Union = 0,
    Intersection = 1,
    Phrase = 2,
    Not = 3,
}

/// The following result types are supported:
/// **Count** (count all results that match the query, but returning top-k results is not required)
/// **Topk** (returns the top-k results per query, but counting all results that match the query is not required)
/// **TopkCount** (returns the top-k results per query + count all results that match the query)
#[derive(Default, PartialEq, Clone, Debug, Serialize, Deserialize)]
pub enum ResultType {
    Count = 0,
    Topk = 1,
    #[default]
    TopkCount = 2,
}

pub(crate) struct SearchResult<'a> {
    pub topk_candidates: MinHeap<'a>,
    pub query_facets: Vec<ResultFacet>,
    pub skip_facet_count: bool,
}

/// Contains the results returned when searching the index.
#[derive(Default, Debug, Deserialize, Serialize, Derivative, Clone)]
pub struct ResultObject {
    /// Search query string
    pub query: String,
    /// Vector of search query terms. Can be used e.g. for custom highlighting.
    pub query_terms: Vec<String>,
    /// Number of returned search results. Identical to results.len()
    pub result_count: usize,

    /// Total number of search results that match the query
    /// result_count_total is only accurate if result_type=TopkCount or ResultType=Count, but not for ResultType=Topk
    pub result_count_total: usize,

    /// List of search results: doc ID and BM25 score
    pub results: Vec<Result>,
    /// List of facet fields: field name and vector of unique values and their counts.
    /// Unique values and their counts are only accurate if result_type=TopkCount or ResultType=Count, but not for ResultType=Topk
    pub facets: Vec<Facet>,
}

/// Create query_list and non_unique_query_list
/// blockwise intersection : if the corresponding blocks with a 65k docid range for each term have at least a single docid,
/// then the intersect_docid within a single block is executed  (=segments?)
/// specifies how to count the frequency of numerical facet field values
#[derive(Debug, Clone, Deserialize, Serialize, Default, PartialEq)]
pub enum RangeType {
    /// within the specified range
    #[default]
    CountWithinRange,
    /// within the range and all ranges above
    CountAboveRange,
    /// within the range and all ranges below
    CountBelowRange,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default, PartialEq)]
pub enum QueryFacet {
    U8 {
        field: String,
        range_type: RangeType,
        ranges: Vec<(String, u8)>,
    },
    U16 {
        field: String,
        range_type: RangeType,
        ranges: Vec<(String, u16)>,
    },
    U32 {
        field: String,
        range_type: RangeType,
        ranges: Vec<(String, u32)>,
    },
    U64 {
        field: String,
        range_type: RangeType,
        ranges: Vec<(String, u64)>,
    },
    I8 {
        field: String,
        range_type: RangeType,
        ranges: Vec<(String, i8)>,
    },
    I16 {
        field: String,
        range_type: RangeType,
        ranges: Vec<(String, i16)>,
    },
    I32 {
        field: String,
        range_type: RangeType,
        ranges: Vec<(String, i32)>,
    },
    I64 {
        field: String,
        range_type: RangeType,
        ranges: Vec<(String, i64)>,
    },
    F32 {
        field: String,
        range_type: RangeType,
        ranges: Vec<(String, f32)>,
    },
    F64 {
        field: String,
        range_type: RangeType,
        ranges: Vec<(String, f64)>,
    },
    String {
        field: String,
        prefix: String,
        length: u16,
    },
    StringSet {
        field: String,
        prefix: String,
        length: u16,
    },
    Point {
        field: String,
        range_type: RangeType,
        ranges: Vec<(String, f64)>,
        base: Point,
        unit: DistanceUnit,
    },
    #[default]
    None,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default, PartialEq)]
pub enum Ranges {
    U8(RangeType, Vec<(String, u8)>),
    U16(RangeType, Vec<(String, u16)>),
    U32(RangeType, Vec<(String, u32)>),
    U64(RangeType, Vec<(String, u64)>),
    I8(RangeType, Vec<(String, i8)>),
    I16(RangeType, Vec<(String, i16)>),
    I32(RangeType, Vec<(String, i32)>),
    I64(RangeType, Vec<(String, i64)>),
    F32(RangeType, Vec<(String, f32)>),
    F64(RangeType, Vec<(String, f64)>),
    Point(RangeType, Vec<(String, f64)>, Point, DistanceUnit),
    #[default]
    None,
}

#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub enum FacetValue {
    Bool(bool),
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    F32(f32),
    F64(f64),
    String(String),
    StringSet(Vec<String>),
    Point(Point),
    None,
}

impl Index {
    /// get_facet_value: Returns value from facet field for a doc_id even if schema stored=false (field not stored in document JSON).  
    /// Facet fields are more compact than fields stored in document JSON.
    /// Strings are stored more compact as indices to a unique term dictionary. Numbers are stored binary, not as strings.
    /// Facet fields are faster because no document loading, decompression and JSON decoding is required.  
    /// Facet fields are always memory mapped, internally always stored with fixed byte length layout, regardless of string size.
    #[inline]
    pub fn get_facet_value(self: &Index, field: &str, doc_id: usize) -> FacetValue {
        if let Some(field_idx) = self.facets_map.get(field) {
            match &self.facets[*field_idx].field_type {
                FieldType::U8 => {
                    let facet_value = &self.facets_file_mmap
                        [(self.facets_size_sum * doc_id) + self.facets[*field_idx].offset];
                    FacetValue::U8(*facet_value)
                }
                FieldType::U16 => {
                    let facet_value = read_u16(
                        &self.facets_file_mmap,
                        (self.facets_size_sum * doc_id) + self.facets[*field_idx].offset,
                    );
                    FacetValue::U16(facet_value)
                }
                FieldType::U32 => {
                    let facet_value = read_u32(
                        &self.facets_file_mmap,
                        (self.facets_size_sum * doc_id) + self.facets[*field_idx].offset,
                    );
                    FacetValue::U32(facet_value)
                }
                FieldType::U64 => {
                    let facet_value = read_u64(
                        &self.facets_file_mmap,
                        (self.facets_size_sum * doc_id) + self.facets[*field_idx].offset,
                    );
                    FacetValue::U64(facet_value)
                }
                FieldType::I8 => {
                    let facet_value = read_i8(
                        &self.facets_file_mmap,
                        (self.facets_size_sum * doc_id) + self.facets[*field_idx].offset,
                    );
                    FacetValue::I8(facet_value)
                }
                FieldType::I16 => {
                    let facet_value = read_i16(
                        &self.facets_file_mmap,
                        (self.facets_size_sum * doc_id) + self.facets[*field_idx].offset,
                    );
                    FacetValue::I16(facet_value)
                }
                FieldType::I32 => {
                    let facet_value = read_i32(
                        &self.facets_file_mmap,
                        (self.facets_size_sum * doc_id) + self.facets[*field_idx].offset,
                    );
                    FacetValue::I32(facet_value)
                }
                FieldType::I64 => {
                    let facet_value = read_i64(
                        &self.facets_file_mmap,
                        (self.facets_size_sum * doc_id) + self.facets[*field_idx].offset,
                    );
                    FacetValue::I64(facet_value)
                }
                FieldType::F32 => {
                    let facet_value = read_f32(
                        &self.facets_file_mmap,
                        (self.facets_size_sum * doc_id) + self.facets[*field_idx].offset,
                    );
                    FacetValue::F32(facet_value)
                }
                FieldType::F64 => {
                    let facet_value = read_f64(
                        &self.facets_file_mmap,
                        (self.facets_size_sum * doc_id) + self.facets[*field_idx].offset,
                    );
                    FacetValue::F64(facet_value)
                }

                FieldType::String => {
                    let facet_id = read_u16(
                        &self.facets_file_mmap,
                        (self.facets_size_sum * doc_id) + self.facets[*field_idx].offset,
                    );

                    let facet_value = self.facets[*field_idx]
                        .values
                        .get_index((facet_id).into())
                        .unwrap();

                    FacetValue::String(facet_value.1 .0[0].clone())
                }

                FieldType::StringSet => {
                    let facet_id = read_u16(
                        &self.facets_file_mmap,
                        (self.facets_size_sum * doc_id) + self.facets[*field_idx].offset,
                    );

                    let facet_value = self.facets[*field_idx]
                        .values
                        .get_index((facet_id).into())
                        .unwrap();

                    FacetValue::StringSet(facet_value.1 .0.clone())
                }

                FieldType::Point => {
                    let code = read_u64(
                        &self.facets_file_mmap,
                        (self.facets_size_sum * doc_id) + self.facets[*field_idx].offset,
                    );

                    let x = decode_morton_2_d(code);

                    FacetValue::Point(x.clone())
                }

                _ => FacetValue::None,
            }
        } else {
            FacetValue::None
        }
    }
}

/// FacetFilter:
/// either numerical range facet filter (range start/end) or
/// string facet filter (vector of strings) at least one (boolean OR) must match.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub enum FacetFilter {
    U8 {
        field: String,
        filter: Range<u8>,
    },
    U16 {
        field: String,
        filter: Range<u16>,
    },
    U32 {
        field: String,
        filter: Range<u32>,
    },
    U64 {
        field: String,
        filter: Range<u64>,
    },
    I8 {
        field: String,
        filter: Range<i8>,
    },
    I16 {
        field: String,
        filter: Range<i16>,
    },
    I32 {
        field: String,
        filter: Range<i32>,
    },
    I64 {
        field: String,
        filter: Range<i64>,
    },
    F32 {
        field: String,
        filter: Range<f32>,
    },
    F64 {
        field: String,
        filter: Range<f64>,
    },
    String {
        field: String,
        filter: Vec<String>,
    },
    StringSet {
        field: String,
        filter: Vec<String>,
    },
    Point {
        field: String,
        filter: (Point, Range<f64>, DistanceUnit),
    },
}

#[derive(Debug, Clone, Deserialize, Serialize, Default, PartialEq)]
pub(crate) enum FilterSparse {
    U8(Range<u8>),
    U16(Range<u16>),
    U32(Range<u32>),
    U64(Range<u64>),
    I8(Range<i8>),
    I16(Range<i16>),
    I32(Range<i32>),
    I64(Range<i64>),
    F32(Range<f32>),
    F64(Range<f64>),
    String(Vec<u16>),
    Point(Point, Range<f64>, DistanceUnit, Range<u64>),
    #[default]
    None,
}

#[derive(Derivative, Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
pub enum SortOrder {
    Ascending = 0,
    Descending = 1,
}

#[derive(Clone, Deserialize, Serialize)]
pub struct ResultSort {
    pub field: String,
    pub order: SortOrder,
    pub base: FacetValue,
}

#[derive(Clone, Serialize)]
pub struct ResultSortIndex<'a> {
    pub idx: usize,
    pub order: SortOrder,
    pub base: &'a FacetValue,
}

/// latitude lat
/// longitude lon
pub type Point = Vec<f64>;

/// Search the index for all indexed documents, both for committed and uncommitted documents.
/// The latter enables true realtime search: documents are available for search in exact the same millisecond they are indexed.
/// Arguments:
/// * `query_string`: query string + - "" search operators are recognized.
/// * `query_type_default`: Specifiy default QueryType: **Union** (OR, disjunction), **Intersection** (AND, conjunction), **Phrase** (""), **Not** (-).
///    The default QueryType is superseded if the query parser detects that a different query type is specified within the query string (+ - "").
/// * `offset`: offset of search results to return.
/// * `length`: number of search results to return.
/// * `result_type`: type of search results to return: Count, Topk, TopkCount.
/// * `include_uncommited`: true realtime search: include indexed documents which where not yet committed into search results.
/// * `field_filter`: Specify field names where to search at querytime, whereas SchemaField.indexed is set at indextime. If set to Vec::new() then all indexed fields are searched.
/// * `query_facets`: Must be set if facets should be returned in ResultObject. If set to Vec::new() then no facet fields are returned.
///    Facet fields are only collected, counted and returned for ResultType::Count and ResultType::TopkCount, but not for ResultType::Topk.
///    The prefix property of a QueryFacet allows at query time to filter the returned facet values to those matching a given prefix, if there are too many distinct values per facet field.
///    The length property of a QueryFacet allows at query time limiting the number of returned distinct values per facet field, if there are too many distinct values.  The QueryFacet can be used to improve the usability in an UI.
///    If the length property of a QueryFacet is set to 0 then no facet values for that facet are collected, counted and returned at query time. That decreases the query latency significantly.
///    The facet values are sorted by the frequency of the appearance of the value within the indexed documents matching the query in descending order.
///    Examples:
///    query_facets = vec![QueryFacet::String {field: "language".into(),prefix: "ger".into(),length: 5},QueryFacet::String {field: "brand".into(),prefix: "a".into(),length: 5}];
///    query_facets = vec![QueryFacet::U8 {field: "age".into(), range_type: RangeType::CountWithinRange, ranges: vec![("0-20".into(), 0),("20-40".into(), 20), ("40-60".into(), 40),("60-80".into(), 60), ("80-100".into(), 80)]}];
///    query_facets = vec![QueryFacet::Point {field: "location".into(),base:vec![38.8951, -77.0364],unit:DistanceUnit::Kilometers,range_type: RangeType::CountWithinRange,ranges: vec![ ("0-200".into(), 0.0),("200-400".into(), 200.0), ("400-600".into(), 400.0), ("600-800".into(), 600.0), ("800-1000".into(), 800.0)]}];
/// * `facet_filter`: Search results are filtered to documents matching specific string values or numerical ranges in the facet fields. If set to Vec::new() then result are not facet filtered.
///    The filter parameter filters the returned results to those documents both matching the query AND matching for all (boolean AND) stated facet filter fields at least one (boolean OR) of the stated values.
///    If the query is changed then both facet counts and search results are changed. If the facet filter is changed then only the search results are changed, while facet counts remain unchanged.
///    The facet counts depend only from the query and not which facet filters are selected.
///    Examples:
///    facet_filter=vec![FacetFilter::String{field:"language".into(),filter:vec!["german".into()]},FacetFilter::String{field:"brand".into(),filter:vec!["apple".into(),"google".into()]}];
///    facet_filter=vec![FacetFilter::U8{field:"age".into(),filter: 21..65}];
///    facet_filter = vec![FacetFilter::Point {field: "location".into(),filter: (vec![38.8951, -77.0364], 0.0..1000.0, DistanceUnit::Kilometers)}];
/// * `result_sort`: Sort field and order: Search results are sorted by the specified facet field, either in ascending or descending order.
///    If no sort field is specified, then the search results are sorted by rank in descending order per default.
///    Multiple sort fields are combined by a "sort by, then sort by"-method ("tie-breaking"-algorithm).
///    The results are sorted by the first field, and only for those results where the first field value is identical (tie) the results are sub-sorted by the second field,
///    until the n-th field value is either not equal or the last field is reached.
///    A special _score field (BM25x), reflecting how relevant the result is for a given search query (phrase match, match in title etc.) can be combined with any of the other sort fields as primary, secondary or n-th search criterium.
///    Sort is only enabled on facet fields that are defined in schema at create_index!
///    Examples:
///    result_sort = vec![ResultSort {field: "price".into(), order: SortOrder::Descending, base: FacetValue::None},ResultSort {field: "lamguage".into(), order: SortOrder::Ascending, base: FacetValue::None}];
///    result_sort = vec![ResultSort {field: "location".into(),order: SortOrder::Ascending, base: FacetValue::Point(vec![38.8951, -77.0364])}];
///  
///    If query_string is empty, then index facets (collected at index time) are returned, otherwise query facets (collected at query time) are returned.
///    Facets are defined in 3 different places:
///    the facet fields are defined in schema at create_index,
///    the facet field values are set in index_document at index time,
///    the query_facets/facet_filter search parameters are specified at query time.
///    Facets are then returned in the search result object.
#[allow(clippy::too_many_arguments)]
#[allow(async_fn_in_trait)]
pub trait Search {
    async fn search(
        &self,
        query_string: String,
        query_type_default: QueryType,
        offset: usize,
        length: usize,
        result_type: ResultType,
        include_uncommited: bool,
        field_filter: Vec<String>,
        query_facets: Vec<QueryFacet>,
        facet_filter: Vec<FacetFilter>,
        result_sort: Vec<ResultSort>,
    ) -> ResultObject;
}

/// Non-recursive binary search of non-consecutive u64 values in a slice of bytes
#[inline(never)]
pub(crate) fn binary_search(byte_array: &[u8], len: usize, key_hash: u64) -> i64 {
    let mut left = 0i64;
    let mut right = len as i64 - 1;
    while left <= right {
        let mid = (left + right) / 2;

        let pivot = read_u64(byte_array, mid as usize * KEY_HEAD_SIZE);
        match pivot.cmp(&key_hash) {
            std::cmp::Ordering::Equal => {
                return mid;
            }
            std::cmp::Ordering::Less => left = mid + 1,
            std::cmp::Ordering::Greater => right = mid - 1,
        }
    }

    -1
}

/// Decode posting_list_object and blocks on demand from mmap, instead keepping all posting_list_object and blocks for all keys in ram
#[inline(always)]
pub(crate) fn decode_posting_list_object(
    segment: &SegmentIndex,
    index: &Index,
    key_hash1: u64,
    calculate_score: bool,
) -> Option<PostingListObjectIndex> {
    let mut posting_count_list = 0u32;
    let mut max_list_score = 0.0;
    let mut first = true;
    let mut blocks_owned: Vec<BlockObjectIndex> = Vec::new();
    let mut bigram_term_index1 = 0;
    let mut bigram_term_index2 = 0;
    for (block_id, pointer) in segment.byte_array_blocks_pointer.iter().enumerate() {
        let key_count = pointer.2 as usize;

        let byte_array = &index.index_file_mmap[pointer.0 - (key_count * KEY_HEAD_SIZE)..pointer.0];
        let key_index = binary_search(byte_array, key_count, key_hash1);

        if key_index >= 0 {
            let key_address = key_index as usize * KEY_HEAD_SIZE;
            let posting_count = read_u16(byte_array, key_address + 8);

            let max_docid = read_u16(byte_array, key_address + 10);
            let max_p_docid = read_u16(byte_array, key_address + 12);
            let pointer_pivot_p_docid = read_u16(byte_array, key_address + 16);
            let compression_type_pointer = read_u32(byte_array, key_address + 18);

            posting_count_list += posting_count as u32 + 1;

            if first {
                bigram_term_index1 = read_u8(byte_array, key_address + 14);
                bigram_term_index2 = read_u8(byte_array, key_address + 15);
                first = false;
            }

            let block_object_index = BlockObjectIndex {
                max_block_score: 0.0,
                block_id: block_id as u32,
                posting_count,
                max_docid,
                max_p_docid,
                pointer_pivot_p_docid,
                compression_type_pointer,
            };
            blocks_owned.push(block_object_index);
        }
    }

    if !blocks_owned.is_empty() {
        if calculate_score {
            for block in blocks_owned.iter_mut() {
                block.max_block_score = get_max_score(
                    index,
                    segment,
                    bigram_term_index1,
                    bigram_term_index2,
                    posting_count_list,
                    block.block_id as usize,
                    block.max_docid as usize,
                    block.max_p_docid as usize,
                    block.pointer_pivot_p_docid as usize,
                    block.compression_type_pointer,
                );

                if block.max_block_score > max_list_score {
                    max_list_score = block.max_block_score
                }
            }
        }

        let posting_list_object_index = PostingListObjectIndex {
            posting_count: posting_count_list,
            bigram_term_index1,
            bigram_term_index2,
            max_list_score,
            blocks: blocks_owned,
            position_range_previous: 0,
        };

        Some(posting_list_object_index)
    } else {
        None
    }
}

impl Search for IndexArc {
    /// Search the index for all indexed documents, both for committed and uncommitted documents.
    /// The latter enables true realtime search: documents are available for search in exact the same millisecond they are indexed.
    /// Arguments:
    /// * `query_string`: query string + - "" search operators are recognized.
    /// * `query_type_default`: Specifiy default QueryType: **Union** (OR, disjunction), **Intersection** (AND, conjunction), **Phrase** (""), **Not** (-).
    ///    The default QueryType is superseded if the query parser detects that a different query type is specified within the query string (+ - "").
    /// * `offset`: offset of search results to return.
    /// * `length`: number of search results to return.
    /// * `result_type`: type of search results to return: Count, Topk, TopkCount.
    /// * `include_uncommited`: true realtime search: include indexed documents which where not yet committed into search results.
    /// * `field_filter`: Specify field names where to search at querytime, whereas SchemaField.indexed is set at indextime. If set to Vec::new() then all indexed fields are searched.
    /// * `query_facets`: Must be set if facets should be returned in ResultObject. If set to Vec::new() then no facet fields are returned.
    ///    Facet fields are only collected, counted and returned for ResultType::Count and ResultType::TopkCount, but not for ResultType::Topk.
    ///    The prefix property of a QueryFacet allows at query time to filter the returned facet values to those matching a given prefix, if there are too many distinct values per facet field.
    ///    The length property of a QueryFacet allows at query time limiting the number of returned distinct values per facet field, if there are too many distinct values.  The prefix and length properties can be used to improve the usability in an UI.
    ///    If the length property of a QueryFacet is set to 0 then no facet values for that facet are collected, counted and returned at query time. That decreases the query latency significantly.
    ///    The facet values are sorted by the frequency of the appearance of the value within the indexed documents matching the query in descending order.
    ///    Examples:
    ///    query_facets = vec![QueryFacet::String {field: "language".into(),prefix: "ger".into(),length: 5},QueryFacet::String {field: "brand".into(),prefix: "a".into(),length: 5}];
    ///    query_facets = vec![QueryFacet::U8 {field: "age".into(), range_type: RangeType::CountWithinRange, ranges: vec![("0-20".into(), 0),("20-40".into(), 20), ("40-60".into(), 40),("60-80".into(), 60), ("80-100".into(), 80)]}];
    ///    query_facets = vec![QueryFacet::Point {field: "location".into(),base:vec![38.8951, -77.0364],unit:DistanceUnit::Kilometers,range_type: RangeType::CountWithinRange,ranges: vec![ ("0-200".into(), 0.0),("200-400".into(), 200.0), ("400-600".into(), 400.0), ("600-800".into(), 600.0), ("800-1000".into(), 800.0)]}];
    /// * `facet_filter`: Search results are filtered to documents matching specific string values or numerical ranges in the facet fields. If set to Vec::new() then result are not facet filtered.
    ///    The filter parameter filters the returned results to those documents both matching the query AND matching for all (boolean AND) stated facet filter fields at least one (boolean OR) of the stated values.
    ///    If the query is changed then both facet counts and search results are changed. If the facet filter is changed then only the search results are changed, while facet counts remain unchanged.
    ///    The facet counts depend only from the query and not which facet filters are selected.
    ///    Examples:
    ///    facet_filter=vec![FacetFilter::String{field:"language".into(),filter:vec!["german".into()]},FacetFilter::String{field:"brand".into(),filter:vec!["apple".into(),"google".into()]}];
    ///    facet_filter=vec![FacetFilter::U8{field:"age".into(),filter: 21..65}];
    ///    facet_filter = vec![FacetFilter::Point {field: "location".into(),filter: (vec![38.8951, -77.0364], 0.0..1000.0, DistanceUnit::Kilometers)}];
    /// * `result_sort`: Sort field and order: Search results are sorted by the specified facet field, either in ascending or descending order.
    ///    If no sort field is specified, then the search results are sorted by rank in descending order per default.
    ///    Multiple sort fields are combined by a "sort by, then sort by"-method ("tie-breaking"-algorithm).
    ///    The results are sorted by the first field, and only for those results where the first field value is identical (tie) the results are sub-sorted by the second field,
    ///    until the n-th field value is either not equal or the last field is reached.
    ///    A special _score field (BM25x), reflecting how relevant the result is for a given search query (phrase match, match in title etc.) can be combined with any of the other sort fields as primary, secondary or n-th search criterium.
    ///    Sort is only enabled on facet fields that are defined in schema at create_index!   
    ///    Examples:
    ///    result_sort = vec![ResultSort {field: "price".into(), order: SortOrder::Descending, base: FacetValue::None},ResultSort {field: "lamguage".into(), order: SortOrder::Ascending, base: FacetValue::None}];
    ///    result_sort = vec![ResultSort {field: "location".into(),order: SortOrder::Ascending, base: FacetValue::Point(vec![38.8951, -77.0364])}];
    ///    If query_string is empty, then index facets (collected at index time) are returned, otherwise query facets (collected at query time) are returned.
    ///    Facets are defined in 3 different places:
    ///    the facet fields are defined in schema at create_index,
    ///    the facet field values are set in index_document at index time,
    ///    the query_facets/facet_filter search parameters are specified at query time.
    ///    Facets are then returned in the search result object.
    async fn search(
        &self,
        query_string: String,
        query_type_default: QueryType,
        offset: usize,
        length: usize,
        result_type: ResultType,
        include_uncommited: bool,
        field_filter: Vec<String>,
        query_facets: Vec<QueryFacet>,
        facet_filter: Vec<FacetFilter>,
        result_sort: Vec<ResultSort>,
    ) -> ResultObject {
        let index_ref = self.read().await;
        let mut query_type_mut = query_type_default;

        let mut result_object: ResultObject = Default::default();

        if index_ref.segments_index.is_empty() {
            return result_object;
        }

        let mut field_filter_set: AHashSet<u16> = AHashSet::new();
        for item in field_filter.iter() {
            match index_ref.schema_map.get(item) {
                Some(value) => {
                    if value.indexed {
                        field_filter_set.insert(value.indexed_field_id as u16);
                    }
                }
                None => {
                    println!("field not found: {}", item)
                }
            }
        }

        let mut result_sort_index: Vec<ResultSortIndex> = Vec::new();
        if !result_sort.is_empty() && result_type != ResultType::Count {
            for rs in result_sort.iter() {
                if let Some(idx) = index_ref.facets_map.get(&rs.field) {
                    result_sort_index.push(ResultSortIndex {
                        idx: *idx,
                        order: rs.order.clone(),
                        base: &rs.base,
                    });
                }
            }
        }

        let mut search_result = SearchResult {
            topk_candidates: MinHeap::new(offset + length, &index_ref, &result_sort_index),
            query_facets: Vec::new(),
            skip_facet_count: false,
        };

        let mut facet_filter_sparse: Vec<FilterSparse> = Vec::new();
        if !facet_filter.is_empty() {
            facet_filter_sparse = vec![FilterSparse::None; index_ref.facets.len()];
            for facet_filter_item in facet_filter.iter() {
                match &facet_filter_item {
                    FacetFilter::U8 { field, filter } => {
                        if let Some(idx) = index_ref.facets_map.get(field) {
                            if index_ref.facets[*idx].field_type == FieldType::U8 {
                                facet_filter_sparse[*idx] = FilterSparse::U8(filter.clone())
                            }
                        }
                    }
                    FacetFilter::U16 { field, filter } => {
                        if let Some(idx) = index_ref.facets_map.get(field) {
                            if index_ref.facets[*idx].field_type == FieldType::U16 {
                                facet_filter_sparse[*idx] = FilterSparse::U16(filter.clone())
                            }
                        }
                    }
                    FacetFilter::U32 { field, filter } => {
                        if let Some(idx) = index_ref.facets_map.get(field) {
                            if index_ref.facets[*idx].field_type == FieldType::U32 {
                                facet_filter_sparse[*idx] = FilterSparse::U32(filter.clone())
                            }
                        }
                    }
                    FacetFilter::U64 { field, filter } => {
                        if let Some(idx) = index_ref.facets_map.get(field) {
                            if index_ref.facets[*idx].field_type == FieldType::U64 {
                                facet_filter_sparse[*idx] = FilterSparse::U64(filter.clone())
                            }
                        }
                    }
                    FacetFilter::I8 { field, filter } => {
                        if let Some(idx) = index_ref.facets_map.get(field) {
                            if index_ref.facets[*idx].field_type == FieldType::I8 {
                                facet_filter_sparse[*idx] = FilterSparse::I8(filter.clone())
                            }
                        }
                    }
                    FacetFilter::I16 { field, filter } => {
                        if let Some(idx) = index_ref.facets_map.get(field) {
                            if index_ref.facets[*idx].field_type == FieldType::I16 {
                                facet_filter_sparse[*idx] = FilterSparse::I16(filter.clone())
                            }
                        }
                    }
                    FacetFilter::I32 { field, filter } => {
                        if let Some(idx) = index_ref.facets_map.get(field) {
                            if index_ref.facets[*idx].field_type == FieldType::I32 {
                                facet_filter_sparse[*idx] = FilterSparse::I32(filter.clone())
                            }
                        }
                    }
                    FacetFilter::I64 { field, filter } => {
                        if let Some(idx) = index_ref.facets_map.get(field) {
                            if index_ref.facets[*idx].field_type == FieldType::I64 {
                                facet_filter_sparse[*idx] = FilterSparse::I64(filter.clone())
                            }
                        }
                    }
                    FacetFilter::F32 { field, filter } => {
                        if let Some(idx) = index_ref.facets_map.get(field) {
                            if index_ref.facets[*idx].field_type == FieldType::F32 {
                                facet_filter_sparse[*idx] = FilterSparse::F32(filter.clone())
                            }
                        }
                    }
                    FacetFilter::F64 { field, filter } => {
                        if let Some(idx) = index_ref.facets_map.get(field) {
                            if index_ref.facets[*idx].field_type == FieldType::F64 {
                                facet_filter_sparse[*idx] = FilterSparse::F64(filter.clone())
                            }
                        }
                    }
                    FacetFilter::String { field, filter } => {
                        if let Some(idx) = index_ref.facets_map.get(field) {
                            let facet = &index_ref.facets[*idx];
                            if index_ref.facets[*idx].field_type == FieldType::String {
                                let mut string_id_vec = Vec::new();
                                for value in filter.iter() {
                                    let key = [value.clone()];
                                    if let Some(facet_value_id) = facet.values.get_index_of(&key[0])
                                    {
                                        string_id_vec.push(facet_value_id as u16);
                                    }
                                }
                                facet_filter_sparse[*idx] = FilterSparse::String(string_id_vec);
                            }
                        }
                    }

                    FacetFilter::StringSet { field, filter } => {
                        if let Some(idx) = index_ref.facets_map.get(field) {
                            let facet = &index_ref.facets[*idx];
                            if index_ref.facets[*idx].field_type == FieldType::StringSet {
                                let mut string_id_vec = Vec::new();
                                for value in filter.iter() {
                                    let key = [value.clone()];
                                    if let Some(facet_value_id) =
                                        facet.values.get_index_of(&key.join("_"))
                                    {
                                        string_id_vec.push(facet_value_id as u16);
                                    }

                                    if let Some(facet_value_ids) = index_ref
                                        .string_set_to_single_term_id_vec[*idx]
                                        .get(&value.clone())
                                    {
                                        for code in facet_value_ids.iter() {
                                            string_id_vec.push(*code);
                                        }
                                    }
                                }
                                facet_filter_sparse[*idx] = FilterSparse::String(string_id_vec);
                            }
                        }
                    }

                    FacetFilter::Point { field, filter } => {
                        if let Some(idx) = index_ref.facets_map.get(field) {
                            if index_ref.facets[*idx].field_type == FieldType::Point {
                                facet_filter_sparse[*idx] = FilterSparse::Point(
                                    filter.0.clone(),
                                    filter.1.clone(),
                                    filter.2.clone(),
                                    point_distance_to_morton_range(
                                        &filter.0,
                                        filter.1.end,
                                        &filter.2,
                                    ),
                                );
                            }
                        }
                    }
                }
            }
        }

        let mut is_range_facet = false;
        if !query_facets.is_empty() {
            search_result.query_facets = vec![ResultFacet::default(); index_ref.facets.len()];
            for query_facet in query_facets.iter() {
                match &query_facet {
                    QueryFacet::U8 {
                        field,
                        range_type,
                        ranges,
                    } => {
                        if let Some(idx) = index_ref.facets_map.get(field) {
                            if index_ref.facets[*idx].field_type == FieldType::U8 {
                                is_range_facet = true;
                                search_result.query_facets[*idx] = ResultFacet {
                                    field: field.clone(),
                                    length: u16::MAX,
                                    ranges: Ranges::U8(range_type.clone(), ranges.clone()),
                                    ..Default::default()
                                };
                            }
                        }
                    }
                    QueryFacet::U16 {
                        field,
                        range_type,
                        ranges,
                    } => {
                        if let Some(idx) = index_ref.facets_map.get(field) {
                            if index_ref.facets[*idx].field_type == FieldType::U16 {
                                is_range_facet = true;
                                search_result.query_facets[*idx] = ResultFacet {
                                    field: field.clone(),
                                    length: u16::MAX,
                                    ranges: Ranges::U16(range_type.clone(), ranges.clone()),
                                    ..Default::default()
                                };
                            }
                        }
                    }
                    QueryFacet::U32 {
                        field,
                        range_type,
                        ranges,
                    } => {
                        if let Some(idx) = index_ref.facets_map.get(field) {
                            if index_ref.facets[*idx].field_type == FieldType::U32 {
                                is_range_facet = true;
                                search_result.query_facets[*idx] = ResultFacet {
                                    field: field.clone(),
                                    length: u16::MAX,
                                    ranges: Ranges::U32(range_type.clone(), ranges.clone()),
                                    ..Default::default()
                                };
                            }
                        }
                    }
                    QueryFacet::U64 {
                        field,
                        range_type,
                        ranges,
                    } => {
                        if let Some(idx) = index_ref.facets_map.get(field) {
                            if index_ref.facets[*idx].field_type == FieldType::U64 {
                                is_range_facet = true;
                                search_result.query_facets[*idx] = ResultFacet {
                                    field: field.clone(),
                                    length: u16::MAX,
                                    ranges: Ranges::U64(range_type.clone(), ranges.clone()),
                                    ..Default::default()
                                };
                            }
                        }
                    }
                    QueryFacet::I8 {
                        field,
                        range_type,
                        ranges,
                    } => {
                        if let Some(idx) = index_ref.facets_map.get(field) {
                            if index_ref.facets[*idx].field_type == FieldType::I8 {
                                is_range_facet = true;
                                search_result.query_facets[*idx] = ResultFacet {
                                    field: field.clone(),
                                    length: u16::MAX,
                                    ranges: Ranges::I8(range_type.clone(), ranges.clone()),
                                    ..Default::default()
                                };
                            }
                        }
                    }
                    QueryFacet::I16 {
                        field,
                        range_type,
                        ranges,
                    } => {
                        if let Some(idx) = index_ref.facets_map.get(field) {
                            if index_ref.facets[*idx].field_type == FieldType::I16 {
                                is_range_facet = true;
                                search_result.query_facets[*idx] = ResultFacet {
                                    field: field.clone(),
                                    length: u16::MAX,
                                    ranges: Ranges::I16(range_type.clone(), ranges.clone()),
                                    ..Default::default()
                                };
                            }
                        }
                    }
                    QueryFacet::I32 {
                        field,
                        range_type,
                        ranges,
                    } => {
                        if let Some(idx) = index_ref.facets_map.get(field) {
                            if index_ref.facets[*idx].field_type == FieldType::I32 {
                                is_range_facet = true;
                                search_result.query_facets[*idx] = ResultFacet {
                                    field: field.clone(),
                                    length: u16::MAX,
                                    ranges: Ranges::I32(range_type.clone(), ranges.clone()),
                                    ..Default::default()
                                };
                            }
                        }
                    }
                    QueryFacet::I64 {
                        field,
                        range_type,
                        ranges,
                    } => {
                        if let Some(idx) = index_ref.facets_map.get(field) {
                            if index_ref.facets[*idx].field_type == FieldType::I64 {
                                is_range_facet = true;
                                search_result.query_facets[*idx] = ResultFacet {
                                    field: field.clone(),
                                    length: u16::MAX,
                                    ranges: Ranges::I64(range_type.clone(), ranges.clone()),
                                    ..Default::default()
                                };
                            }
                        }
                    }
                    QueryFacet::F32 {
                        field,
                        range_type,
                        ranges,
                    } => {
                        if let Some(idx) = index_ref.facets_map.get(field) {
                            if index_ref.facets[*idx].field_type == FieldType::F32 {
                                is_range_facet = true;
                                search_result.query_facets[*idx] = ResultFacet {
                                    field: field.clone(),
                                    length: u16::MAX,
                                    ranges: Ranges::F32(range_type.clone(), ranges.clone()),
                                    ..Default::default()
                                };
                            }
                        }
                    }
                    QueryFacet::F64 {
                        field,
                        range_type,
                        ranges,
                    } => {
                        if let Some(idx) = index_ref.facets_map.get(field) {
                            if index_ref.facets[*idx].field_type == FieldType::F64 {
                                is_range_facet = true;
                                search_result.query_facets[*idx] = ResultFacet {
                                    field: field.clone(),
                                    length: u16::MAX,
                                    ranges: Ranges::F64(range_type.clone(), ranges.clone()),
                                    ..Default::default()
                                };
                            }
                        }
                    }
                    QueryFacet::String {
                        field,
                        prefix,
                        length,
                    } => {
                        if let Some(idx) = index_ref.facets_map.get(field) {
                            if index_ref.facets[*idx].field_type == FieldType::String {
                                search_result.query_facets[*idx] = ResultFacet {
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
                        if let Some(idx) = index_ref.facets_map.get(field) {
                            if index_ref.facets[*idx].field_type == FieldType::StringSet {
                                search_result.query_facets[*idx] = ResultFacet {
                                    field: field.clone(),
                                    prefix: prefix.clone(),
                                    length: *length,
                                    ..Default::default()
                                }
                            }
                        }
                    }

                    QueryFacet::Point {
                        field,
                        range_type,
                        ranges,
                        base,
                        unit,
                    } => {
                        if let Some(idx) = index_ref.facets_map.get(field) {
                            if index_ref.facets[*idx].field_type == FieldType::Point {
                                is_range_facet = true;
                                search_result.query_facets[*idx] = ResultFacet {
                                    field: field.clone(),
                                    length: u16::MAX,
                                    ranges: Ranges::Point(
                                        range_type.clone(),
                                        ranges.clone(),
                                        base.clone(),
                                        unit.clone(),
                                    ),
                                    ..Default::default()
                                };
                            }
                        }
                    }

                    QueryFacet::None => {}
                };
            }
        }

        let result_count_arc = Arc::new(AtomicUsize::new(0));
        let result_count_uncommitted_arc = Arc::new(AtomicUsize::new(0));

        'fallback: loop {
            let mut unique_terms: AHashMap<String, TermObject> = AHashMap::new();
            let mut non_unique_terms: Vec<NonUniqueTermObject> = Vec::new();
            let mut nonunique_terms_count = 0u32;

            tokenizer(
                &query_string,
                &mut unique_terms,
                &mut non_unique_terms,
                index_ref.meta.tokenizer,
                index_ref.segment_number_mask1,
                &mut nonunique_terms_count,
                u16::MAX as u32,
                MAX_POSITIONS_PER_TERM,
                true,
                &mut query_type_mut,
                index_ref.enable_bigram,
                0,
                1,
            );

            if include_uncommited && index_ref.uncommitted {
                index_ref.search_uncommitted(
                    &unique_terms,
                    &non_unique_terms,
                    &mut query_type_mut,
                    &result_type,
                    &field_filter_set,
                    &facet_filter_sparse,
                    &mut search_result,
                    &result_count_uncommitted_arc,
                    offset + length,
                );
            }

            let mut query_list_map: AHashMap<u64, PostingListObjectQuery> = AHashMap::new();
            let mut query_list: Vec<PostingListObjectQuery>;

            let mut not_query_list_map: AHashMap<u64, PostingListObjectQuery> = AHashMap::new();
            let mut not_query_list: Vec<PostingListObjectQuery>;

            let mut non_unique_query_list: Vec<NonUniquePostingListObjectQuery> = Vec::new();
            let mut preceding_bigram_count = 0;

            let mut blocks_vec: Vec<Vec<BlockObjectIndex>> = Vec::new();

            let mut not_found_terms_hashset: AHashSet<u64> = AHashSet::new();

            for non_unique_term in non_unique_terms.iter() {
                let term = unique_terms.get(&non_unique_term.term).unwrap();
                let key0: u32 = term.key0;
                let key_hash: u64 = term.key_hash;
                let term_no_diacritics_umlaut_case = &non_unique_term.term;

                let mut idf = 0.0;
                let mut idf_bigram1 = 0.0;
                let mut idf_bigram2 = 0.0;

                let mut term_index_unique = 0;
                if non_unique_term.op == QueryType::Not {
                    let query_list_map_len = not_query_list_map.len();
                    let not_query_list_option = not_query_list_map.get(&key_hash);
                    if not_query_list_option.is_none()
                        && !not_found_terms_hashset.contains(&key_hash)
                    {
                        let posting_count;
                        let max_list_score;
                        let blocks;
                        let blocks_len;
                        let found_plo = if index_ref.meta.access_type == AccessType::Mmap {
                            let posting_list_object_index_option = decode_posting_list_object(
                                &index_ref.segments_index[key0 as usize],
                                &index_ref,
                                key_hash,
                                false,
                            );

                            if posting_list_object_index_option.is_none() {
                                posting_count = 0;
                                max_list_score = 0.0;
                                blocks = &DUMMY_VEC;
                                blocks_len = 0;
                                false
                            } else {
                                let plo = posting_list_object_index_option.unwrap();

                                posting_count = plo.posting_count;
                                max_list_score = plo.max_list_score;
                                blocks = &DUMMY_VEC;
                                blocks_len = plo.blocks.len();
                                blocks_vec.push(plo.blocks);
                                true
                            }
                        } else {
                            let posting_list_object_index_option = index_ref.segments_index
                                [key0 as usize]
                                .segment
                                .get(&key_hash);
                            if posting_list_object_index_option.is_none() {
                                posting_count = 0;
                                max_list_score = 0.0;
                                blocks = &DUMMY_VEC;
                                blocks_len = 0;
                                false
                            } else {
                                let plo = posting_list_object_index_option.unwrap();

                                posting_count = plo.posting_count;
                                max_list_score = plo.max_list_score;
                                blocks_len = plo.blocks.len();
                                blocks = &plo.blocks;
                                true
                            }
                        };

                        if found_plo {
                            let value_new = PostingListObjectQuery {
                                posting_count,
                                max_list_score,
                                blocks,
                                blocks_index: blocks_vec.len(),
                                p_block_max: blocks_len as i32,
                                term: term_no_diacritics_umlaut_case.clone(),
                                key0,
                                term_index_unique: query_list_map_len,
                                idf,
                                idf_bigram1,
                                idf_bigram2,
                                is_bigram: non_unique_term.is_bigram,
                                ..Default::default()
                            };
                            not_query_list_map.insert(key_hash, value_new);
                        } else {
                            not_found_terms_hashset.insert(key_hash);
                        }
                    }
                } else {
                    let query_list_map_len = query_list_map.len();
                    let mut found = true;
                    let query_list_option = query_list_map.get(&key_hash);
                    match query_list_option {
                        None => {
                            if !not_found_terms_hashset.contains(&key_hash) {
                                let posting_count;
                                let bigram_term_index1;
                                let bigram_term_index2;
                                let max_list_score;
                                let blocks;
                                let blocks_len;
                                let found_plo = if index_ref.meta.access_type == AccessType::Mmap {
                                    let posting_list_object_index_option =
                                        decode_posting_list_object(
                                            &index_ref.segments_index[key0 as usize],
                                            &index_ref,
                                            key_hash,
                                            true,
                                        );

                                    if posting_list_object_index_option.is_none() {
                                        posting_count = 0;
                                        bigram_term_index1 = 0;
                                        bigram_term_index2 = 0;
                                        max_list_score = 0.0;
                                        blocks = &DUMMY_VEC;
                                        blocks_len = 0;
                                        false
                                    } else {
                                        let plo = posting_list_object_index_option.unwrap();

                                        posting_count = plo.posting_count;
                                        bigram_term_index1 = plo.bigram_term_index1;
                                        bigram_term_index2 = plo.bigram_term_index2;
                                        max_list_score = plo.max_list_score;
                                        blocks = &DUMMY_VEC;
                                        blocks_len = plo.blocks.len();
                                        blocks_vec.push(plo.blocks);
                                        true
                                    }
                                } else {
                                    let posting_list_object_index_option = index_ref.segments_index
                                        [key0 as usize]
                                        .segment
                                        .get(&key_hash);
                                    if posting_list_object_index_option.is_none() {
                                        posting_count = 0;
                                        bigram_term_index1 = 0;
                                        bigram_term_index2 = 0;
                                        max_list_score = 0.0;
                                        blocks = &DUMMY_VEC;
                                        blocks_len = 0;
                                        false
                                    } else {
                                        let plo = posting_list_object_index_option.unwrap();

                                        posting_count = plo.posting_count;
                                        bigram_term_index1 = plo.bigram_term_index1;
                                        bigram_term_index2 = plo.bigram_term_index2;
                                        max_list_score = plo.max_list_score;
                                        blocks_len = plo.blocks.len();
                                        blocks = &plo.blocks;

                                        true
                                    }
                                };

                                if found_plo {
                                    if result_type != ResultType::Count {
                                        if !non_unique_term.is_bigram
                                            || index_ref.meta.similarity
                                                == SimilarityType::Bm25fProximity
                                        {
                                            idf = (((index_ref.indexed_doc_count as f32
                                                - posting_count as f32
                                                + 0.5)
                                                / (posting_count as f32 + 0.5))
                                                + 1.0)
                                                .ln();
                                        } else {
                                            let posting_count1 = index_ref.stopword_posting_counts
                                                [bigram_term_index1 as usize];
                                            let posting_count2 = index_ref.stopword_posting_counts
                                                [bigram_term_index2 as usize];

                                            idf_bigram1 = (((index_ref.indexed_doc_count as f32
                                                - posting_count1 as f32
                                                + 0.5)
                                                / (posting_count1 as f32 + 0.5))
                                                + 1.0)
                                                .ln();

                                            idf_bigram2 = (((index_ref.indexed_doc_count as f32
                                                - posting_count2 as f32
                                                + 0.5)
                                                / (posting_count2 as f32 + 0.5))
                                                + 1.0)
                                                .ln();
                                        }
                                    }

                                    let value_new = PostingListObjectQuery {
                                        posting_count,
                                        max_list_score,
                                        blocks,
                                        blocks_index: blocks_vec.len(),
                                        p_block_max: blocks_len as i32,
                                        term: term_no_diacritics_umlaut_case.clone(),
                                        key0,
                                        term_index_unique: query_list_map_len,
                                        idf,
                                        idf_bigram1,
                                        idf_bigram2,
                                        is_bigram: non_unique_term.is_bigram,
                                        ..Default::default()
                                    };
                                    term_index_unique = value_new.term_index_unique;
                                    query_list_map.insert(key_hash, value_new);
                                } else {
                                    if non_unique_term.op == QueryType::Intersection
                                        || non_unique_term.op == QueryType::Phrase
                                    {
                                        break 'fallback;
                                    }
                                    not_found_terms_hashset.insert(key_hash);
                                    found = false;
                                }
                            }
                        }
                        Some(value) => {
                            term_index_unique = value.term_index_unique;
                        }
                    }

                    if found && non_unique_term.op == QueryType::Phrase {
                        let nu_plo = NonUniquePostingListObjectQuery {
                            term_index_unique,
                            term_index_nonunique: non_unique_query_list.len()
                                + preceding_bigram_count,
                            pos: 0,
                            p_pos: 0,
                            positions_pointer: 0,
                            positions_count: 0,
                            byte_array: &DUMMY_VEC_8,
                            field_vec: SmallVec::new(),
                            p_field: 0,
                            key0,
                            is_embedded: false,
                            embedded_positions: [0; 4],
                        };

                        if non_unique_term.is_bigram {
                            preceding_bigram_count += 1
                        };

                        non_unique_query_list.push(nu_plo);
                    }
                }
            }

            not_query_list = not_query_list_map.into_values().collect();
            query_list = query_list_map.into_values().collect();

            if index_ref.meta.access_type == AccessType::Mmap {
                for plo in query_list.iter_mut() {
                    plo.blocks = &blocks_vec[plo.blocks_index - 1]
                }
                for plo in not_query_list.iter_mut() {
                    plo.blocks = &blocks_vec[plo.blocks_index - 1]
                }
            }

            let query_list_len = query_list.len();
            let non_unique_query_list_len = non_unique_query_list.len();

            for term in non_unique_terms.iter() {
                if term.is_bigram {
                    result_object
                        .query_terms
                        .push(term.term_bigram1.to_string());
                    result_object
                        .query_terms
                        .push(term.term_bigram2.to_string());
                }
                {
                    result_object.query_terms.push(term.term.to_string());
                }
            }

            let mut matching_blocks: i32 = 0;
            if query_list_len == 0 {
            } else if query_list_len == 1 {
                if !(index_ref.uncommitted && include_uncommited)
                    && offset + length <= 1000
                    && not_query_list.is_empty()
                    && field_filter_set.is_empty()
                    && index_ref.delete_hashset.is_empty()
                    && facet_filter_sparse.is_empty()
                    && !is_range_facet
                    && result_sort_index.is_empty()
                {
                    if let Some(stopword_result_object) =
                        index_ref.stopword_results.get(&non_unique_terms[0].term)
                    {
                        result_object.query = stopword_result_object.query.clone();
                        result_object
                            .query_terms
                            .clone_from(&stopword_result_object.query_terms);
                        result_object.result_count = stopword_result_object.result_count;
                        result_object.result_count_total =
                            stopword_result_object.result_count_total;

                        if result_type != ResultType::Count {
                            result_object
                                .results
                                .clone_from(&stopword_result_object.results);
                            if offset > 0 {
                                result_object.results.drain(..offset);
                            }
                            if length < 1000 {
                                result_object.results.truncate(length);
                            }
                        }

                        if !search_result.query_facets.is_empty() && result_type != ResultType::Topk
                        {
                            let mut facets: Vec<Facet> = Vec::new();
                            for (i, facet) in search_result.query_facets.iter().enumerate() {
                                if facet.length == 0
                                    || stopword_result_object.facets[i].values.is_empty()
                                {
                                    continue;
                                }

                                let v = stopword_result_object.facets[i]
                                    .values
                                    .iter()
                                    .sorted_unstable_by(|a, b| b.1.cmp(&a.1))
                                    .map(|(a, c)| (a.clone(), *c))
                                    .filter(|(a, _c)| {
                                        facet.prefix.is_empty() || a.starts_with(&facet.prefix)
                                    })
                                    .take(facet.length as usize)
                                    .collect::<Vec<_>>();

                                if !v.is_empty() {
                                    facets.push(Facet {
                                        field: facet.field.clone(),
                                        values: v,
                                    });
                                }
                            }
                            result_object.facets = facets;
                        };

                        return result_object;
                    }
                }

                single_blockid(
                    &index_ref,
                    &mut non_unique_query_list,
                    &mut query_list,
                    &mut not_query_list,
                    &result_count_arc,
                    &mut search_result,
                    offset + length,
                    &result_type,
                    &field_filter_set,
                    &facet_filter_sparse,
                    &mut matching_blocks,
                )
                .await;
            } else if query_type_mut == QueryType::Union {
                search_result.skip_facet_count = true;

                if result_type == ResultType::Count {
                    union_blockid(
                        &index_ref,
                        &mut non_unique_query_list,
                        &mut query_list,
                        &mut not_query_list,
                        &result_count_arc,
                        &mut search_result,
                        offset + length,
                        &result_type,
                        &field_filter_set,
                        &facet_filter_sparse,
                    )
                    .await;
                } else if SPEEDUP_FLAG
                    && query_list_len == 2
                    && search_result.query_facets.is_empty()
                    && facet_filter_sparse.is_empty()
                    && search_result.topk_candidates.result_sort.is_empty()
                {
                    union_docid_2(
                        &index_ref,
                        &mut non_unique_query_list,
                        &mut query_list,
                        &mut not_query_list,
                        &result_count_arc,
                        &mut search_result,
                        offset + length,
                        &result_type,
                        &field_filter_set,
                        &facet_filter_sparse,
                        &mut matching_blocks,
                    )
                    .await;
                } else if SPEEDUP_FLAG && search_result.topk_candidates.result_sort.is_empty() {
                    union_docid_3(
                        &index_ref,
                        &mut non_unique_query_list,
                        &mut Vec::from([QueueObject {
                            query_list: query_list.clone(),
                            query_index: 0,
                            max_score: f32::MAX,
                        }]),
                        &mut not_query_list,
                        &result_count_arc,
                        &mut search_result,
                        offset + length,
                        &result_type,
                        &field_filter_set,
                        &facet_filter_sparse,
                        &mut matching_blocks,
                    )
                    .await;
                } else {
                    union_blockid(
                        &index_ref,
                        &mut non_unique_query_list,
                        &mut query_list,
                        &mut not_query_list,
                        &result_count_arc,
                        &mut search_result,
                        offset + length,
                        &result_type,
                        &field_filter_set,
                        &facet_filter_sparse,
                    )
                    .await;
                }
            } else {
                intersection_blockid(
                    &index_ref,
                    &mut non_unique_query_list,
                    &mut query_list,
                    &mut not_query_list,
                    &result_count_arc,
                    &mut search_result,
                    offset + length,
                    &result_type,
                    &field_filter_set,
                    &facet_filter_sparse,
                    &mut matching_blocks,
                    query_type_mut == QueryType::Phrase && non_unique_query_list_len >= 2,
                )
                .await;
                if index_ref.enable_fallback
                    && (result_count_arc.load(Ordering::Relaxed) < offset + length)
                {
                    continue 'fallback;
                }
            }

            break;
        }

        result_object.result_count = search_result.topk_candidates.current_heap_size;

        if search_result.topk_candidates.current_heap_size > offset {
            result_object.results = mem::take(&mut search_result.topk_candidates._elements);

            if search_result.topk_candidates.current_heap_size < offset + length {
                result_object
                    .results
                    .truncate(search_result.topk_candidates.current_heap_size);
            }

            result_object
                .results
                .sort_by(|a, b| search_result.topk_candidates.result_ordering(*b, *a));

            if offset > 0 {
                result_object.results.drain(..offset);
            }
        }

        result_object.result_count_total = result_count_uncommitted_arc.load(Ordering::Relaxed)
            + result_count_arc.load(Ordering::Relaxed);

        if !search_result.query_facets.is_empty() {
            result_object.facets = if result_object.query_terms.is_empty() {
                index_ref
                    .get_index_string_facets(query_facets)
                    .unwrap_or_default()
            } else {
                let mut facets: Vec<Facet> = Vec::new();
                for (i, facet) in search_result.query_facets.iter_mut().enumerate() {
                    if facet.length == 0 || facet.values.is_empty() {
                        continue;
                    }

                    let v = if facet.ranges == Ranges::None {
                        if index_ref.facets[i].field_type == FieldType::StringSet {
                            let mut hash_map: AHashMap<String, usize> = AHashMap::new();
                            for value in facet.values.iter() {
                                let value2 = index_ref.facets[i]
                                    .values
                                    .get_index((*value.0).into())
                                    .unwrap();

                                for term in value2.1 .0.iter() {
                                    *hash_map.entry(term.clone()).or_insert(0) += value.1;
                                }
                            }

                            hash_map
                                .iter()
                                .sorted_unstable_by(|a, b| b.1.cmp(a.1))
                                .map(|(a, c)| (a.clone(), *c))
                                .filter(|(a, _c)| {
                                    facet.prefix.is_empty() || a.starts_with(&facet.prefix)
                                })
                                .take(facet.length as usize)
                                .collect::<Vec<_>>()
                        } else {
                            facet
                                .values
                                .iter()
                                .sorted_unstable_by(|a, b| b.1.cmp(a.1))
                                .map(|(a, c)| {
                                    (
                                        index_ref.facets[i]
                                            .values
                                            .get_index((*a).into())
                                            .unwrap()
                                            .0
                                            .clone(),
                                        *c,
                                    )
                                })
                                .filter(|(a, _c)| {
                                    facet.prefix.is_empty() || a.starts_with(&facet.prefix)
                                })
                                .take(facet.length as usize)
                                .collect::<Vec<_>>()
                        }
                    } else {
                        let range_type = match &facet.ranges {
                            Ranges::U8(range_type, _ranges) => range_type.clone(),
                            Ranges::U16(range_type, _ranges) => range_type.clone(),
                            Ranges::U32(range_type, _ranges) => range_type.clone(),
                            Ranges::U64(range_type, _ranges) => range_type.clone(),
                            Ranges::I8(range_type, _ranges) => range_type.clone(),
                            Ranges::I16(range_type, _ranges) => range_type.clone(),
                            Ranges::I32(range_type, _ranges) => range_type.clone(),
                            Ranges::I64(range_type, _ranges) => range_type.clone(),
                            Ranges::F32(range_type, _ranges) => range_type.clone(),
                            Ranges::F64(range_type, _ranges) => range_type.clone(),
                            Ranges::Point(range_type, _ranges, _base, _unit) => range_type.clone(),
                            _ => RangeType::CountWithinRange,
                        };

                        match range_type {
                            RangeType::CountAboveRange => {
                                let mut sum = 0usize;
                                for value in facet
                                    .values
                                    .iter_mut()
                                    .sorted_unstable_by(|a, b| b.0.cmp(a.0))
                                {
                                    sum += *value.1;
                                    *value.1 = sum;
                                }
                            }
                            RangeType::CountBelowRange => {
                                let mut sum = 0usize;
                                for value in facet
                                    .values
                                    .iter_mut()
                                    .sorted_unstable_by(|a, b| a.0.cmp(b.0))
                                {
                                    sum += *value.1;
                                    *value.1 = sum;
                                }
                            }
                            RangeType::CountWithinRange => {}
                        }

                        facet
                            .values
                            .iter()
                            .sorted_unstable_by(|a, b| a.0.cmp(b.0))
                            .map(|(a, c)| {
                                (
                                    match &facet.ranges {
                                        Ranges::U8(_range_type, ranges) => {
                                            ranges[*a as usize].0.clone()
                                        }
                                        Ranges::U16(_range_type, ranges) => {
                                            ranges[*a as usize].0.clone()
                                        }
                                        Ranges::U32(_range_type, ranges) => {
                                            ranges[*a as usize].0.clone()
                                        }
                                        Ranges::U64(_range_type, ranges) => {
                                            ranges[*a as usize].0.clone()
                                        }
                                        Ranges::I8(_range_type, ranges) => {
                                            ranges[*a as usize].0.clone()
                                        }
                                        Ranges::I16(_range_type, ranges) => {
                                            ranges[*a as usize].0.clone()
                                        }
                                        Ranges::I32(_range_type, ranges) => {
                                            ranges[*a as usize].0.clone()
                                        }
                                        Ranges::I64(_range_type, ranges) => {
                                            ranges[*a as usize].0.clone()
                                        }
                                        Ranges::F32(_range_type, ranges) => {
                                            ranges[*a as usize].0.clone()
                                        }
                                        Ranges::F64(_range_type, ranges) => {
                                            ranges[*a as usize].0.clone()
                                        }

                                        Ranges::Point(_range_type, ranges, _base, _unit) => {
                                            ranges[*a as usize].0.clone()
                                        }

                                        _ => "".into(),
                                    },
                                    *c,
                                )
                            })
                            .filter(|(a, _c)| {
                                facet.prefix.is_empty() || a.starts_with(&facet.prefix)
                            })
                            .take(facet.length as usize)
                            .collect::<Vec<_>>()
                    };

                    if !v.is_empty() {
                        facets.push(Facet {
                            field: facet.field.clone(),
                            values: v,
                        });
                    }
                }
                facets
            };
        }

        result_object
    }
}
