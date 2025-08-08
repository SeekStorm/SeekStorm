use crate::geo_search::{decode_morton_2_d, point_distance_to_morton_range};
use crate::index::{
    DOCUMENT_LENGTH_COMPRESSION, DistanceUnit, Facet, FieldType, NgramType, ResultFacet,
};
use crate::min_heap::Result;
use crate::tokenizer::tokenizer;
use crate::union::{union_docid_2, union_docid_3};
use crate::utils::{
    read_f32, read_f64, read_i8, read_i16, read_i32, read_i64, read_u8, read_u16, read_u32,
    read_u64,
};
use crate::{
    index::{
        AccessType, BlockObjectIndex, DUMMY_VEC, DUMMY_VEC_8, Index, IndexArc,
        MAX_POSITIONS_PER_TERM, NonUniquePostingListObjectQuery, NonUniqueTermObject,
        PostingListObjectIndex, PostingListObjectQuery, QueueObject, SPEEDUP_FLAG, SegmentIndex,
        SimilarityType, TermObject, get_max_score,
    },
    intersection::intersection_blockid,
    min_heap::MinHeap,
    single::single_blockid,
    union::union_blockid,
};
use num::FromPrimitive;

use ahash::{AHashMap, AHashSet};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use std::mem;
use std::ops::Range;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use utoipa::ToSchema;

/// Specifies the default QueryType: The following query types are supported:
/// - **Union** (OR, disjunction),
/// - **Intersection** (AND, conjunction),
/// - **Phrase** (""),
/// - **Not** (-).
///
/// The default QueryType is superseded if the query parser detects that a different query type is specified within the query string (+ - "").
#[derive(Default, PartialEq, Clone, Debug, Serialize, Deserialize, ToSchema)]
pub enum QueryType {
    /// Union (OR, disjunction)
    #[default]
    Union = 0,
    /// Intersection (AND, conjunction)
    Intersection = 1,
    /// Phrase ("")
    Phrase = 2,
    /// Not (-)
    Not = 3,
}

/// The following result types are supported:
/// - **Count** (count all results that match the query, but returning top-k results is not required)
/// - **Topk** (returns the top-k results per query, but counting all results that match the query is not required)
/// - **TopkCount** (returns the top-k results per query + count all results that match the query)
#[derive(Default, PartialEq, Clone, Debug, Serialize, Deserialize, ToSchema)]
pub enum ResultType {
    /// Count all results that match the query, without returning top-k results
    Count = 0,
    /// Return the top-k results per query, without counting all results that match the query
    Topk = 1,
    /// Return the top-k results per query and count all results that match the query
    #[default]
    TopkCount = 2,
}

pub(crate) struct SearchResult<'a> {
    pub topk_candidates: MinHeap<'a>,
    pub query_facets: Vec<ResultFacet>,
    pub skip_facet_count: bool,
}

/// Contains the results returned when searching the index.
#[derive(Default, Debug, Deserialize, Serialize, Clone)]
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
    pub facets: AHashMap<String, Facet>,
}

/// Create query_list and non_unique_query_list
/// blockwise intersection : if the corresponding blocks with a 65k docid range for each term have at least a single docid,
/// then the intersect_docid within a single block is executed  (=segments?)
/// specifies how to count the frequency of numerical facet field values
#[derive(Debug, Clone, Deserialize, Serialize, Default, PartialEq, ToSchema)]
pub enum RangeType {
    /// within the specified range
    #[default]
    CountWithinRange,
    /// within the range and all ranges above
    CountAboveRange,
    /// within the range and all ranges below
    CountBelowRange,
}

/// Defines the query facets:
/// - string facet field values
/// - range segments for numerical facet field values
#[derive(Debug, Clone, Deserialize, Serialize, Default, PartialEq, ToSchema)]
pub enum QueryFacet {
    /// Range segment definition for numerical facet field values of type u8
    U8 {
        /// field name
        field: String,
        /// range type (CountWithinRange,CountBelowRange,CountAboveRange)
        range_type: RangeType,
        /// range label, range start
        ranges: Vec<(String, u8)>,
    },
    /// Range segment definition for numerical facet field values of type u16
    U16 {
        /// field name
        field: String,
        /// range type (CountWithinRange,CountBelowRange,CountAboveRange)
        range_type: RangeType,
        /// range label, range start
        ranges: Vec<(String, u16)>,
    },
    /// Range segment definition for numerical facet field values of type u32
    U32 {
        /// field name
        field: String,
        /// range type (CountWithinRange,CountBelowRange,CountAboveRange)
        range_type: RangeType,
        /// range label, range start
        ranges: Vec<(String, u32)>,
    },
    /// Range segment definition for numerical facet field values of type u64
    U64 {
        /// field name
        field: String,
        /// range type (CountWithinRange,CountBelowRange,CountAboveRange)
        range_type: RangeType,
        /// range label, range start
        ranges: Vec<(String, u64)>,
    },
    /// Range segment definition for numerical facet field values of type i8
    I8 {
        /// field name
        field: String,
        /// range type (CountWithinRange,CountBelowRange,CountAboveRange)
        range_type: RangeType,
        /// range label, range start
        ranges: Vec<(String, i8)>,
    },
    /// Range segment definition for numerical facet field values of type i16
    I16 {
        /// field name
        field: String,
        /// range type (CountWithinRange,CountBelowRange,CountAboveRange)
        range_type: RangeType,
        /// range label, range start
        ranges: Vec<(String, i16)>,
    },
    /// Range segment definition for numerical facet field values of type i32
    I32 {
        /// field name
        field: String,
        /// range type (CountWithinRange,CountBelowRange,CountAboveRange)
        range_type: RangeType,
        /// range label, range start
        ranges: Vec<(String, i32)>,
    },
    /// Range segment definition for numerical facet field values of type i64
    I64 {
        /// field name
        field: String,
        /// range type (CountWithinRange,CountBelowRange,CountAboveRange)
        range_type: RangeType,
        /// range label, range start
        ranges: Vec<(String, i64)>,
    },
    /// Range segment definition for numerical facet field values of type Unix timestamp
    Timestamp {
        /// field name
        field: String,
        /// range type (CountWithinRange,CountBelowRange,CountAboveRange)
        range_type: RangeType,
        /// range label, range start
        ranges: Vec<(String, i64)>,
    },
    /// Range segment definition for numerical facet field values of type f32
    F32 {
        /// field name
        field: String,
        /// range type (CountWithinRange,CountBelowRange,CountAboveRange)
        range_type: RangeType,
        /// range label, range start
        ranges: Vec<(String, f32)>,
    },
    /// Range segment definition for numerical facet field values of type f64
    F64 {
        /// field name
        field: String,
        /// range type (CountWithinRange,CountBelowRange,CountAboveRange)
        range_type: RangeType,
        /// range label, range start
        ranges: Vec<(String, f64)>,
    },
    /// Facet field values of type string
    String {
        /// field name
        field: String,
        /// Prefix filter of facet values to return
        prefix: String,
        /// maximum number of facet values to return
        length: u16,
    },
    /// Facet field values of type string set
    StringSet {
        /// field name
        field: String,
        /// Prefix filter of facet values to return
        prefix: String,
        /// maximum number of facet values to return
        length: u16,
    },
    /// Range segment definition for numerical facet field values of type Point (distance between base of type Point and facet field of type Point)
    Point {
        /// field name
        field: String,
        /// range type (CountWithinRange,CountBelowRange,CountAboveRange)
        range_type: RangeType,
        /// range label, range start
        ranges: Vec<(String, f64)>,
        /// base point (latitude/lat, longitude/lon)
        base: Point,
        /// distance unit (kilometers/miles)
        unit: DistanceUnit,
    },
    /// No query facet
    #[default]
    None,
}

/// Defines the range segments for numerical facet field values
#[derive(Debug, Clone, Deserialize, Serialize, Default, PartialEq)]
pub enum Ranges {
    /// U8 range filter: range type (CountWithinRange,CountBelowRange,CountAboveRange), range label, range start
    U8(RangeType, Vec<(String, u8)>),
    /// U16 range filter: range type (CountWithinRange,CountBelowRange,CountAboveRange), range label, range start
    U16(RangeType, Vec<(String, u16)>),
    /// U32 range filter: range type (CountWithinRange,CountBelowRange,CountAboveRange), range label, range start
    U32(RangeType, Vec<(String, u32)>),
    /// U64 range filter: range type (CountWithinRange,CountBelowRange,CountAboveRange), range label, range start
    U64(RangeType, Vec<(String, u64)>),
    /// I8 range filter: range type (CountWithinRange,CountBelowRange,CountAboveRange), range label, range start
    I8(RangeType, Vec<(String, i8)>),
    /// I16 range filter: range type (CountWithinRange,CountBelowRange,CountAboveRange), range label, range start
    I16(RangeType, Vec<(String, i16)>),
    /// I32 range filter: range type (CountWithinRange,CountBelowRange,CountAboveRange), range label, range start
    I32(RangeType, Vec<(String, i32)>),
    /// I64 range filter: range type (CountWithinRange,CountBelowRange,CountAboveRange), range label, range start
    I64(RangeType, Vec<(String, i64)>),
    /// Unix timestamp (number of seconds since 1 January 1970) range filter: range type (CountWithinRange,CountBelowRange,CountAboveRange), range label, range start
    Timestamp(RangeType, Vec<(String, i64)>),
    /// F32 range filter: range type (CountWithinRange,CountBelowRange,CountAboveRange), range label, range start
    F32(RangeType, Vec<(String, f32)>),
    /// F64 range filter: range type (CountWithinRange,CountBelowRange,CountAboveRange), range label, range start
    F64(RangeType, Vec<(String, f64)>),
    /// Proximity range filter: range type (CountWithinRange,CountBelowRange,CountAboveRange), range label, base point (longitude/lon, latitude/lat), distance unit
    Point(RangeType, Vec<(String, f64)>, Point, DistanceUnit),
    #[default]
    /// No range filter
    None,
}

/// FacetValue: Facet field value types
#[derive(Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub enum FacetValue {
    /// Boolean value
    Bool(bool),
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
    /// 32-bit floating point number
    F32(f32),
    /// 64-bit floating point number
    F64(f64),
    /// String value
    String(String),
    /// String set value
    StringSet(Vec<String>),
    /// Point value: latitude/lat, longitude/lon
    Point(Point),
    /// No value
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
                FieldType::Timestamp => {
                    let facet_value = read_i64(
                        &self.facets_file_mmap,
                        (self.facets_size_sum * doc_id) + self.facets[*field_idx].offset,
                    );
                    FacetValue::Timestamp(facet_value)
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

                    FacetValue::String(facet_value.1.0[0].clone())
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

                    FacetValue::StringSet(facet_value.1.0.clone())
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

/// U8 range filter
#[allow(dead_code)]
#[derive(ToSchema)]
pub struct RangeU8 {
    /// range start
    pub start: u8,
    /// range end
    pub end: u8,
}

/// U16 range filter
#[allow(dead_code)]
#[derive(ToSchema)]
pub struct RangeU16 {
    /// range start
    pub start: u16,
    /// range end
    pub end: u16,
}

/// U32 range filter
#[allow(dead_code)]
#[derive(ToSchema)]
pub struct RangeU32 {
    /// range start
    pub start: u32,
    /// range end
    pub end: u32,
}

/// U64 range filter
#[allow(dead_code)]
#[derive(ToSchema)]
pub struct RangeU64 {
    /// range start
    pub start: u64,
    /// range end
    pub end: u64,
}

/// I8 range filter
#[allow(dead_code)]
#[derive(ToSchema)]
pub struct RangeI8 {
    /// range start
    pub start: i8,
    /// range end
    pub end: i8,
}

/// I16 range filter
#[allow(dead_code)]
#[derive(ToSchema)]
pub struct RangeI16 {
    /// range start
    pub start: i16,
    /// range end
    pub end: i16,
}

/// I32 range filter
#[allow(dead_code)]
#[derive(ToSchema)]
pub struct RangeI32 {
    /// range start
    pub start: i32,
    /// range end
    pub end: i32,
}

/// I64 range filter
#[allow(dead_code)]
#[derive(ToSchema)]
pub struct RangeI64 {
    /// range start
    pub start: i64,
    /// range end
    pub end: i64,
}

/// F32 range filter
#[allow(dead_code)]
#[derive(ToSchema)]
pub struct RangeF32 {
    /// range start
    pub start: f32,
    /// range end
    pub end: f32,
}

/// F64 range filter
#[allow(dead_code)]
#[derive(ToSchema)]
pub struct RangeF64 {
    /// range start
    pub start: f64,
    /// range end
    pub end: f64,
}

/// FacetFilter:
/// either numerical range facet filter (range start/end) or
/// string facet filter (vector of strings) at least one (boolean OR) must match.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, ToSchema)]
pub enum FacetFilter {
    /// U8 range filter
    U8 {
        /// field name
        field: String,
        /// filter: range start, range end
        #[schema(value_type=RangeU8)]
        filter: Range<u8>,
    },
    /// U16 range filter
    U16 {
        /// field name
        field: String,
        /// filter: range start, range end
        #[schema(value_type=RangeU16)]
        filter: Range<u16>,
    },
    /// U32 range filter
    U32 {
        /// field name
        field: String,
        /// filter: range start, range end
        #[schema(value_type=RangeU32)]
        filter: Range<u32>,
    },
    /// U64 range filter
    U64 {
        /// field name
        field: String,
        /// filter: range start, range end
        #[schema(value_type=RangeU64)]
        filter: Range<u64>,
    },
    /// I8 range filter
    I8 {
        /// field name
        field: String,
        /// filter: range start, range end
        #[schema(value_type=RangeI8)]
        filter: Range<i8>,
    },
    /// I16 range filter
    I16 {
        /// field name
        field: String,
        /// filter: range start, range end
        #[schema(value_type=RangeI16)]
        filter: Range<i16>,
    },
    /// I32 range filter
    I32 {
        /// field name
        field: String,
        /// filter: range start, range end
        #[schema(value_type=RangeI32)]
        filter: Range<i32>,
    },
    /// I64 range filter
    I64 {
        /// field name
        field: String,
        /// filter: range start, range end
        #[schema(value_type=RangeI64)]
        filter: Range<i64>,
    },
    /// Timestamp range filter, Unix timestamp: the number of seconds since 1 January 1970
    Timestamp {
        /// field name
        field: String,
        /// filter: range start, range end
        #[schema(value_type=RangeI64)]
        filter: Range<i64>,
    },
    /// F32 range filter
    F32 {
        /// field name
        field: String,
        /// filter: range start, range end
        #[schema(value_type=RangeF32)]
        filter: Range<f32>,
    },
    /// F64 range filter
    F64 {
        /// field name
        field: String,
        /// filter: range start, range end
        #[schema(value_type=RangeF64)]
        filter: Range<f64>,
    },
    /// String filter
    String {
        /// field name
        field: String,
        /// filter: array of facet string values
        filter: Vec<String>,
    },
    /// StringSet filter
    StringSet {
        /// field name
        field: String,
        /// filter: array of facet string values
        filter: Vec<String>,
    },
    /// Point proximity range filter
    Point {
        /// field name
        field: String,
        /// filter: base point (latitude/lat, longitude/lon), proximity range start, proximity range end, distance unit
        #[schema(value_type=(Point, RangeF64, DistanceUnit))]
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
    /// Unix timestamp: the number of seconds since 1 January 1970
    Timestamp(Range<i64>),
    F32(Range<f32>),
    F64(Range<f64>),
    String(Vec<u16>),
    Point(Point, Range<f64>, DistanceUnit, Range<u64>),
    #[default]
    None,
}

/// Specifies the sort order for the search results.
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq, ToSchema)]
pub enum SortOrder {
    /// Ascending sort order
    Ascending = 0,
    /// Descending sort order
    Descending = 1,
}

/// Specifies the sort order for the search results.
#[derive(Clone, Deserialize, Serialize, ToSchema)]
pub struct ResultSort {
    /// name of the facet field to sort by
    pub field: String,
    /// Sort order: Ascending or Descending
    pub order: SortOrder,
    /// Base value/point for (geo) proximity sorting
    pub base: FacetValue,
}

/// Specifies the sort order for the search results.
#[derive(Clone, Serialize)]
pub(crate) struct ResultSortIndex<'a> {
    /// Index/ID of the facet field to sort by
    pub idx: usize,
    /// Sort order: Ascending or Descending
    pub order: SortOrder,
    /// Base value/point for (geo) proximity sorting
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
///   The default QueryType is superseded if the query parser detects that a different query type is specified within the query string (+ - "").
/// * `offset`: offset of search results to return.
/// * `length`: number of search results to return.
///   With length=0, resultType::TopkCount will be automatically downgraded to resultType::Count, returning the number of results only, without returning the results itself.
/// * `result_type`: type of search results to return: Count, Topk, TopkCount.
/// * `include_uncommited`: true realtime search: include indexed documents which where not yet committed into search results.
/// * `field_filter`: Specify field names where to search at querytime, whereas SchemaField.indexed is set at indextime. If set to Vec::new() then all indexed fields are searched.
/// * `query_facets`: Must be set if facets should be returned in ResultObject. If set to Vec::new() then no facet fields are returned.
///   Facet fields are only collected, counted and returned for ResultType::Count and ResultType::TopkCount, but not for ResultType::Topk.
///   The prefix property of a QueryFacet allows at query time to filter the returned facet values to those matching a given prefix, if there are too many distinct values per facet field.
///   The length property of a QueryFacet allows at query time limiting the number of returned distinct values per facet field, if there are too many distinct values.  The QueryFacet can be used to improve the usability in an UI.
///   If the length property of a QueryFacet is set to 0 then no facet values for that facet are collected, counted and returned at query time. That decreases the query latency significantly.
///   The facet values are sorted by the frequency of the appearance of the value within the indexed documents matching the query in descending order.
///   Examples:
///   query_facets = vec![QueryFacet::String {field: "language".into(),prefix: "ger".into(),length: 5},QueryFacet::String {field: "brand".into(),prefix: "a".into(),length: 5}];
///   query_facets = vec![QueryFacet::U8 {field: "age".into(), range_type: RangeType::CountWithinRange, ranges: vec![("0-20".into(), 0),("20-40".into(), 20), ("40-60".into(), 40),("60-80".into(), 60), ("80-100".into(), 80)]}];
///   query_facets = vec![QueryFacet::Point {field: "location".into(),base:vec![38.8951, -77.0364],unit:DistanceUnit::Kilometers,range_type: RangeType::CountWithinRange,ranges: vec![ ("0-200".into(), 0.0),("200-400".into(), 200.0), ("400-600".into(), 400.0), ("600-800".into(), 600.0), ("800-1000".into(), 800.0)]}];
/// * `facet_filter`: Search results are filtered to documents matching specific string values or numerical ranges in the facet fields. If set to Vec::new() then result are not facet filtered.
///   The filter parameter filters the returned results to those documents both matching the query AND matching for all (boolean AND) stated facet filter fields at least one (boolean OR) of the stated values.
///   If the query is changed then both facet counts and search results are changed. If the facet filter is changed then only the search results are changed, while facet counts remain unchanged.
///   The facet counts depend only from the query and not which facet filters are selected.
///   Examples:
///   facet_filter=vec![FacetFilter::String{field:"language".into(),filter:vec!["german".into()]},FacetFilter::String{field:"brand".into(),filter:vec!["apple".into(),"google".into()]}];
///   facet_filter=vec![FacetFilter::U8{field:"age".into(),filter: 21..65}];
///   facet_filter = vec![FacetFilter::Point {field: "location".into(),filter: (vec![38.8951, -77.0364], 0.0..1000.0, DistanceUnit::Kilometers)}];
/// * `result_sort`: Sort field and order: Search results are sorted by the specified facet field, either in ascending or descending order.
///   If no sort field is specified, then the search results are sorted by rank in descending order per default.
///   Multiple sort fields are combined by a "sort by, then sort by"-method ("tie-breaking"-algorithm).
///   The results are sorted by the first field, and only for those results where the first field value is identical (tie) the results are sub-sorted by the second field,
///   until the n-th field value is either not equal or the last field is reached.
///   A special _score field (BM25x), reflecting how relevant the result is for a given search query (phrase match, match in title etc.) can be combined with any of the other sort fields as primary, secondary or n-th search criterium.
///   Sort is only enabled on facet fields that are defined in schema at create_index!
///   Examples:
///   result_sort = vec![ResultSort {field: "price".into(), order: SortOrder::Descending, base: FacetValue::None},ResultSort {field: "language".into(), order: SortOrder::Ascending, base: FacetValue::None}];
///   result_sort = vec![ResultSort {field: "location".into(),order: SortOrder::Ascending, base: FacetValue::Point(vec![38.8951, -77.0364])}];
///  
///   If query_string is empty, then index facets (collected at index time) are returned, otherwise query facets (collected at query time) are returned.
///   Facets are defined in 3 different places:
///   the facet fields are defined in schema at create_index,
///   the facet field values are set in index_document at index time,
///   the query_facets/facet_filter search parameters are specified at query time.
///   Facets are then returned in the search result object.
#[allow(clippy::too_many_arguments)]
#[allow(async_fn_in_trait)]
pub trait Search {
    /// Search the index for all indexed documents, both for committed and uncommitted documents.
    /// The latter enables true realtime search: documents are available for search in exact the same millisecond they are indexed.
    /// Arguments:
    /// * `query_string`: query string + - "" search operators are recognized.
    /// * `query_type_default`: Specifiy default QueryType: **Union** (OR, disjunction), **Intersection** (AND, conjunction), **Phrase** (""), **Not** (-).
    ///   The default QueryType is superseded if the query parser detects that a different query type is specified within the query string (+ - "").
    /// * `offset`: offset of search results to return.
    /// * `length`: number of search results to return.
    ///   With length=0, resultType::TopkCount will be automatically downgraded to resultType::Count, returning the number of results only, without returning the results itself.
    /// * `result_type`: type of search results to return: Count, Topk, TopkCount.
    /// * `include_uncommited`: true realtime search: include indexed documents which where not yet committed into search results.
    /// * `field_filter`: Specify field names where to search at querytime, whereas SchemaField.indexed is set at indextime. If set to Vec::new() then all indexed fields are searched.
    /// * `query_facets`: Must be set if facets should be returned in ResultObject. If set to Vec::new() then no facet fields are returned.
    ///   Facet fields are only collected, counted and returned for ResultType::Count and ResultType::TopkCount, but not for ResultType::Topk.
    ///   The prefix property of a QueryFacet allows at query time to filter the returned facet values to those matching a given prefix, if there are too many distinct values per facet field.
    ///   The length property of a QueryFacet allows at query time limiting the number of returned distinct values per facet field, if there are too many distinct values.  The QueryFacet can be used to improve the usability in an UI.
    ///   If the length property of a QueryFacet is set to 0 then no facet values for that facet are collected, counted and returned at query time. That decreases the query latency significantly.
    ///   The facet values are sorted by the frequency of the appearance of the value within the indexed documents matching the query in descending order.
    ///   Examples:
    ///   query_facets = vec![QueryFacet::String {field: "language".into(),prefix: "ger".into(),length: 5},QueryFacet::String {field: "brand".into(),prefix: "a".into(),length: 5}];
    ///   query_facets = vec![QueryFacet::U8 {field: "age".into(), range_type: RangeType::CountWithinRange, ranges: vec![("0-20".into(), 0),("20-40".into(), 20), ("40-60".into(), 40),("60-80".into(), 60), ("80-100".into(), 80)]}];
    ///   query_facets = vec![QueryFacet::Point {field: "location".into(),base:vec![38.8951, -77.0364],unit:DistanceUnit::Kilometers,range_type: RangeType::CountWithinRange,ranges: vec![ ("0-200".into(), 0.0),("200-400".into(), 200.0), ("400-600".into(), 400.0), ("600-800".into(), 600.0), ("800-1000".into(), 800.0)]}];
    /// * `facet_filter`: Search results are filtered to documents matching specific string values or numerical ranges in the facet fields. If set to Vec::new() then result are not facet filtered.
    ///   The filter parameter filters the returned results to those documents both matching the query AND matching for all (boolean AND) stated facet filter fields at least one (boolean OR) of the stated values.
    ///   If the query is changed then both facet counts and search results are changed. If the facet filter is changed then only the search results are changed, while facet counts remain unchanged.
    ///   The facet counts depend only from the query and not which facet filters are selected.
    ///   Examples:
    ///   facet_filter=vec![FacetFilter::String{field:"language".into(),filter:vec!["german".into()]},FacetFilter::String{field:"brand".into(),filter:vec!["apple".into(),"google".into()]}];
    ///   facet_filter=vec![FacetFilter::U8{field:"age".into(),filter: 21..65}];
    ///   facet_filter = vec![FacetFilter::Point {field: "location".into(),filter: (vec![38.8951, -77.0364], 0.0..1000.0, DistanceUnit::Kilometers)}];
    /// * `result_sort`: Sort field and order: Search results are sorted by the specified facet field, either in ascending or descending order.
    ///   If no sort field is specified, then the search results are sorted by rank in descending order per default.
    ///   Multiple sort fields are combined by a "sort by, then sort by"-method ("tie-breaking"-algorithm).
    ///   The results are sorted by the first field, and only for those results where the first field value is identical (tie) the results are sub-sorted by the second field,
    ///   until the n-th field value is either not equal or the last field is reached.
    ///   A special _score field (BM25x), reflecting how relevant the result is for a given search query (phrase match, match in title etc.) can be combined with any of the other sort fields as primary, secondary or n-th search criterium.
    ///   Sort is only enabled on facet fields that are defined in schema at create_index!
    ///   Examples:
    ///   result_sort = vec![ResultSort {field: "price".into(), order: SortOrder::Descending, base: FacetValue::None},ResultSort {field: "language".into(), order: SortOrder::Ascending, base: FacetValue::None}];
    ///   result_sort = vec![ResultSort {field: "location".into(),order: SortOrder::Ascending, base: FacetValue::Point(vec![38.8951, -77.0364])}];
    ///  
    ///   If query_string is empty, then index facets (collected at index time) are returned, otherwise query facets (collected at query time) are returned.
    ///   Facets are defined in 3 different places:
    ///   the facet fields are defined in schema at create_index,
    ///   the facet field values are set in index_document at index time,
    ///   the query_facets/facet_filter search parameters are specified at query time.
    ///   Facets are then returned in the search result object.
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
pub(crate) fn binary_search(
    byte_array: &[u8],
    len: usize,
    key_hash: u64,
    key_head_size: usize,
) -> i64 {
    let mut left = 0i64;
    let mut right = len as i64 - 1;
    while left <= right {
        let mid = (left + right) / 2;

        let pivot = read_u64(byte_array, mid as usize * key_head_size);
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
pub(crate) fn decode_posting_list_count(
    segment: &SegmentIndex,
    index: &Index,
    key_hash1: u64,
    previous: bool,
) -> Option<u32> {
    let offset = if previous { 1 } else { 0 };

    let mut posting_count_list = 0u32;
    let mut found = false;

    if segment.byte_array_blocks_pointer.len() <= offset {
        return None;
    }

    let block_id_last = segment.byte_array_blocks_pointer.len() - 1 - offset;
    for pointer in segment
        .byte_array_blocks_pointer
        .iter()
        .take(block_id_last + 1)
    {
        let key_count = pointer.2 as usize;

        let byte_array =
            &index.index_file_mmap[pointer.0 - (key_count * index.key_head_size)..pointer.0];
        let key_index = binary_search(byte_array, key_count, key_hash1, index.key_head_size);

        if key_index >= 0 {
            found = true;
            let key_address = key_index as usize * index.key_head_size;
            let posting_count = read_u16(byte_array, key_address + 8);
            posting_count_list += posting_count as u32 + 1;
        }
    }

    if found {
        Some(posting_count_list)
    } else {
        None
    }
}

#[inline(always)]
pub(crate) fn decode_posting_list_counts(
    segment: &SegmentIndex,
    index: &Index,
    key_hash1: u64,
) -> Option<(u32, u32, u32, u32)> {
    let mut posting_count_list = 0u32;
    let mut posting_count_ngram_1_compressed = 0;
    let mut posting_count_ngram_2_compressed = 0;
    let mut posting_count_ngram_3_compressed = 0;
    let mut posting_count_ngram_1 = 0;
    let mut posting_count_ngram_2 = 0;
    let mut posting_count_ngram_3 = 0;
    let mut found = false;

    let read_flag = key_hash1 & 0b111 > 0;

    if segment.byte_array_blocks_pointer.is_empty() {
        return None;
    }

    for pointer in segment.byte_array_blocks_pointer.iter() {
        let key_count = pointer.2 as usize;

        let byte_array =
            &index.index_file_mmap[pointer.0 - (key_count * index.key_head_size)..pointer.0];
        let key_index = binary_search(byte_array, key_count, key_hash1, index.key_head_size);

        if key_index >= 0 {
            found = true;
            let key_address = key_index as usize * index.key_head_size;
            let posting_count = read_u16(byte_array, key_address + 8);

            match index.key_head_size {
                20 => {}
                22 => {
                    if read_flag {
                        posting_count_ngram_1_compressed = read_u8(byte_array, key_address + 14);
                        posting_count_ngram_2_compressed = read_u8(byte_array, key_address + 15);
                    }
                }
                _ => {
                    if read_flag {
                        posting_count_ngram_1_compressed = read_u8(byte_array, key_address + 14);
                        posting_count_ngram_2_compressed = read_u8(byte_array, key_address + 15);
                        posting_count_ngram_3_compressed = read_u8(byte_array, key_address + 16);
                    }
                }
            }

            posting_count_list += posting_count as u32 + 1;
        }
    }

    if found {
        match index.key_head_size {
            20 => {}
            22 => {
                if read_flag {
                    posting_count_ngram_1 =
                        DOCUMENT_LENGTH_COMPRESSION[posting_count_ngram_1_compressed as usize];
                    posting_count_ngram_2 =
                        DOCUMENT_LENGTH_COMPRESSION[posting_count_ngram_2_compressed as usize];
                }
            }
            _ => {
                if read_flag {
                    posting_count_ngram_1 =
                        DOCUMENT_LENGTH_COMPRESSION[posting_count_ngram_1_compressed as usize];
                    posting_count_ngram_2 =
                        DOCUMENT_LENGTH_COMPRESSION[posting_count_ngram_2_compressed as usize];
                    posting_count_ngram_3 =
                        DOCUMENT_LENGTH_COMPRESSION[posting_count_ngram_3_compressed as usize];
                }
            }
        }

        Some((
            posting_count_list,
            posting_count_ngram_1,
            posting_count_ngram_2,
            posting_count_ngram_3,
        ))
    } else {
        None
    }
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
    let mut blocks_owned: Vec<BlockObjectIndex> = Vec::new();
    let mut posting_count_ngram_1_compressed = 0;
    let mut posting_count_ngram_2_compressed = 0;
    let mut posting_count_ngram_3_compressed = 0;
    let mut posting_count_ngram_1 = 0;
    let mut posting_count_ngram_2 = 0;
    let mut posting_count_ngram_3 = 0;

    let read_flag = key_hash1 & 0b111 > 0;

    for (block_id, pointer) in segment.byte_array_blocks_pointer.iter().enumerate() {
        let key_count = pointer.2 as usize;

        let byte_array =
            &index.index_file_mmap[pointer.0 - (key_count * index.key_head_size)..pointer.0];
        let key_index = binary_search(byte_array, key_count, key_hash1, index.key_head_size);

        if key_index >= 0 {
            let key_address = key_index as usize * index.key_head_size;
            let posting_count = read_u16(byte_array, key_address + 8);

            let max_docid = read_u16(byte_array, key_address + 10);
            let max_p_docid = read_u16(byte_array, key_address + 12);

            match index.key_head_size {
                20 => {}
                22 => {
                    if read_flag {
                        posting_count_ngram_1_compressed = read_u8(byte_array, key_address + 14);
                        posting_count_ngram_2_compressed = read_u8(byte_array, key_address + 15);
                    }
                }
                _ => {
                    if read_flag {
                        posting_count_ngram_1_compressed = read_u8(byte_array, key_address + 14);
                        posting_count_ngram_2_compressed = read_u8(byte_array, key_address + 15);
                        posting_count_ngram_3_compressed = read_u8(byte_array, key_address + 16);
                    }
                }
            }

            let pointer_pivot_p_docid = read_u16(byte_array, key_address + index.key_head_size - 6);
            let compression_type_pointer =
                read_u32(byte_array, key_address + index.key_head_size - 4);

            posting_count_list += posting_count as u32 + 1;

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
            match index.key_head_size {
                20 => {}
                22 => {
                    if read_flag {
                        posting_count_ngram_1 =
                            DOCUMENT_LENGTH_COMPRESSION[posting_count_ngram_1_compressed as usize];
                        posting_count_ngram_2 =
                            DOCUMENT_LENGTH_COMPRESSION[posting_count_ngram_2_compressed as usize];
                    }
                }
                _ => {
                    if read_flag {
                        posting_count_ngram_1 =
                            DOCUMENT_LENGTH_COMPRESSION[posting_count_ngram_1_compressed as usize];
                        posting_count_ngram_2 =
                            DOCUMENT_LENGTH_COMPRESSION[posting_count_ngram_2_compressed as usize];
                        posting_count_ngram_3 =
                            DOCUMENT_LENGTH_COMPRESSION[posting_count_ngram_3_compressed as usize];
                    }
                }
            }

            let ngram_type =
                FromPrimitive::from_u64(key_hash1 & 0b111).unwrap_or(NgramType::SingleTerm);

            for block in blocks_owned.iter_mut() {
                block.max_block_score = get_max_score(
                    index,
                    segment,
                    posting_count_ngram_1,
                    posting_count_ngram_2,
                    posting_count_ngram_3,
                    posting_count_list,
                    block.block_id as usize,
                    block.max_docid as usize,
                    block.max_p_docid as usize,
                    block.pointer_pivot_p_docid as usize,
                    block.compression_type_pointer,
                    &ngram_type,
                );

                if block.max_block_score > max_list_score {
                    max_list_score = block.max_block_score
                }
            }
        }

        let posting_list_object_index = PostingListObjectIndex {
            posting_count: posting_count_list,
            posting_count_ngram_1,
            posting_count_ngram_2,
            posting_count_ngram_3,
            max_list_score,
            blocks: blocks_owned,
            position_range_previous: 0,
            ..Default::default()
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
    ///   The default QueryType is superseded if the query parser detects that a different query type is specified within the query string (+ - "").
    /// * `offset`: offset of search results to return.
    /// * `length`: number of search results to return.
    ///   With length=0, resultType::TopkCount will be automatically downgraded to resultType::Count, returning the number of results only, without returning the results itself.
    /// * `result_type`: type of search results to return: Count, Topk, TopkCount.
    /// * `include_uncommited`: true realtime search: include indexed documents which where not yet committed into search results.
    /// * `field_filter`: Specify field names where to search at querytime, whereas SchemaField.indexed is set at indextime. If set to Vec::new() then all indexed fields are searched.
    /// * `query_facets`: Must be set if facets should be returned in ResultObject. If set to Vec::new() then no facet fields are returned.
    ///   Facet fields are only collected, counted and returned for ResultType::Count and ResultType::TopkCount, but not for ResultType::Topk.
    ///   The prefix property of a QueryFacet allows at query time to filter the returned facet values to those matching a given prefix, if there are too many distinct values per facet field.
    ///   The length property of a QueryFacet allows at query time limiting the number of returned distinct values per facet field, if there are too many distinct values.  The prefix and length properties can be used to improve the usability in an UI.
    ///   If the length property of a QueryFacet is set to 0 then no facet values for that facet are collected, counted and returned at query time. That decreases the query latency significantly.
    ///   The facet values are sorted by the frequency of the appearance of the value within the indexed documents matching the query in descending order.
    ///   Examples:
    ///   query_facets = vec![QueryFacet::String {field: "language".into(),prefix: "ger".into(),length: 5},QueryFacet::String {field: "brand".into(),prefix: "a".into(),length: 5}];
    ///   query_facets = vec![QueryFacet::U8 {field: "age".into(), range_type: RangeType::CountWithinRange, ranges: vec![("0-20".into(), 0),("20-40".into(), 20), ("40-60".into(), 40),("60-80".into(), 60), ("80-100".into(), 80)]}];
    ///   query_facets = vec![QueryFacet::Point {field: "location".into(),base:vec![38.8951, -77.0364],unit:DistanceUnit::Kilometers,range_type: RangeType::CountWithinRange,ranges: vec![ ("0-200".into(), 0.0),("200-400".into(), 200.0), ("400-600".into(), 400.0), ("600-800".into(), 600.0), ("800-1000".into(), 800.0)]}];
    /// * `facet_filter`: Search results are filtered to documents matching specific string values or numerical ranges in the facet fields. If set to Vec::new() then result are not facet filtered.
    ///   The filter parameter filters the returned results to those documents both matching the query AND matching for all (boolean AND) stated facet filter fields at least one (boolean OR) of the stated values.
    ///   If the query is changed then both facet counts and search results are changed. If the facet filter is changed then only the search results are changed, while facet counts remain unchanged.
    ///   The facet counts depend only from the query and not which facet filters are selected.
    ///   Examples:
    ///   facet_filter=vec![FacetFilter::String{field:"language".into(),filter:vec!["german".into()]},FacetFilter::String{field:"brand".into(),filter:vec!["apple".into(),"google".into()]}];
    ///   facet_filter=vec![FacetFilter::U8{field:"age".into(),filter: 21..65}];
    ///   facet_filter = vec![FacetFilter::Point {field: "location".into(),filter: (vec![38.8951, -77.0364], 0.0..1000.0, DistanceUnit::Kilometers)}];
    /// * `result_sort`: Sort field and order: Search results are sorted by the specified facet field, either in ascending or descending order.
    ///   If no sort field is specified, then the search results are sorted by rank in descending order per default.
    ///   Multiple sort fields are combined by a "sort by, then sort by"-method ("tie-breaking"-algorithm).
    ///   The results are sorted by the first field, and only for those results where the first field value is identical (tie) the results are sub-sorted by the second field,
    ///   until the n-th field value is either not equal or the last field is reached.
    ///   A special _score field (BM25x), reflecting how relevant the result is for a given search query (phrase match, match in title etc.) can be combined with any of the other sort fields as primary, secondary or n-th search criterium.
    ///   Sort is only enabled on facet fields that are defined in schema at create_index!   
    ///   Examples:
    ///   result_sort = vec![ResultSort {field: "price".into(), order: SortOrder::Descending, base: FacetValue::None},ResultSort {field: "language".into(), order: SortOrder::Ascending, base: FacetValue::None}];
    ///   result_sort = vec![ResultSort {field: "location".into(),order: SortOrder::Ascending, base: FacetValue::Point(vec![38.8951, -77.0364])}];
    ///   If query_string is empty, then index facets (collected at index time) are returned, otherwise query facets (collected at query time) are returned.
    ///   Facets are defined in 3 different places:
    ///   the facet fields are defined in schema at create_index,
    ///   the facet field values are set in index_document at index time,
    ///   the query_facets/facet_filter search parameters are specified at query time.
    ///   Facets are then returned in the search result object.
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

        let mut result_type = result_type;
        if length == 0 && result_type != ResultType::Count {
            if result_type == ResultType::Topk {
                return result_object;
            }
            result_type = ResultType::Count;
        }

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
                        if let Some(idx) = index_ref.facets_map.get(field)
                            && index_ref.facets[*idx].field_type == FieldType::U8
                        {
                            facet_filter_sparse[*idx] = FilterSparse::U8(filter.clone())
                        }
                    }
                    FacetFilter::U16 { field, filter } => {
                        if let Some(idx) = index_ref.facets_map.get(field)
                            && index_ref.facets[*idx].field_type == FieldType::U16
                        {
                            facet_filter_sparse[*idx] = FilterSparse::U16(filter.clone())
                        }
                    }
                    FacetFilter::U32 { field, filter } => {
                        if let Some(idx) = index_ref.facets_map.get(field)
                            && index_ref.facets[*idx].field_type == FieldType::U32
                        {
                            facet_filter_sparse[*idx] = FilterSparse::U32(filter.clone())
                        }
                    }
                    FacetFilter::U64 { field, filter } => {
                        if let Some(idx) = index_ref.facets_map.get(field)
                            && index_ref.facets[*idx].field_type == FieldType::U64
                        {
                            facet_filter_sparse[*idx] = FilterSparse::U64(filter.clone())
                        }
                    }
                    FacetFilter::I8 { field, filter } => {
                        if let Some(idx) = index_ref.facets_map.get(field)
                            && index_ref.facets[*idx].field_type == FieldType::I8
                        {
                            facet_filter_sparse[*idx] = FilterSparse::I8(filter.clone())
                        }
                    }
                    FacetFilter::I16 { field, filter } => {
                        if let Some(idx) = index_ref.facets_map.get(field)
                            && index_ref.facets[*idx].field_type == FieldType::I16
                        {
                            facet_filter_sparse[*idx] = FilterSparse::I16(filter.clone())
                        }
                    }
                    FacetFilter::I32 { field, filter } => {
                        if let Some(idx) = index_ref.facets_map.get(field)
                            && index_ref.facets[*idx].field_type == FieldType::I32
                        {
                            facet_filter_sparse[*idx] = FilterSparse::I32(filter.clone())
                        }
                    }
                    FacetFilter::I64 { field, filter } => {
                        if let Some(idx) = index_ref.facets_map.get(field)
                            && index_ref.facets[*idx].field_type == FieldType::I64
                        {
                            facet_filter_sparse[*idx] = FilterSparse::I64(filter.clone())
                        }
                    }
                    FacetFilter::Timestamp { field, filter } => {
                        if let Some(idx) = index_ref.facets_map.get(field)
                            && index_ref.facets[*idx].field_type == FieldType::Timestamp
                        {
                            facet_filter_sparse[*idx] = FilterSparse::Timestamp(filter.clone())
                        }
                    }
                    FacetFilter::F32 { field, filter } => {
                        if let Some(idx) = index_ref.facets_map.get(field)
                            && index_ref.facets[*idx].field_type == FieldType::F32
                        {
                            facet_filter_sparse[*idx] = FilterSparse::F32(filter.clone())
                        }
                    }
                    FacetFilter::F64 { field, filter } => {
                        if let Some(idx) = index_ref.facets_map.get(field)
                            && index_ref.facets[*idx].field_type == FieldType::F64
                        {
                            facet_filter_sparse[*idx] = FilterSparse::F64(filter.clone())
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
                        if let Some(idx) = index_ref.facets_map.get(field)
                            && index_ref.facets[*idx].field_type == FieldType::Point
                        {
                            facet_filter_sparse[*idx] = FilterSparse::Point(
                                filter.0.clone(),
                                filter.1.clone(),
                                filter.2.clone(),
                                point_distance_to_morton_range(&filter.0, filter.1.end, &filter.2),
                            );
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
                        if let Some(idx) = index_ref.facets_map.get(field)
                            && index_ref.facets[*idx].field_type == FieldType::U8
                        {
                            is_range_facet = true;
                            search_result.query_facets[*idx] = ResultFacet {
                                field: field.clone(),
                                length: u16::MAX,
                                ranges: Ranges::U8(range_type.clone(), ranges.clone()),
                                ..Default::default()
                            };
                        }
                    }
                    QueryFacet::U16 {
                        field,
                        range_type,
                        ranges,
                    } => {
                        if let Some(idx) = index_ref.facets_map.get(field)
                            && index_ref.facets[*idx].field_type == FieldType::U16
                        {
                            is_range_facet = true;
                            search_result.query_facets[*idx] = ResultFacet {
                                field: field.clone(),
                                length: u16::MAX,
                                ranges: Ranges::U16(range_type.clone(), ranges.clone()),
                                ..Default::default()
                            };
                        }
                    }
                    QueryFacet::U32 {
                        field,
                        range_type,
                        ranges,
                    } => {
                        if let Some(idx) = index_ref.facets_map.get(field)
                            && index_ref.facets[*idx].field_type == FieldType::U32
                        {
                            is_range_facet = true;
                            search_result.query_facets[*idx] = ResultFacet {
                                field: field.clone(),
                                length: u16::MAX,
                                ranges: Ranges::U32(range_type.clone(), ranges.clone()),
                                ..Default::default()
                            };
                        }
                    }
                    QueryFacet::U64 {
                        field,
                        range_type,
                        ranges,
                    } => {
                        if let Some(idx) = index_ref.facets_map.get(field)
                            && index_ref.facets[*idx].field_type == FieldType::U64
                        {
                            is_range_facet = true;
                            search_result.query_facets[*idx] = ResultFacet {
                                field: field.clone(),
                                length: u16::MAX,
                                ranges: Ranges::U64(range_type.clone(), ranges.clone()),
                                ..Default::default()
                            };
                        }
                    }
                    QueryFacet::I8 {
                        field,
                        range_type,
                        ranges,
                    } => {
                        if let Some(idx) = index_ref.facets_map.get(field)
                            && index_ref.facets[*idx].field_type == FieldType::I8
                        {
                            is_range_facet = true;
                            search_result.query_facets[*idx] = ResultFacet {
                                field: field.clone(),
                                length: u16::MAX,
                                ranges: Ranges::I8(range_type.clone(), ranges.clone()),
                                ..Default::default()
                            };
                        }
                    }
                    QueryFacet::I16 {
                        field,
                        range_type,
                        ranges,
                    } => {
                        if let Some(idx) = index_ref.facets_map.get(field)
                            && index_ref.facets[*idx].field_type == FieldType::I16
                        {
                            is_range_facet = true;
                            search_result.query_facets[*idx] = ResultFacet {
                                field: field.clone(),
                                length: u16::MAX,
                                ranges: Ranges::I16(range_type.clone(), ranges.clone()),
                                ..Default::default()
                            };
                        }
                    }
                    QueryFacet::I32 {
                        field,
                        range_type,
                        ranges,
                    } => {
                        if let Some(idx) = index_ref.facets_map.get(field)
                            && index_ref.facets[*idx].field_type == FieldType::I32
                        {
                            is_range_facet = true;
                            search_result.query_facets[*idx] = ResultFacet {
                                field: field.clone(),
                                length: u16::MAX,
                                ranges: Ranges::I32(range_type.clone(), ranges.clone()),
                                ..Default::default()
                            };
                        }
                    }
                    QueryFacet::I64 {
                        field,
                        range_type,
                        ranges,
                    } => {
                        if let Some(idx) = index_ref.facets_map.get(field)
                            && index_ref.facets[*idx].field_type == FieldType::I64
                        {
                            is_range_facet = true;
                            search_result.query_facets[*idx] = ResultFacet {
                                field: field.clone(),
                                length: u16::MAX,
                                ranges: Ranges::I64(range_type.clone(), ranges.clone()),
                                ..Default::default()
                            };
                        }
                    }
                    QueryFacet::Timestamp {
                        field,
                        range_type,
                        ranges,
                    } => {
                        if let Some(idx) = index_ref.facets_map.get(field)
                            && index_ref.facets[*idx].field_type == FieldType::Timestamp
                        {
                            is_range_facet = true;
                            search_result.query_facets[*idx] = ResultFacet {
                                field: field.clone(),
                                length: u16::MAX,
                                ranges: Ranges::Timestamp(range_type.clone(), ranges.clone()),
                                ..Default::default()
                            };
                        }
                    }
                    QueryFacet::F32 {
                        field,
                        range_type,
                        ranges,
                    } => {
                        if let Some(idx) = index_ref.facets_map.get(field)
                            && index_ref.facets[*idx].field_type == FieldType::F32
                        {
                            is_range_facet = true;
                            search_result.query_facets[*idx] = ResultFacet {
                                field: field.clone(),
                                length: u16::MAX,
                                ranges: Ranges::F32(range_type.clone(), ranges.clone()),
                                ..Default::default()
                            };
                        }
                    }
                    QueryFacet::F64 {
                        field,
                        range_type,
                        ranges,
                    } => {
                        if let Some(idx) = index_ref.facets_map.get(field)
                            && index_ref.facets[*idx].field_type == FieldType::F64
                        {
                            is_range_facet = true;
                            search_result.query_facets[*idx] = ResultFacet {
                                field: field.clone(),
                                length: u16::MAX,
                                ranges: Ranges::F64(range_type.clone(), ranges.clone()),
                                ..Default::default()
                            };
                        }
                    }
                    QueryFacet::String {
                        field,
                        prefix,
                        length,
                    } => {
                        if let Some(idx) = index_ref.facets_map.get(field)
                            && index_ref.facets[*idx].field_type == FieldType::String
                        {
                            search_result.query_facets[*idx] = ResultFacet {
                                field: field.clone(),
                                prefix: prefix.clone(),
                                length: *length,
                                ..Default::default()
                            }
                        }
                    }
                    QueryFacet::StringSet {
                        field,
                        prefix,
                        length,
                    } => {
                        if let Some(idx) = index_ref.facets_map.get(field)
                            && index_ref.facets[*idx].field_type == FieldType::StringSet
                        {
                            search_result.query_facets[*idx] = ResultFacet {
                                field: field.clone(),
                                prefix: prefix.clone(),
                                length: *length,
                                ..Default::default()
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
                        if let Some(idx) = index_ref.facets_map.get(field)
                            && index_ref.facets[*idx].field_type == FieldType::Point
                        {
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
                &index_ref,
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
                index_ref.meta.ngram_indexing,
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
            let mut preceding_ngram_count = 0;

            let mut blocks_vec: Vec<Vec<BlockObjectIndex>> = Vec::new();

            let mut not_found_terms_hashset: AHashSet<u64> = AHashSet::new();

            for non_unique_term in non_unique_terms.iter() {
                let term = unique_terms.get(&non_unique_term.term).unwrap();
                let key0: u32 = term.key0;
                let key_hash: u64 = term.key_hash;
                let term_no_diacritics_umlaut_case = &non_unique_term.term;

                let mut idf = 0.0;
                let mut idf_ngram1 = 0.0;
                let mut idf_ngram2 = 0.0;
                let mut idf_ngram3 = 0.0;

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

                            if let Some(plo) = posting_list_object_index_option {
                                posting_count = plo.posting_count;
                                max_list_score = plo.max_list_score;
                                blocks = &DUMMY_VEC;
                                blocks_len = plo.blocks.len();
                                blocks_vec.push(plo.blocks);
                                true
                            } else {
                                posting_count = 0;
                                max_list_score = 0.0;
                                blocks = &DUMMY_VEC;
                                blocks_len = 0;
                                false
                            }
                        } else {
                            let posting_list_object_index_option = index_ref.segments_index
                                [key0 as usize]
                                .segment
                                .get(&key_hash);

                            if let Some(plo) = posting_list_object_index_option {
                                posting_count = plo.posting_count;
                                max_list_score = plo.max_list_score;
                                blocks_len = plo.blocks.len();
                                blocks = &plo.blocks;
                                true
                            } else {
                                posting_count = 0;
                                max_list_score = 0.0;
                                blocks = &DUMMY_VEC;
                                blocks_len = 0;
                                false
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
                                idf_ngram1,
                                idf_ngram2,
                                idf_ngram3,
                                ngram_type: non_unique_term.ngram_type.clone(),
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
                                let posting_count_ngram_1;
                                let posting_count_ngram_2;
                                let posting_count_ngram_3;
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

                                    if let Some(plo) = posting_list_object_index_option {
                                        posting_count = plo.posting_count;
                                        posting_count_ngram_1 = plo.posting_count_ngram_1;
                                        posting_count_ngram_2 = plo.posting_count_ngram_2;
                                        posting_count_ngram_3 = plo.posting_count_ngram_3;
                                        max_list_score = plo.max_list_score;
                                        blocks = &DUMMY_VEC;
                                        blocks_len = plo.blocks.len();
                                        blocks_vec.push(plo.blocks);
                                        true
                                    } else {
                                        posting_count = 0;
                                        posting_count_ngram_1 = 0;
                                        posting_count_ngram_2 = 0;
                                        posting_count_ngram_3 = 0;
                                        max_list_score = 0.0;
                                        blocks = &DUMMY_VEC;
                                        blocks_len = 0;
                                        false
                                    }
                                } else {
                                    let posting_list_object_index_option = index_ref.segments_index
                                        [key0 as usize]
                                        .segment
                                        .get(&key_hash);

                                    if let Some(plo) = posting_list_object_index_option {
                                        posting_count = plo.posting_count;
                                        posting_count_ngram_1 = plo.posting_count_ngram_1;
                                        posting_count_ngram_2 = plo.posting_count_ngram_2;
                                        posting_count_ngram_3 = plo.posting_count_ngram_3;
                                        max_list_score = plo.max_list_score;
                                        blocks_len = plo.blocks.len();
                                        blocks = &plo.blocks;
                                        true
                                    } else {
                                        posting_count = 0;
                                        posting_count_ngram_1 = 0;
                                        posting_count_ngram_2 = 0;
                                        posting_count_ngram_3 = 0;
                                        max_list_score = 0.0;
                                        blocks = &DUMMY_VEC;
                                        blocks_len = 0;
                                        false
                                    }
                                };

                                if found_plo {
                                    if result_type != ResultType::Count {
                                        if non_unique_term.ngram_type == NgramType::SingleTerm
                                            || index_ref.meta.similarity
                                                == SimilarityType::Bm25fProximity
                                        {
                                            idf = (((index_ref.indexed_doc_count as f32
                                                - posting_count as f32
                                                + 0.5)
                                                / (posting_count as f32 + 0.5))
                                                + 1.0)
                                                .ln();
                                        } else if non_unique_term.ngram_type == NgramType::NgramFF
                                            || non_unique_term.ngram_type == NgramType::NgramRF
                                            || non_unique_term.ngram_type == NgramType::NgramFR
                                        {
                                            idf_ngram1 = (((index_ref.indexed_doc_count as f32
                                                - posting_count_ngram_1 as f32
                                                + 0.5)
                                                / (posting_count_ngram_1 as f32 + 0.5))
                                                + 1.0)
                                                .ln();

                                            idf_ngram2 = (((index_ref.indexed_doc_count as f32
                                                - posting_count_ngram_2 as f32
                                                + 0.5)
                                                / (posting_count_ngram_2 as f32 + 0.5))
                                                + 1.0)
                                                .ln();
                                        } else {
                                            idf_ngram1 = (((index_ref.indexed_doc_count as f32
                                                - posting_count_ngram_1 as f32
                                                + 0.5)
                                                / (posting_count_ngram_1 as f32 + 0.5))
                                                + 1.0)
                                                .ln();

                                            idf_ngram2 = (((index_ref.indexed_doc_count as f32
                                                - posting_count_ngram_2 as f32
                                                + 0.5)
                                                / (posting_count_ngram_2 as f32 + 0.5))
                                                + 1.0)
                                                .ln();

                                            idf_ngram3 = (((index_ref.indexed_doc_count as f32
                                                - posting_count_ngram_3 as f32
                                                + 0.5)
                                                / (posting_count_ngram_3 as f32 + 0.5))
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
                                        idf_ngram1,
                                        idf_ngram2,
                                        idf_ngram3,
                                        ngram_type: non_unique_term.ngram_type.clone(),
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
                                + preceding_ngram_count,
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

                        match non_unique_term.ngram_type {
                            NgramType::SingleTerm => {}
                            NgramType::NgramFF | NgramType::NgramRF | NgramType::NgramFR => {
                                preceding_ngram_count += 1
                            }
                            _ => preceding_ngram_count += 2,
                        };

                        non_unique_query_list.push(nu_plo);
                    }
                }
                match term.ngram_type {
                    NgramType::SingleTerm => {}
                    NgramType::NgramFF | NgramType::NgramRF | NgramType::NgramFR => {
                        result_object
                            .query_terms
                            .push(term.term_ngram_1.to_string());
                        result_object
                            .query_terms
                            .push(term.term_ngram_0.to_string());
                    }
                    _ => {
                        result_object
                            .query_terms
                            .push(term.term_ngram_2.to_string());
                        result_object
                            .query_terms
                            .push(term.term_ngram_1.to_string());
                        result_object
                            .query_terms
                            .push(term.term_ngram_0.to_string());
                    }
                };

                {
                    result_object.query_terms.push(term.term.to_string());
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

            let mut matching_blocks: i32 = 0;
            let query_term_count = non_unique_terms.len();
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
                    && let Some(frequentword_result_object) = index_ref
                        .frequentword_results
                        .get(&non_unique_terms[0].term)
                {
                    result_object.query = frequentword_result_object.query.clone();
                    result_object
                        .query_terms
                        .clone_from(&frequentword_result_object.query_terms);
                    result_object.result_count = frequentword_result_object.result_count;
                    result_object.result_count_total =
                        frequentword_result_object.result_count_total;

                    if result_type != ResultType::Count {
                        result_object
                            .results
                            .clone_from(&frequentword_result_object.results);
                        if offset > 0 {
                            result_object.results.drain(..offset);
                        }
                        if length < 1000 {
                            result_object.results.truncate(length);
                        }
                    }

                    if !search_result.query_facets.is_empty() && result_type != ResultType::Topk {
                        let mut facets: AHashMap<String, Facet> = AHashMap::new();
                        for facet in search_result.query_facets.iter() {
                            if facet.length == 0
                                || frequentword_result_object.facets[&facet.field].is_empty()
                            {
                                continue;
                            }

                            let v = frequentword_result_object.facets[&facet.field]
                                .iter()
                                .sorted_unstable_by(|a, b| b.1.cmp(&a.1))
                                .map(|(a, c)| (a.clone(), *c))
                                .filter(|(a, _c)| {
                                    facet.prefix.is_empty() || a.starts_with(&facet.prefix)
                                })
                                .take(facet.length as usize)
                                .collect::<Vec<_>>();

                            if !v.is_empty() {
                                facets.insert(facet.field.clone(), v);
                            }
                        }
                        result_object.facets = facets;
                    };

                    return result_object;
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

                if result_type == ResultType::Count && query_list_len != 2 {
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
                        query_term_count,
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
                        0,
                        query_term_count,
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
                    query_term_count,
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
                let mut facets: AHashMap<String, Facet> = AHashMap::new();
                for (i, facet) in search_result.query_facets.iter_mut().enumerate() {
                    if facet.length == 0 || facet.values.is_empty() {
                        continue;
                    }

                    let v = if facet.ranges == Ranges::None {
                        if index_ref.facets[i].values.is_empty() {
                            continue;
                        }

                        if index_ref.facets[i].field_type == FieldType::StringSet {
                            let mut hash_map: AHashMap<String, usize> = AHashMap::new();
                            for value in facet.values.iter() {
                                let value2 = index_ref.facets[i]
                                    .values
                                    .get_index((*value.0).into())
                                    .unwrap();

                                for term in value2.1.0.iter() {
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
                            Ranges::Timestamp(range_type, _ranges) => range_type.clone(),
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
                                        Ranges::Timestamp(_range_type, ranges) => {
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
                        facets.insert(facet.field.clone(), v);
                    }
                }
                facets
            };
        }

        result_object
    }
}
