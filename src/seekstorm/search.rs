use crate::commit::KEY_HEAD_SIZE;
use crate::min_heap::CandidateObject;
use crate::tokenizer::tokenizer;
use crate::union::{union_docid_2, union_docid_3};
use crate::utils::{read_u16, read_u32, read_u64, read_u8};
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
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use std::cmp::Ordering as OtherOrdering;
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
#[derive(PartialEq, Clone, Debug)]
pub enum ResultType {
    Count = 0,
    Topk = 1,
    TopkCount = 2,
}

/// Contains the results returned when searching the index.
#[derive(Default, Debug, Deserialize, Serialize, Derivative, Clone)]
pub struct ResultListObject {
    pub query: String,
    pub result_count: i64,

    #[serde(rename = "countEvaluated")]
    pub result_count_total: i64,

    #[serde(rename = "count")]
    pub result_count_estimated: i64,

    #[serde(skip)]
    #[derivative(Default(value = "1"))]
    pub start: i32,

    #[serde(skip)]
    #[derivative(Default(value = "10"))]
    pub length: i32,
    pub time: i64,
    pub suggestions: Vec<String>,

    pub results: Vec<CandidateObject>,
    pub query_term_strings: Vec<String>,
}

/// Create query_list and non_unique_query_list
/// blockwise intersection : if the corresponding blocks with a 65k docid range for each term have at least a single docid,
/// then the intersect_docid within a single block is executed  (=segments?)
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
/// * `field_filter`: Specify field names where to search at querytime, whereas SchemaField.field_indexed is set at indextime. Search in all indexed fields if empty (default).
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
    ) -> ResultListObject;
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
    /// * `field_filter`: Specify field names where to search at querytime, whereas SchemaField.field_indexed is set at indextime. Search in all indexed fields if empty (default).
    async fn search(
        &self,
        query_string: String,
        query_type_default: QueryType,
        offset: usize,
        length: usize,
        result_type: ResultType,
        include_uncommited: bool,
        field_filter: Vec<String>,
    ) -> ResultListObject {
        let index_ref = self.read().await;
        let mut query_type_mut = query_type_default;
        let mut topk_candidates = MinHeap::new(offset + length);
        let mut rl: ResultListObject = Default::default();

        if index_ref.segments_index.is_empty() {
            return rl;
        }

        let mut field_filter_set: AHashSet<u16> = AHashSet::new();
        for item in field_filter.iter() {
            match index_ref.schema_map.get(item) {
                Some(value) => {
                    if value.field_indexed {
                        field_filter_set.insert(value.indexed_field_id as u16);
                    }
                }
                None => {
                    println!("field not found: {}", item)
                }
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
                    &mut topk_candidates,
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
                    match not_query_list_option {
                        None => {
                            if !not_found_terms_hashset.contains(&key_hash) {
                                let posting_count;
                                let max_list_score;
                                let blocks;
                                let blocks_len;
                                let found_plo = if index_ref.meta.access_type == AccessType::Mmap {
                                    let posting_list_object_index_option =
                                        decode_posting_list_object(
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
                        }
                        Some(_) => {}
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
                    rl.query_term_strings.push(term.term_bigram1.to_string());
                    rl.query_term_strings.push(term.term_bigram2.to_string());
                }
                {
                    rl.query_term_strings.push(term.term.to_string());
                }
            }

            let mut matching_blocks: i32 = 0;
            if query_list_len == 0 {
            } else if query_list_len == 1 {
                single_blockid(
                    &index_ref,
                    &mut non_unique_query_list,
                    &mut query_list,
                    &mut not_query_list,
                    &result_count_arc,
                    &mut topk_candidates,
                    offset + length,
                    &result_type,
                    &field_filter_set,
                    &mut matching_blocks,
                )
                .await;
            } else if query_type_mut == QueryType::Union {
                if result_type == ResultType::Count {
                    union_blockid(
                        &index_ref,
                        &mut non_unique_query_list,
                        &mut query_list,
                        &mut not_query_list,
                        &result_count_arc,
                        &mut topk_candidates,
                        offset + length,
                        &result_type,
                        &field_filter_set,
                    )
                    .await;
                } else if SPEEDUP_FLAG && query_list_len == 2 {
                    union_docid_2(
                        &index_ref,
                        &mut non_unique_query_list,
                        &mut query_list,
                        &mut not_query_list,
                        &result_count_arc,
                        &mut topk_candidates,
                        offset + length,
                        &result_type,
                        &field_filter_set,
                        &mut matching_blocks,
                    )
                    .await;
                } else if SPEEDUP_FLAG {
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
                        &mut topk_candidates,
                        offset + length,
                        &result_type,
                        &field_filter_set,
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
                        &mut topk_candidates,
                        offset + length,
                        &result_type,
                        &field_filter_set,
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
                    &mut topk_candidates,
                    offset + length,
                    &result_type,
                    &field_filter_set,
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

        rl.result_count = topk_candidates.current_heap_size as i64;

        if topk_candidates.current_heap_size > offset {
            rl.results = topk_candidates._elements;

            if topk_candidates.current_heap_size < offset + length {
                rl.results.truncate(topk_candidates.current_heap_size);
            }

            rl.results.sort_by(|a, b| {
                let result = b.score.partial_cmp(&a.score).unwrap();
                if result != OtherOrdering::Equal {
                    result
                } else {
                    a.doc_id.partial_cmp(&b.doc_id).unwrap()
                }
            });

            if offset > 0 {
                rl.results.drain(..offset);
            }
        }

        rl.result_count_total = (result_count_uncommitted_arc.load(Ordering::Relaxed)
            + result_count_arc.load(Ordering::Relaxed)) as i64;
        rl.result_count_estimated = rl.result_count_total;

        rl
    }
}
