use ahash::{AHashMap, AHashSet};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};
use tokio::sync::RwLock;
use utoipa::ToSchema;

use crate::{
    add_result::{PostingListObjectSingle, add_result_singleterm_multifield},
    index::{Index, IndexArc, NgramType, Shard},
    min_heap::Result,
    search::{FilterSparse, ResultObject, ResultSort, ResultType, SearchResult, SortOrder},
};

/// Iterator result
#[derive(Debug, Clone, Deserialize, Serialize, ToSchema)]
pub struct IteratorResultItem {
    /// document ID
    pub doc_id: u64,
    /// document
    pub doc: Option<HashMap<String, serde_json::Value>>,
}

/// Iterator
#[derive(Debug, Clone, Deserialize, Serialize, ToSchema)]
pub struct IteratorResult {
    /// number of actually skipped documents
    pub skip: usize,
    /// document IDs, and optionally the documents themselves
    pub results: Vec<IteratorResultItem>,
}

/// Document iterator
///
/// The document iterator allows to iterate over all document IDs and documents in the entire index, forward or backward.
/// It enables efficient sequential access to every document, even in very large indexes, without running a search.
/// Paging through the index works without collecting document IDs to Min-heap in size-limited RAM first.
/// The iterator guarantees that only valid document IDs are returned, even though document IDs are not strictly continuous.
/// Document IDs can also be fetched in batches, reducing round trips and significantly improving performance, especially when using the REST API.
/// Typical use cases include index export, conversion, analytics, audits, and inspection.
/// Explanation of "eventually continuous" docid:
/// In SeekStorm, document IDs become continuous over time. In a multi-sharded index, each shard maintains its own document ID space.
/// Because documents are distributed across shards in a non-deterministic, load-dependent way, shard-local document IDs advance at different rates.
/// When these are mapped to global document IDs, temporary gaps can appear.
/// As a result, simply iterating from 0 to the total document count may encounter invalid IDs near the end.
/// The Document Iterator abstracts this complexity and reliably returns only valid document IDs.
///
/// - docid=None, take>0: **skip first s document IDs**, then **take next t document IDs** of an index.
/// - docid=None, take<0: **skip last s document IDs**, then **take previous t document IDs** of an index.
/// - docid=Some, take>0: **skip next s document IDs**, then **take next t document IDs** of an index, relative to a given document ID, with end-of-index indicator.
/// - docid=Some, take<0: **skip previous s document IDs**, then **take previous t document IDs**, relative to a given document ID, with start-of-index indicator.
/// - take=0: does not make sense, that defies the purpose of get_iterator.
/// - The sign of take indicates the direction of iteration: positive take for forward iteration, negative take for backward iteration.
/// - The skip parameter is always positive, indicating the number of document IDs to skip before taking document IDs. The skip direction is determined by the sign of take too.
///
/// Next page:     take last  docid from previous result set, skip=1, take=+page_size
/// Previous page: take first docid from previous result set, skip=1, take=-page_size
///
/// Returns a tuple of (number of actually skipped document IDs, vec of taken document IDs, sorted ascending).
/// Detect end/begin of index during iteration: if returned vec.len() < requested take || if returned skip <requested skip
#[allow(async_fn_in_trait)]
pub trait GetIterator {
    /// Document iterator
    ///
    /// The document iterator allows to iterate over all document IDs and documents in the entire index, forward or backward.
    /// It enables efficient sequential access to every document, even in very large indexes, without running a search.
    /// Paging through the index works without collecting document IDs to Min-heap in size-limited RAM first.
    /// The iterator guarantees that only valid document IDs are returned, even though document IDs are not strictly continuous.
    /// Document IDs can also be fetched in batches, reducing round trips and significantly improving performance, especially when using the REST API.
    /// Typical use cases include index export, conversion, analytics, audits, and inspection.
    /// Explanation of "eventually continuous" docid:
    /// In SeekStorm, document IDs become continuous over time. In a multi-sharded index, each shard maintains its own document ID space.
    /// Because documents are distributed across shards in a non-deterministic, load-dependent way, shard-local document IDs advance at different rates.
    /// When these are mapped to global document IDs, temporary gaps can appear.
    /// As a result, simply iterating from 0 to the total document count may encounter invalid IDs near the end.
    /// The Document Iterator abstracts this complexity and reliably returns only valid document IDs.
    ///
    /// - docid=None, take>0: **skip first s document IDs**, then **take next t document IDs** of an index.
    /// - docid=None, take<0: **skip last s document IDs**, then **take previous t document IDs** of an index.
    /// - docid=Some, take>0: **skip next s document IDs**, then **take next t document IDs** of an index, relative to a given document ID, with end-of-index indicator.
    /// - docid=Some, take<0: **skip previous s document IDs**, then **take previous t document IDs**, relative to a given document ID, with start-of-index indicator.
    /// - take=0: does not make sense, that defies the purpose of get_iterator.
    /// - The sign of take indicates the direction of iteration: positive take for forward iteration, negative take for backward iteration.
    /// - The skip parameter is always positive, indicating the number of document IDs to skip before taking document IDs. The skip direction is determined by the sign of take too.
    ///
    /// Next page:     take last  docid from previous result set, skip=1, take=+page_size
    /// Previous page: take first docid from previous result set, skip=1, take=-page_size
    ///
    /// Returns a tuple of (number of actually skipped document IDs, vec of taken document IDs, sorted ascending).
    /// Detect end/begin of index during iteration: if returned vec.len() < requested take || if returned skip <requested skip
    async fn get_iterator(
        &self,
        docid: Option<u64>,
        skip: usize,
        take: isize,
        include_deleted: bool,
        include_document: bool,
        fields: Vec<String>,
    ) -> IteratorResult;
}

impl GetIterator for IndexArc {
    async fn get_iterator(
        &self,
        docid: Option<u64>,
        skip: usize,
        take: isize,
        include_deleted: bool,
        include_document: bool,
        fields: Vec<String>,
    ) -> IteratorResult {
        if take == 0 {
            return IteratorResult {
                skip,
                results: Vec::new(),
            };
        }

        let fields_hashset: std::collections::HashSet<String> =
            std::collections::HashSet::from_iter(fields);

        let mut min_docid: Option<u64> = None;
        let mut max_docid: Option<u64> = None;
        let shard_number = self.read().await.shard_number as u64;
        for (shard_id, shard) in self.read().await.shard_vec.iter().enumerate() {
            let shard_ref = shard.read().await;
            let shard_indexed_doc_count = shard_ref.indexed_doc_count as u64;

            if shard_indexed_doc_count == 0 {
                continue;
            }

            if shard_indexed_doc_count > 0 {
                let shard_max_docid =
                    shard_id as u64 + ((shard_indexed_doc_count - 1) * shard_number);

                if min_docid.is_none() {
                    min_docid = Some(shard_id as u64);
                }

                if max_docid.is_none() || shard_max_docid > max_docid.unwrap() {
                    max_docid = Some(shard_max_docid);
                }
            }
        }

        if min_docid.is_none() || max_docid.is_none() {
            return IteratorResult {
                skip,
                results: Vec::new(),
            };
        }

        let mut results: Vec<IteratorResultItem> = Vec::new();
        let mut skip_count = 0;

        if take > 0 {
            let mut docid = if let Some(docid_value) = docid {
                if docid_value < min_docid.unwrap() || docid_value > max_docid.unwrap() {
                    return IteratorResult {
                        skip,
                        results: Vec::new(),
                    };
                }
                docid_value
            } else {
                min_docid.unwrap()
            };

            while results.len() < take.unsigned_abs() {
                let shard_id = docid % shard_number;
                let docid_shard = docid / shard_number;

                let shard_ref = &self.read().await.shard_vec[shard_id as usize];
                let docid_shard_max = shard_ref.read().await.indexed_doc_count as u64;
                if docid_shard_max == 0
                    || docid_shard >= docid_shard_max
                    || (!include_deleted
                        && shard_ref
                            .read()
                            .await
                            .delete_hashset
                            .contains(&(docid_shard as usize)))
                {
                    if docid >= max_docid.unwrap() {
                        break;
                    }
                    docid += 1;
                    continue;
                }

                if skip_count < skip {
                    if docid >= max_docid.unwrap() {
                        break;
                    }
                    docid += 1;
                    skip_count += 1;
                    continue;
                }

                let result = if include_document {
                    IteratorResultItem {
                        doc_id: docid,
                        doc: self
                            .read()
                            .await
                            .get_document(
                                docid as usize,
                                false,
                                &None,
                                &fields_hashset,
                                &Vec::new(),
                            )
                            .await
                            .ok(),
                    }
                } else {
                    IteratorResultItem {
                        doc_id: docid,
                        doc: None,
                    }
                };
                results.push(result);
                if docid >= max_docid.unwrap() {
                    break;
                }
                docid += 1;
            }
            IteratorResult {
                skip: skip_count,
                results,
            }
        } else {
            let mut docid = if let Some(docid_value) = docid {
                if docid_value < min_docid.unwrap() || docid_value > max_docid.unwrap() {
                    return IteratorResult {
                        skip,
                        results: Vec::new(),
                    };
                }
                docid_value
            } else {
                max_docid.unwrap()
            };

            while results.len() < take.unsigned_abs() {
                let shard_id = docid % shard_number;
                let docid_shard = docid / shard_number;

                let shard_ref = &self.read().await.shard_vec[shard_id as usize];
                let docid_shard_max = shard_ref.read().await.indexed_doc_count as u64;
                if docid_shard_max == 0
                    || docid_shard >= docid_shard_max
                    || (!include_deleted
                        && shard_ref
                            .read()
                            .await
                            .delete_hashset
                            .contains(&(docid_shard as usize)))
                {
                    if docid <= min_docid.unwrap() {
                        break;
                    }
                    docid -= 1;

                    continue;
                }

                if skip_count < skip {
                    if docid <= min_docid.unwrap() {
                        break;
                    }
                    docid -= 1;
                    skip_count += 1;
                    continue;
                }

                let result = if include_document {
                    IteratorResultItem {
                        doc_id: docid,
                        doc: self
                            .read()
                            .await
                            .get_document(
                                docid as usize,
                                false,
                                &None,
                                &std::collections::HashSet::new(),
                                &Vec::new(),
                            )
                            .await
                            .ok(),
                    }
                } else {
                    IteratorResultItem {
                        doc_id: docid,
                        doc: None,
                    }
                };
                results.push(result);
                if docid <= min_docid.unwrap() {
                    break;
                }
                docid -= 1;
            }

            IteratorResult {
                skip: skip_count,
                results,
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn search_iterator_shard(
    shard: &Shard,
    result_type: ResultType,
    _include_uncommitted: bool,
    result_count_arc: &Arc<AtomicUsize>,
    search_result: &mut SearchResult<'_>,
    top_k: usize,
    facet_filter: &[FilterSparse],
) {
    let mut result_count_local = 0i32;

    let plo_single = PostingListObjectSingle {
        rank_position_pointer_range: 0,
        pointer_pivot_p_docid: 0,
        byte_array: &[],
        p_docid: 0,
        idf: 0.0,
        idf_ngram1: 0.0,
        idf_ngram2: 0.0,
        idf_ngram3: 0.0,
        ngram_type: NgramType::SingleTerm,
    };

    let mut not_query_list = Vec::new();

    for docid in 0..shard.indexed_doc_count {
        add_result_singleterm_multifield(
            shard,
            docid,
            &mut result_count_local,
            search_result,
            top_k,
            &result_type,
            &AHashSet::new(),
            facet_filter,
            &plo_single,
            &mut not_query_list,
            0.0,
        );
    }

    result_count_arc.fetch_add(result_count_local as usize, Ordering::Relaxed);
}

pub(crate) async fn search_iterator_index(
    index_arc: &Arc<RwLock<Index>>,
    offset: usize,
    length: usize,
    result_type: ResultType,
    _include_uncommitted: bool,
    result_sort: Vec<ResultSort>,
) -> ResultObject {
    let mut result_object = ResultObject {
        original_query: "".to_string(),
        query: "".to_string(),
        query_terms: Vec::new(),
        result_count: 0,
        result_count_total: 0,
        results: Vec::new(),
        facets: AHashMap::new(),
        suggestions: Vec::new(),
    };

    let indexed_doc_count = index_arc.read().await.indexed_doc_count().await;

    if result_type != ResultType::Count {
        let iterator = if result_sort.len() == 1
            && !result_sort.is_empty()
            && result_sort.first().unwrap().order == SortOrder::Ascending
        {
            index_arc
                .get_iterator(None, offset, length as isize, false, false, vec![])
                .await
        } else {
            index_arc
                .get_iterator(None, offset, -(length as isize), false, false, vec![])
                .await
        };

        for result in iterator.results.iter() {
            result_object.results.push(Result {
                doc_id: result.doc_id as usize,
                score: 0.0,
            });
        }

        result_object.result_count = result_object.results.len();
    }
    result_object.result_count_total = indexed_doc_count;

    result_object
}
