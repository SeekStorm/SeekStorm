use crate::{
    add_result::add_result_multiterm_multifield,
    compatible::{_blsr_u64, _mm_tzcnt_64},
    index::{
        AccessType, CompressionType, Index, NonUniquePostingListObjectQuery,
        PostingListObjectQuery, SORT_FLAG, SPEEDUP_FLAG,
    },
    intersection_simd::intersection_vector16,
    search::{FilterSparse, ResultType, SearchResult},
    utils::{cast_byte_ulong_slice, cast_byte_ushort_slice, read_u16, read_u64},
};
use ahash::AHashSet;
use num_traits::FromPrimitive;
use std::{
    cmp,
    cmp::Ordering as OtherOrdering,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

pub(crate) fn bitpacking32_get_delta(body: &[u8], bitposition: u32, rangebits: u32) -> u16 {
    let bodyspan = &body[((bitposition >> 3) as usize)..];
    let bodyspan_4: &[u8; 4] = bodyspan.try_into().unwrap();
    let source_bytes = u32::from_be_bytes(*bodyspan_4);
    ((source_bytes >> (32 - rangebits - (bitposition & 7)) as i32)
        & (0b1111_1111_1111_1111_1111_1111_1111_1111 >> (32 - rangebits as i32))) as u16
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn intersection_bitmap_2(
    result_count: &mut i32,
    block_id: usize,
    index: &Index,
    search_result: &mut SearchResult,
    top_k: usize,
    result_type: &ResultType,
    field_filter_set: &AHashSet<u16>,
    facet_filter: &[FilterSparse],
    non_unique_query_list: &mut [NonUniquePostingListObjectQuery],
    query_list: &mut [PostingListObjectQuery],
    not_query_list: &mut [PostingListObjectQuery],
    phrase_query: bool,
    filtered: bool,
    block_score: f32,
    all_terms_frequent: bool,
) {
    let ulongs0 = cast_byte_ulong_slice(
        &query_list[0].byte_array
            [query_list[0].compressed_doc_id_range..query_list[0].compressed_doc_id_range + 8192],
    );
    let ulongs1 = cast_byte_ulong_slice(
        &query_list[1].byte_array
            [query_list[1].compressed_doc_id_range..query_list[1].compressed_doc_id_range + 8192],
    );

    for ulong_pos in 0..1024 {
        let mut bits1 = ulongs0[ulong_pos];
        let mut bits2 = ulongs1[ulong_pos];

        let mut intersect = bits1 & bits2;

        if !filtered && result_type == &ResultType::Count {
            *result_count += u64::count_ones(intersect) as i32;
        } else if !filtered
            && search_result.topk_candidates.current_heap_size == top_k
            && block_score <= search_result.topk_candidates._elements[0].score
        {
            if result_type != &ResultType::Topk {
                *result_count += u64::count_ones(intersect) as i32;
            }
        } else {
            while intersect != 0 {
                let bit_pos = unsafe { _mm_tzcnt_64(intersect) } as usize;
                let doc_id1 = (ulong_pos << 6) + bit_pos;

                if bit_pos > 0 {
                    let mask2 = u64::MAX << bit_pos;
                    let mask1 = !mask2;
                    query_list[0].p_docid += (bits1 & mask1).count_ones() as usize;
                    query_list[1].p_docid += (bits2 & mask1).count_ones() as usize;
                    bits1 &= mask2;
                    bits2 &= mask2;
                }

                add_result_multiterm_multifield(
                    index,
                    (block_id << 16) | doc_id1 as usize,
                    result_count,
                    search_result,
                    top_k,
                    result_type,
                    field_filter_set,
                    facet_filter,
                    non_unique_query_list,
                    query_list,
                    not_query_list,
                    phrase_query,
                    block_score,
                    all_terms_frequent,
                );

                intersect = unsafe { _blsr_u64(intersect) };
            }
            query_list[0].p_docid += bits1.count_ones() as usize;
            query_list[1].p_docid += bits2.count_ones() as usize;
        }
    }
}

#[allow(clippy::too_many_arguments)]
#[allow(clippy::never_loop)]
pub(crate) async fn intersection_docid(
    index: &Index,
    non_unique_query_list: &mut [NonUniquePostingListObjectQuery<'_>],
    query_list: &mut [PostingListObjectQuery<'_>],
    not_query_list: &mut [PostingListObjectQuery<'_>],
    block_id: usize,
    result_count: &mut i32,
    search_result: &mut SearchResult,
    top_k: usize,
    result_type: &ResultType,
    field_filter_set: &AHashSet<u16>,
    facet_filter: &[FilterSparse],
    phrase_query: bool,
    block_score: f32,
) {
    let t1 = 0;
    let mut t2 = 1;

    let filtered = !not_query_list.is_empty()
        || phrase_query
        || !field_filter_set.is_empty()
        || (!search_result.query_facets.is_empty() || !facet_filter.is_empty())
            && (!search_result.skip_facet_count || !facet_filter.is_empty());

    for plo in not_query_list.iter_mut() {
        let query_list_item_mut = plo;

        let result = query_list_item_mut
            .blocks
            .binary_search_by(|block| block.block_id.cmp(&(block_id as u32)));
        match result {
            Ok(p_block) => {
                query_list_item_mut.bm25_flag = true;
                query_list_item_mut.p_block = p_block as i32
            }
            Err(_) => {
                query_list_item_mut.bm25_flag = false;
                continue;
            }
        };

        let blo = &query_list_item_mut.blocks[query_list_item_mut.p_block as usize];

        query_list_item_mut.compression_type =
            FromPrimitive::from_i32((blo.compression_type_pointer >> 30) as i32).unwrap();

        query_list_item_mut.rank_position_pointer_range =
            blo.compression_type_pointer & 0b0011_1111_1111_1111_1111_1111_1111_1111;

        let posting_pointer_size_sum = blo.pointer_pivot_p_docid as usize * 2
            + if (blo.pointer_pivot_p_docid as usize) <= blo.posting_count as usize {
                ((blo.posting_count as usize + 1) - blo.pointer_pivot_p_docid as usize) * 3
            } else {
                0
            };
        query_list_item_mut.compressed_doc_id_range =
            query_list_item_mut.rank_position_pointer_range as usize + posting_pointer_size_sum;

        query_list_item_mut.p_docid = 0;
        query_list_item_mut.p_docid_count = blo.posting_count as usize + 1;

        query_list_item_mut.pointer_pivot_p_docid = blo.pointer_pivot_p_docid;

        query_list_item_mut.docid = 0;

        if query_list_item_mut.compression_type == CompressionType::Rle {
            query_list_item_mut.p_run_count = read_u16(
                query_list_item_mut.byte_array,
                query_list_item_mut.compressed_doc_id_range,
            ) as i32;
            let startdocid = read_u16(
                query_list_item_mut.byte_array,
                query_list_item_mut.compressed_doc_id_range + 2,
            );
            let runlength = read_u16(
                query_list_item_mut.byte_array,
                query_list_item_mut.compressed_doc_id_range + 4,
            );
            query_list_item_mut.docid = startdocid as i32;
            query_list_item_mut.run_end = (startdocid + runlength) as i32;
            query_list_item_mut.p_run_sum = runlength as i32;
            query_list_item_mut.p_run = 6;
        }
    }

    let mut all_terms_frequent = true;
    for query_list_item_mut in query_list.iter_mut() {
        let blo = &query_list_item_mut.blocks[query_list_item_mut.p_block as usize];

        query_list_item_mut.p_docid = 0;
        query_list_item_mut.p_docid_count = blo.posting_count as usize + 1;

        if query_list_item_mut.bm25_flag
            && (query_list_item_mut.posting_count as f32) / (index.indexed_doc_count as f32) < 0.5
        {
            all_terms_frequent = false;
        }

        query_list_item_mut.compression_type =
            FromPrimitive::from_i32((blo.compression_type_pointer >> 30) as i32).unwrap();

        query_list_item_mut.rank_position_pointer_range =
            blo.compression_type_pointer & 0b0011_1111_1111_1111_1111_1111_1111_1111;

        query_list_item_mut.pointer_pivot_p_docid = blo.pointer_pivot_p_docid;

        let posting_pointer_size_sum = blo.pointer_pivot_p_docid as usize * 2
            + if (blo.pointer_pivot_p_docid as usize) <= blo.posting_count as usize {
                ((blo.posting_count as usize + 1) - blo.pointer_pivot_p_docid as usize) * 3
            } else {
                0
            };
        query_list_item_mut.compressed_doc_id_range =
            query_list_item_mut.rank_position_pointer_range as usize + posting_pointer_size_sum;

        query_list_item_mut.docid = 0;
        query_list_item_mut.p_run = 0;
        query_list_item_mut.p_run_count = 0;

        query_list_item_mut.p_run_sum =
            if query_list_item_mut.compression_type == CompressionType::Rle {
                read_u16(
                    query_list_item_mut.byte_array,
                    query_list_item_mut.compressed_doc_id_range + 4,
                )
                .into()
            } else {
                0
            };
    }

    if SPEEDUP_FLAG &&
    /* !phrase_query && */
    (result_type == &ResultType::Topk)
        && (search_result.topk_candidates.current_heap_size == top_k)
        && (block_score <= search_result.topk_candidates._elements[0].score)
    {
        return;
    }

    query_list.sort_unstable_by(|x, y| {
        if (x.compression_type == CompressionType::Bitmap)
            != (y.compression_type == CompressionType::Bitmap)
        {
            if x.compression_type == CompressionType::Bitmap {
                OtherOrdering::Greater
            } else {
                OtherOrdering::Less
            }
        } else {
            x.blocks[x.p_block as usize]
                .posting_count
                .partial_cmp(&y.blocks[y.p_block as usize].posting_count)
                .unwrap()
        }
    });

    'restart: loop {
        match (
            &query_list[t1].compression_type,
            &query_list[t2].compression_type,
        ) {
            (CompressionType::Array, CompressionType::Array) => 'exit: loop {
                let ushorts1 = cast_byte_ushort_slice(
                    &query_list[t1].byte_array[query_list[t1].compressed_doc_id_range..],
                );
                let mut ushorts2 = cast_byte_ushort_slice(
                    &query_list[t2].byte_array[query_list[t2].compressed_doc_id_range..],
                );

                let mut doc_id1: u16 = ushorts1[query_list[t1].p_docid];
                let mut doc_id2: u16 = ushorts2[query_list[t2].p_docid];

                if query_list.len() == 2
                    && cfg!(any(target_arch = "x86_64", target_arch = "aarch64"))
                {
                    intersection_vector16(
                        ushorts1,
                        query_list[0].p_docid_count,
                        ushorts2,
                        query_list[1].p_docid_count,
                        result_count,
                        block_id,
                        index,
                        search_result,
                        top_k,
                        result_type,
                        field_filter_set,
                        facet_filter,
                        non_unique_query_list,
                        query_list,
                        not_query_list,
                        phrase_query,
                        all_terms_frequent,
                    );
                    break 'exit;
                }

                loop {
                    match doc_id1.cmp(&doc_id2) {
                        std::cmp::Ordering::Less => {
                            if t2 > 1 {
                                t2 = 1;
                                if query_list[t2].compression_type != CompressionType::Array {
                                    query_list[t1].p_docid += 1;
                                    if query_list[t1].p_docid == query_list[t1].p_docid_count {
                                        break;
                                    }
                                    continue 'restart;
                                } else {
                                    ushorts2 = cast_byte_ushort_slice(
                                        &query_list[t2].byte_array
                                            [query_list[t2].compressed_doc_id_range..],
                                    );

                                    doc_id2 = ushorts2[query_list[t2].p_docid];
                                }
                            }

                            query_list[t1].p_docid += 1;
                            if query_list[t1].p_docid == query_list[t1].p_docid_count {
                                break;
                            }
                            doc_id1 = ushorts1[query_list[t1].p_docid];
                        }
                        std::cmp::Ordering::Greater => {
                            query_list[t2].p_docid += 1;
                            if query_list[t2].p_docid == query_list[t2].p_docid_count {
                                break;
                            }

                            let mut bound = 2;
                            while (query_list[t2].p_docid + bound < query_list[t2].p_docid_count)
                                && (ushorts2[query_list[t2].p_docid + bound] < doc_id1)
                            {
                                query_list[t2].p_docid += bound;
                                bound <<= 1;
                            }

                            doc_id2 = ushorts2[query_list[t2].p_docid];
                        }
                        std::cmp::Ordering::Equal => {
                            if t2 + 1 < query_list.len() {
                                t2 += 1;
                                if query_list[t2].compression_type != CompressionType::Array {
                                    continue 'restart;
                                } else {
                                    ushorts2 = cast_byte_ushort_slice(
                                        &query_list[t2].byte_array
                                            [query_list[t2].compressed_doc_id_range..],
                                    );
                                    doc_id2 = ushorts2[query_list[t2].p_docid];
                                    continue;
                                }
                            }

                            add_result_multiterm_multifield(
                                index,
                                (block_id << 16) | doc_id1 as usize,
                                result_count,
                                search_result,
                                top_k,
                                result_type,
                                field_filter_set,
                                facet_filter,
                                non_unique_query_list,
                                query_list,
                                not_query_list,
                                phrase_query,
                                block_score,
                                all_terms_frequent,
                            );

                            query_list[t1].p_docid += 1;
                            if query_list[t1].p_docid == query_list[t1].p_docid_count {
                                break 'exit;
                            }
                            for item in query_list.iter_mut().skip(1) {
                                if item.compression_type == CompressionType::Array {
                                    item.p_docid += 1;
                                    if item.p_docid == item.p_docid_count {
                                        break 'exit;
                                    }
                                } else if (item.compression_type == CompressionType::Rle)
                                    && (doc_id1 == item.run_end as u16)
                                {
                                    item.p_run += 1;
                                    if item.p_run == item.p_run_count {
                                        break 'exit;
                                    }
                                    item.p_run_sum += read_u16(
                                        item.byte_array,
                                        item.compressed_doc_id_range
                                            + 4
                                            + (item.p_run << 2) as usize,
                                    ) as i32;
                                }
                            }

                            t2 = 1;
                            if query_list[t2].compression_type != CompressionType::Array {
                                continue 'restart;
                            }
                            doc_id1 = ushorts1[query_list[t1].p_docid];
                            ushorts2 = cast_byte_ushort_slice(
                                &query_list[t2].byte_array
                                    [query_list[t2].compressed_doc_id_range..],
                            );
                            doc_id2 = ushorts2[query_list[t2].p_docid];
                        }
                    }
                }

                break;
            },

            (CompressionType::Array, CompressionType::Delta) => {
                'exit: loop {
                    // !!!kein galopping für delta side!!!

                    let ushorts1 = cast_byte_ushort_slice(
                        &query_list[t1].byte_array[query_list[t1].compressed_doc_id_range..],
                    );
                    let mut doc_id1: u16 = ushorts1[query_list[t1].p_docid];
                    let mut doc_id2: u16 = query_list[t2].docid as u16;

                    loop {
                        match doc_id1.cmp(&doc_id2) {
                            std::cmp::Ordering::Less => {
                                if t2 > 1 {
                                    t2 = 1;
                                    if query_list[t2].compression_type != CompressionType::Delta {
                                        query_list[t1].p_docid += 1;
                                        if query_list[t1].p_docid == query_list[t1].p_docid_count {
                                            break;
                                        }
                                        continue 'restart;
                                    } else {
                                        doc_id2 = query_list[t2].docid as u16;
                                    }
                                }

                                query_list[t1].p_docid += 1;
                                if query_list[t1].p_docid == query_list[t1].p_docid_count {
                                    break;
                                }
                                doc_id1 = ushorts1[query_list[t1].p_docid];
                            }
                            std::cmp::Ordering::Greater => {
                                query_list[t2].p_docid += 1;
                                if query_list[t2].p_docid == query_list[t2].p_docid_count {
                                    break;
                                }

                                query_list[t2].bitposition += query_list[t2].rangebits as u32;
                                doc_id2 = query_list[t2].docid as u16
                                    + bitpacking32_get_delta(
                                        query_list[t2].byte_array,
                                        query_list[t2].bitposition,
                                        query_list[t2].rangebits as u32,
                                    )
                                    + 1;
                                query_list[t2].docid = doc_id2 as i32;
                            }
                            std::cmp::Ordering::Equal => {
                                if t2 + 1 < query_list.len() {
                                    t2 += 1;
                                    if query_list[t2].compression_type != CompressionType::Delta {
                                        continue 'restart;
                                    } else {
                                        doc_id2 = query_list[t2].docid as u16;
                                        continue;
                                    }
                                }

                                add_result_multiterm_multifield(
                                    index,
                                    (block_id << 16) | doc_id1 as usize,
                                    result_count,
                                    search_result,
                                    top_k,
                                    result_type,
                                    field_filter_set,
                                    facet_filter,
                                    non_unique_query_list,
                                    query_list,
                                    not_query_list,
                                    phrase_query,
                                    block_score,
                                    all_terms_frequent,
                                );

                                for item in query_list.iter_mut() {
                                    if item.compression_type == CompressionType::Array {
                                        item.p_docid += 1;
                                        if item.p_docid == item.p_docid_count {
                                            break 'exit;
                                        }
                                    } else if (item.compression_type == CompressionType::Rle)
                                        && (doc_id1 == item.run_end as u16)
                                    {
                                        item.p_run += 1;
                                        if item.p_run == item.p_run_count {
                                            break 'exit;
                                        }
                                        item.p_run_sum += read_u16(
                                            item.byte_array,
                                            item.compressed_doc_id_range
                                                + 4
                                                + (item.p_run << 2) as usize,
                                        )
                                            as i32;
                                    }
                                }

                                t2 = 1;
                                if query_list[t2].compression_type != CompressionType::Delta {
                                    continue 'restart;
                                }
                                doc_id1 = ushorts1[query_list[t1].p_docid];
                                doc_id2 = query_list[t2].docid as u16;
                            }
                        }
                    }

                    break;
                }
            }
            (CompressionType::Bitmap, CompressionType::Bitmap) => 'exit: loop {
                if query_list.len() == 2 && SPEEDUP_FLAG {
                    intersection_bitmap_2(
                        result_count,
                        block_id,
                        index,
                        search_result,
                        top_k,
                        result_type,
                        field_filter_set,
                        facet_filter,
                        non_unique_query_list,
                        query_list,
                        not_query_list,
                        phrase_query,
                        filtered,
                        block_score,
                        all_terms_frequent,
                    );
                    break 'exit;
                }

                let ulongs1 = cast_byte_ulong_slice(
                    &query_list[t1].byte_array[query_list[t1].compressed_doc_id_range..],
                );
                let mut ulongs2 = cast_byte_ulong_slice(
                    &query_list[t2].byte_array[query_list[t2].compressed_doc_id_range..],
                );

                let mut intersect_mask: u64 = u64::MAX << (query_list[t1].docid & 63);

                for ulong_pos in (query_list[t1].docid as usize >> 6)..1024 {
                    let mut intersect: u64 =
                        ulongs1[ulong_pos] & ulongs2[ulong_pos] & intersect_mask;

                    while intersect != 0 {
                        let bit_pos = unsafe { _mm_tzcnt_64(intersect) } as usize;
                        let doc_id1 = (ulong_pos << 6) + bit_pos;

                        if t2 + 1 < query_list.len() {
                            for item in ulongs2
                                .iter()
                                .take(ulong_pos)
                                .skip(query_list[t2].p_run as usize)
                            {
                                query_list[t2].p_run_sum += item.count_ones() as i32
                            }
                            query_list[t2].p_docid = if bit_pos == 0 {
                                query_list[t2].p_run_sum as usize
                            } else {
                                query_list[t2].p_run_sum as usize
                                    + (ulongs2[ulong_pos] << (64 - bit_pos)).count_ones() as usize
                            };

                            query_list[t2].p_run = ulong_pos as i32;

                            t2 += 1;

                            if query_list[t2].compression_type != CompressionType::Bitmap {
                                query_list[t1].docid = doc_id1 as i32;
                                continue 'restart;
                            } else {
                                ulongs2 = cast_byte_ulong_slice(
                                    &query_list[t2].byte_array
                                        [query_list[t2].compressed_doc_id_range..],
                                );
                                intersect &= ulongs2[ulong_pos];

                                if ((1u64 << bit_pos) & intersect) == 0 {
                                    t2 = 1;
                                    ulongs2 = cast_byte_ulong_slice(
                                        &query_list[t2].byte_array
                                            [query_list[t2].compressed_doc_id_range..],
                                    );
                                }
                                continue;
                            }
                        }

                        intersect = unsafe { _blsr_u64(intersect) };

                        if SPEEDUP_FLAG
                            && !filtered
                            && (search_result.topk_candidates.current_heap_size == top_k)
                            && (block_score <= search_result.topk_candidates._elements[0].score)
                        {
                            if result_type != &ResultType::Topk {
                                *result_count += 1;
                            }
                        } else {
                            for item in ulongs1
                                .iter()
                                .take(ulong_pos)
                                .skip(query_list[t1].p_run as usize)
                            {
                                query_list[t1].p_run_sum += item.count_ones() as i32
                            }
                            query_list[t1].p_docid = if bit_pos == 0 {
                                query_list[t1].p_run_sum as usize
                            } else {
                                query_list[t1].p_run_sum as usize
                                    + (ulongs1[ulong_pos] << (64 - bit_pos)).count_ones() as usize
                            };
                            query_list[t1].p_run = ulong_pos as i32;

                            for item in ulongs2
                                .iter()
                                .take(ulong_pos)
                                .skip(query_list[t2].p_run as usize)
                            {
                                query_list[t2].p_run_sum += item.count_ones() as i32
                            }
                            query_list[t2].p_docid = if bit_pos == 0 {
                                query_list[t2].p_run_sum as usize
                            } else {
                                query_list[t2].p_run_sum as usize
                                    + (ulongs2[ulong_pos] << (64 - bit_pos)).count_ones() as usize
                            };
                            query_list[t2].p_run = ulong_pos as i32;

                            add_result_multiterm_multifield(
                                index,
                                (block_id << 16) | doc_id1 as usize,
                                result_count,
                                search_result,
                                top_k,
                                result_type,
                                field_filter_set,
                                facet_filter,
                                non_unique_query_list,
                                query_list,
                                not_query_list,
                                phrase_query,
                                block_score,
                                all_terms_frequent,
                            );

                            for item in query_list.iter_mut().skip(1) {
                                if item.compression_type == CompressionType::Array {
                                    item.p_docid += 1;
                                    if item.p_docid == item.p_docid_count {
                                        break 'exit;
                                    }
                                } else if (item.compression_type == CompressionType::Rle)
                                    && (doc_id1 == item.run_end as usize)
                                {
                                    item.p_run += 1;
                                    if item.p_run == item.p_run_count {
                                        break 'exit;
                                    }
                                    item.p_run_sum += read_u16(
                                        item.byte_array,
                                        item.compressed_doc_id_range
                                            + 4
                                            + (item.p_run << 2) as usize,
                                    ) as i32;
                                }
                            }
                        }

                        t2 = 1;
                        if query_list[t2].compression_type != CompressionType::Bitmap {
                            query_list[t1].docid = doc_id1 as i32 + 1;
                            continue 'restart;
                        }
                        ulongs2 = cast_byte_ulong_slice(
                            &query_list[t2].byte_array[query_list[t2].compressed_doc_id_range..],
                        );
                    }

                    intersect_mask = u64::MAX;
                }

                break;
            },

            (CompressionType::Array, CompressionType::Bitmap) => 'exit: loop {
                let ushorts1 = cast_byte_ushort_slice(
                    &query_list[t1].byte_array[query_list[t1].compressed_doc_id_range..],
                );

                if query_list.len() == 2 {
                    let block_id_bits = block_id << 16;
                    let mut p_docid = query_list[0].p_docid;
                    let compressed_doc_id_range = query_list[1].compressed_doc_id_range;
                    let p_docid_count = query_list[0].p_docid_count;
                    loop {
                        let doc_id1 = ushorts1[p_docid];
                        if (query_list[1].byte_array
                            [compressed_doc_id_range + (doc_id1 >> 3) as usize]
                            & (1u32 << (doc_id1 & 7)) as u8)
                            > 0
                        {
                            query_list[0].p_docid = p_docid;

                            let byte_pos = (doc_id1 >> 6) << 3;
                            let bit_pos = doc_id1 & 63;
                            for i in (((query_list[t2].p_run << 3) as usize)..byte_pos as usize)
                                .step_by(8)
                            {
                                query_list[t2].p_run_sum += read_u64(
                                    query_list[t2].byte_array,
                                    query_list[t2].compressed_doc_id_range + i,
                                )
                                .count_ones()
                                    as i32;
                            }

                            query_list[t2].p_docid = if bit_pos == 0 {
                                query_list[t2].p_run_sum as usize
                            } else {
                                query_list[t2].p_run_sum as usize
                                    + (read_u64(
                                        query_list[t2].byte_array,
                                        query_list[t2].compressed_doc_id_range + byte_pos as usize,
                                    ) << (64 - bit_pos))
                                        .count_ones() as usize
                            };

                            query_list[t2].p_run = (doc_id1 >> 6) as i32;
                            add_result_multiterm_multifield(
                                index,
                                block_id_bits | doc_id1 as usize,
                                result_count,
                                search_result,
                                top_k,
                                result_type,
                                field_filter_set,
                                facet_filter,
                                non_unique_query_list,
                                query_list,
                                not_query_list,
                                phrase_query,
                                block_score,
                                all_terms_frequent,
                            );
                        }

                        p_docid += 1;
                        if p_docid == p_docid_count {
                            break 'exit;
                        }
                    }
                }

                loop {
                    let doc_id1 = ushorts1[query_list[t1].p_docid];

                    if (query_list[t2].byte_array
                        [query_list[t2].compressed_doc_id_range + (doc_id1 >> 3) as usize]
                        & (1u32 << (doc_id1 & 7)) as u8)
                        > 0
                    {
                        let byte_pos2 = (doc_id1 >> 6) << 3;
                        let bit_pos2 = doc_id1 & 63;

                        if t2 + 1 < query_list.len() {
                            for i in (((query_list[t2].p_run << 3) as usize)..byte_pos2 as usize)
                                .step_by(8)
                            {
                                query_list[t2].p_run_sum += read_u64(
                                    query_list[t2].byte_array,
                                    query_list[t2].compressed_doc_id_range + i,
                                )
                                .count_ones()
                                    as i32;
                            }
                            query_list[t2].p_docid = if bit_pos2 == 0 {
                                query_list[t2].p_run_sum as usize
                            } else {
                                query_list[t2].p_run_sum as usize
                                    + (read_u64(
                                        query_list[t2].byte_array,
                                        query_list[t2].compressed_doc_id_range + byte_pos2 as usize,
                                    ) << (64 - bit_pos2))
                                        .count_ones() as usize
                            };
                            query_list[t2].p_run = (doc_id1 >> 6) as i32;

                            t2 += 1;
                            if query_list[t2].compression_type != CompressionType::Bitmap {
                                continue 'restart;
                            } else {
                                continue;
                            }
                        }

                        for i in
                            (((query_list[t2].p_run << 3) as usize)..byte_pos2 as usize).step_by(8)
                        {
                            query_list[t2].p_run_sum += (read_u64(
                                query_list[t2].byte_array,
                                query_list[t2].compressed_doc_id_range + i,
                            ))
                            .count_ones()
                                as i32;
                        }
                        query_list[t2].p_docid = if bit_pos2 == 0 {
                            query_list[t2].p_run_sum as usize
                        } else {
                            query_list[t2].p_run_sum as usize
                                + (read_u64(
                                    query_list[t2].byte_array,
                                    query_list[t2].compressed_doc_id_range + byte_pos2 as usize,
                                ) << (64 - bit_pos2))
                                    .count_ones() as usize
                        };
                        query_list[t2].p_run = (doc_id1 >> 6) as i32;

                        add_result_multiterm_multifield(
                            index,
                            (block_id << 16) | doc_id1 as usize,
                            result_count,
                            search_result,
                            top_k,
                            result_type,
                            field_filter_set,
                            facet_filter,
                            non_unique_query_list,
                            query_list,
                            not_query_list,
                            phrase_query,
                            block_score,
                            all_terms_frequent,
                        );

                        for item in query_list.iter_mut().skip(1) {
                            if item.compression_type == CompressionType::Array {
                                item.p_docid += 1;
                                if item.p_docid == item.p_docid_count {
                                    break 'exit;
                                }
                            } else if (item.compression_type == CompressionType::Rle)
                                && (doc_id1 as i32 == item.run_end)
                            {
                                item.p_run += 1;
                                if item.p_run == item.p_run_count {
                                    break 'exit;
                                }
                                item.p_run_sum += read_u16(
                                    item.byte_array,
                                    item.compressed_doc_id_range + 4 + (item.p_run << 2) as usize,
                                ) as i32;
                            }
                        }
                    }

                    query_list[t1].p_docid += 1;
                    if query_list[t1].p_docid == query_list[t1].p_docid_count {
                        break 'exit;
                    }
                    t2 = 1;
                    if query_list[t2].compression_type != CompressionType::Bitmap {
                        continue 'restart;
                    }
                }
            },

            (CompressionType::Array, CompressionType::Rle) => 'exit: loop {
                let ushorts1 = cast_byte_ushort_slice(
                    &query_list[t1].byte_array[query_list[t1].compressed_doc_id_range..],
                );
                let mut ushorts2 = cast_byte_ushort_slice(
                    &query_list[t2].byte_array[query_list[t2].compressed_doc_id_range..],
                );

                query_list[t2].p_run_count = ushorts2[0] as i32;

                let mut doc_id1 = ushorts1[query_list[t1].p_docid];
                let mut run_start2 = ushorts2[(1 + query_list[t2].p_run * 2) as usize];
                let mut run_length2 = ushorts2[(2 + query_list[t2].p_run * 2) as usize];
                let mut run_end2 = run_start2 + run_length2;
                query_list[t2].run_end = run_end2 as i32;

                loop {
                    if doc_id1 > run_end2 {
                        query_list[t2].p_run += 1;
                        if query_list[t2].p_run == query_list[t2].p_run_count {
                            break;
                        }

                        if false {
                            let mut bound: i32 = 2;
                            while (query_list[t2].p_run + bound < query_list[t2].p_run_count)
                                && (ushorts2[1 + ((query_list[t2].p_run + bound) << 1) as usize]
                                    + ushorts2[2 + ((query_list[t2].p_run + bound) << 1) as usize]
                                    < doc_id1)
                            {
                                query_list[t2].p_run += bound;
                                bound <<= 1;
                            }
                        }

                        run_start2 = ushorts2[(1 + query_list[t2].p_run * 2) as usize];
                        run_length2 = ushorts2[(2 + query_list[t2].p_run * 2) as usize];
                        run_end2 = run_start2 + run_length2;
                        query_list[t2].p_run_sum += run_length2 as i32;
                        query_list[t2].run_end = run_end2 as i32;
                    } else if doc_id1 < run_start2 {
                        if t2 > 1 {
                            t2 = 1;
                            if query_list[t2].compression_type != CompressionType::Rle {
                                query_list[t1].p_docid += 1;
                                if query_list[t1].p_docid == query_list[t1].p_docid_count {
                                    break;
                                }
                                continue 'restart;
                            } else {
                                ushorts2 = cast_byte_ushort_slice(
                                    &query_list[t2].byte_array
                                        [query_list[t2].compressed_doc_id_range..],
                                );

                                run_start2 = ushorts2[(1 + query_list[t2].p_run * 2) as usize];
                                run_end2 = query_list[t2].run_end as u16;
                            }
                        }

                        query_list[t1].p_docid += 1;
                        if query_list[t1].p_docid == query_list[t1].p_docid_count {
                            break;
                        }
                        doc_id1 = ushorts1[query_list[t1].p_docid];
                    } else {
                        if t2 + 1 < query_list.len() {
                            query_list[t2].p_docid = query_list[t2].p_run_sum as usize
                                - run_length2 as usize
                                + doc_id1 as usize
                                - run_start2 as usize
                                + query_list[t2].p_run as usize;
                            t2 += 1;
                            if query_list[t2].compression_type != CompressionType::Rle {
                                continue 'restart;
                            } else {
                                ushorts2 = cast_byte_ushort_slice(
                                    &query_list[t2].byte_array
                                        [query_list[t2].compressed_doc_id_range..],
                                );

                                query_list[t2].p_docid_count = ushorts2[0] as usize;
                                run_start2 = ushorts2[(1 + query_list[t2].p_run * 2) as usize];
                                run_length2 = ushorts2[(2 + query_list[t2].p_run * 2) as usize];
                                run_end2 = run_start2 + run_length2;
                                query_list[t2].run_end = run_end2 as i32;

                                continue;
                            }
                        }

                        query_list[t2].p_docid = query_list[t2].p_run_sum as usize
                            - run_length2 as usize
                            + doc_id1 as usize
                            - run_start2 as usize
                            + query_list[t2].p_run as usize;
                        add_result_multiterm_multifield(
                            index,
                            (block_id << 16) | doc_id1 as usize,
                            result_count,
                            search_result,
                            top_k,
                            result_type,
                            field_filter_set,
                            facet_filter,
                            non_unique_query_list,
                            query_list,
                            not_query_list,
                            phrase_query,
                            block_score,
                            all_terms_frequent,
                        );

                        query_list[t1].p_docid += 1;
                        if query_list[t1].p_docid == query_list[t1].p_docid_count {
                            break 'exit;
                        }
                        for item in query_list.iter_mut().skip(1) {
                            if item.compression_type == CompressionType::Array {
                                item.p_docid += 1;
                                if item.p_docid == item.p_docid_count {
                                    break 'exit;
                                }
                            } else if (item.compression_type == CompressionType::Rle)
                                && (doc_id1 as i32 == item.run_end)
                            {
                                item.p_run += 1;
                                if item.p_run == item.p_run_count {
                                    break 'exit;
                                }
                                item.p_run_sum += read_u16(
                                    item.byte_array,
                                    item.compressed_doc_id_range + 4 + (item.p_run << 2) as usize,
                                ) as i32;
                            }
                        }

                        t2 = 1;
                        if query_list[t2].compression_type != CompressionType::Rle {
                            continue 'restart;
                        }
                        doc_id1 = ushorts1[query_list[t1].p_docid];
                        ushorts2 = cast_byte_ushort_slice(
                            &query_list[t2].byte_array[query_list[t2].compressed_doc_id_range..],
                        );
                        run_start2 = ushorts2[(1 + query_list[t2].p_run * 2) as usize];
                        run_length2 = ushorts2[(2 + query_list[t2].p_run * 2) as usize];
                        run_end2 = run_start2 + run_length2;
                        query_list[t2].run_end = run_end2 as i32;
                    }
                }

                break;
            },

            (CompressionType::Delta, CompressionType::Delta) => {
                'exit: loop {
                    // !!!kein galopping für delta side!!!

                    let mut doc_id1: u16 = query_list[t1].docid as u16;
                    let mut doc_id2: u16 = query_list[t2].docid as u16;

                    loop {
                        match doc_id1.cmp(&doc_id2) {
                            std::cmp::Ordering::Less => {
                                if t2 > 1 {
                                    t2 = 1;
                                    if query_list[t2].compression_type != CompressionType::Delta {
                                        query_list[t1].p_docid += 1;
                                        if query_list[t1].p_docid == query_list[t1].p_docid_count {
                                            break;
                                        }
                                        continue 'restart;
                                    } else {
                                        doc_id2 = query_list[t2].docid as u16;
                                    }
                                }

                                query_list[t1].p_docid += 1;
                                if query_list[t1].p_docid == query_list[t1].p_docid_count {
                                    break;
                                }

                                query_list[t1].bitposition += query_list[t1].rangebits as u32;
                                doc_id1 = query_list[t1].docid as u16
                                    + bitpacking32_get_delta(
                                        query_list[t1].byte_array,
                                        query_list[t1].bitposition,
                                        query_list[t1].rangebits as u32,
                                    )
                                    + 1;
                                query_list[t1].docid = doc_id1 as i32;
                            }
                            std::cmp::Ordering::Greater => {
                                query_list[t2].p_docid += 1;
                                if query_list[t2].p_docid == query_list[t2].p_docid_count {
                                    break;
                                }

                                query_list[t2].bitposition += query_list[t2].rangebits as u32;
                                doc_id2 = query_list[t2].docid as u16
                                    + bitpacking32_get_delta(
                                        query_list[t2].byte_array,
                                        query_list[t2].bitposition,
                                        query_list[t2].rangebits as u32,
                                    )
                                    + 1;
                                query_list[t2].docid = doc_id2 as i32;
                            }
                            std::cmp::Ordering::Equal => {
                                if t2 + 1 < query_list.len() {
                                    t2 += 1;
                                    if query_list[t2].compression_type != CompressionType::Delta {
                                        continue 'restart;
                                    } else {
                                        doc_id2 = query_list[t2].docid as u16;
                                        continue;
                                    }
                                }

                                add_result_multiterm_multifield(
                                    index,
                                    (block_id << 16) | doc_id1 as usize,
                                    result_count,
                                    search_result,
                                    top_k,
                                    result_type,
                                    field_filter_set,
                                    facet_filter,
                                    non_unique_query_list,
                                    query_list,
                                    not_query_list,
                                    phrase_query,
                                    block_score,
                                    all_terms_frequent,
                                );

                                for item in query_list.iter_mut() {
                                    if item.compression_type == CompressionType::Array {
                                        item.p_docid += 1;
                                        if item.p_docid == item.p_docid_count {
                                            break 'exit;
                                        }
                                    } else if (item.compression_type == CompressionType::Rle)
                                        && (doc_id1 == item.run_end as u16)
                                    {
                                        item.p_run += 1;
                                        if item.p_run == item.p_run_count {
                                            break 'exit;
                                        }
                                        item.p_run_sum += read_u16(
                                            item.byte_array,
                                            item.compressed_doc_id_range
                                                + 4
                                                + (item.p_run << 2) as usize,
                                        )
                                            as i32;
                                    }
                                }

                                t2 = 1;
                                if query_list[t2].compression_type != CompressionType::Delta {
                                    continue 'restart;
                                }
                                doc_id1 = query_list[t1].docid as u16;
                                doc_id2 = query_list[t2].docid as u16;
                            }
                        }
                    }

                    break;
                }
            }

            (CompressionType::Bitmap, CompressionType::Delta) => 'exit: loop {
                loop {
                    let doc_id2 = query_list[t2].docid as u16;
                    let byte_pos = doc_id2 >> 3;
                    let bit_pos = doc_id2 & 7;

                    if (query_list[t1].byte_array
                        [query_list[t1].compressed_doc_id_range + byte_pos as usize]
                        & (1u32 << bit_pos) as u8)
                        > 0
                    {
                        if t2 + 1 < query_list.len() {
                            t2 += 1;
                            if query_list[t2].compression_type != CompressionType::Delta {
                                continue 'restart;
                            } else {
                                continue;
                            }
                        }

                        add_result_multiterm_multifield(
                            index,
                            (block_id << 16) | doc_id2 as usize,
                            result_count,
                            search_result,
                            top_k,
                            result_type,
                            field_filter_set,
                            facet_filter,
                            non_unique_query_list,
                            query_list,
                            not_query_list,
                            phrase_query,
                            block_score,
                            all_terms_frequent,
                        );

                        for item in query_list.iter_mut().skip(1) {
                            if item.compression_type == CompressionType::Array {
                                item.p_docid += 1;
                                if item.p_docid == item.p_docid_count {
                                    break 'exit;
                                }
                            } else if (item.compression_type == CompressionType::Rle)
                                && (doc_id2 == item.run_end as u16)
                            {
                                item.p_run += 1;
                                if item.p_run == item.p_run_count {
                                    break 'exit;
                                }
                                item.p_run_sum += read_u16(
                                    item.byte_array,
                                    item.compressed_doc_id_range + 4 + (item.p_run << 2) as usize,
                                ) as i32;
                            }
                        }

                        t2 = 1;
                        if query_list[t2].compression_type != CompressionType::Delta {
                            continue 'restart;
                        }
                    } else {
                        query_list[t2].p_docid += 1;
                        if query_list[t2].p_docid == query_list[t2].p_docid_count {
                            break 'exit;
                        }
                    }
                }
            },

            (CompressionType::Rle, CompressionType::Rle) => 'exit: loop {
                let ushorts1 = cast_byte_ushort_slice(
                    &query_list[t1].byte_array[query_list[t1].compressed_doc_id_range..],
                );
                let mut ushorts2 = cast_byte_ushort_slice(
                    &query_list[t2].byte_array[query_list[t2].compressed_doc_id_range..],
                );

                query_list[t1].p_run_count = ushorts1[0] as i32;
                let mut runstart1 = ushorts1[(1 + query_list[t1].p_run * 2) as usize];
                let mut runlength1 = ushorts1[(2 + query_list[t1].p_run * 2) as usize];
                let mut runend1 = runstart1 + runlength1;
                query_list[t1].run_end = runend1 as i32;

                query_list[t2].p_run_count = ushorts2[0] as i32;
                let mut runstart2 = ushorts2[(1 + query_list[t2].p_run * 2) as usize];
                let mut runlength2 = ushorts2[(2 + query_list[t2].p_run * 2) as usize];
                let mut runend2 = runstart2 + runlength2;
                query_list[t2].run_end = runend2 as i32;

                runstart1 = cmp::max(runstart1, query_list[t1].docid as u16);
                runstart2 = cmp::max(runstart2, query_list[t1].docid as u16);

                'start: loop {
                    if runstart1 > runend2 {
                        query_list[t2].p_run += 1;
                        if query_list[t2].p_run == query_list[t2].p_run_count {
                            break;
                        }

                        runstart2 = ushorts2[(1 + query_list[t2].p_run * 2) as usize];
                        runlength2 = ushorts2[(2 + query_list[t2].p_run * 2) as usize];
                        runend2 = runstart2 + runlength2;
                        query_list[t2].p_run_sum += runlength2 as i32;
                        query_list[t2].run_end = runend2 as i32;
                    } else if runend1 < runstart2 {
                        if t2 > 1 {
                            t2 = 1;
                            if query_list[t2].compression_type != CompressionType::Rle {
                                query_list[t1].p_run += 1;
                                if query_list[t1].p_run == query_list[t1].p_run_count {
                                    break;
                                }

                                query_list[t1].p_run_sum += read_u16(
                                    query_list[t1].byte_array,
                                    query_list[t1].compressed_doc_id_range
                                        + 4
                                        + (query_list[t1].p_run << 2) as usize,
                                )
                                    as i32;

                                continue 'restart;
                            } else {
                                ushorts2 = cast_byte_ushort_slice(
                                    &query_list[t2].byte_array
                                        [query_list[t2].compressed_doc_id_range..],
                                );
                                runstart2 = ushorts2[(1 + query_list[t2].p_run * 2) as usize];
                                runlength2 = ushorts2[(2 + query_list[t2].p_run * 2) as usize];
                                runend2 = query_list[t2].run_end as u16;
                            }
                        }

                        query_list[t1].p_run += 1;
                        if query_list[t1].p_run == query_list[t1].p_run_count {
                            break;
                        }

                        runstart1 = ushorts1[(1 + query_list[t1].p_run * 2) as usize];
                        runlength1 = ushorts1[(2 + query_list[t1].p_run * 2) as usize];
                        runend1 = runstart1 + runlength1;
                        query_list[t1].p_run_sum += runlength1 as i32;
                        query_list[t1].run_end = runend1 as i32;
                    } else {
                        for doc_id in cmp::max(
                            query_list[t1].docid,
                            cmp::max(runstart1 as i32, runstart2 as i32),
                        )
                            ..=(cmp::min(runend1 as i32, runend2 as i32))
                        {
                            if t2 + 1 < query_list.len() {
                                query_list[t2].p_docid = query_list[t2].p_run_sum as usize
                                    - runlength2 as usize
                                    + doc_id as usize
                                    - ushorts2[(1 + query_list[t2].p_run * 2) as usize] as usize
                                    + query_list[t2].p_run as usize;
                                t2 += 1;
                                if query_list[t2].compression_type == CompressionType::Rle {
                                    query_list[t1].docid = doc_id;
                                    continue 'restart;
                                } else {
                                    ushorts2 = cast_byte_ushort_slice(
                                        &query_list[t2].byte_array
                                            [query_list[t2].compressed_doc_id_range..],
                                    );
                                    query_list[t2].p_docid_count = ushorts2[0] as usize;
                                    runstart2 = ushorts2[(1 + query_list[t2].p_run * 2) as usize];
                                    runlength2 = ushorts2[(2 + query_list[t2].p_run * 2) as usize];
                                    runend2 = runstart2 + runlength2;
                                    query_list[t2].run_end = runend2 as i32;

                                    continue 'start;
                                }
                            }

                            query_list[t1].p_docid = query_list[t1].p_run_sum as usize
                                - runlength1 as usize
                                + doc_id as usize
                                - ushorts1[(1 + query_list[t1].p_run * 2) as usize] as usize
                                + query_list[t1].p_run as usize;
                            query_list[t2].p_docid = query_list[t2].p_run_sum as usize
                                - runlength2 as usize
                                + doc_id as usize
                                - ushorts2[(1 + query_list[t2].p_run * 2) as usize] as usize
                                + query_list[t2].p_run as usize;
                            add_result_multiterm_multifield(
                                index,
                                (block_id << 16) | doc_id as usize,
                                result_count,
                                search_result,
                                top_k,
                                result_type,
                                field_filter_set,
                                facet_filter,
                                non_unique_query_list,
                                query_list,
                                not_query_list,
                                phrase_query,
                                block_score,
                                all_terms_frequent,
                            );

                            query_list[t1].docid = doc_id + 1;

                            for item in query_list.iter_mut().skip(1) {
                                if item.compression_type == CompressionType::Array {
                                    item.p_docid += 1;
                                    if item.p_docid == item.p_docid_count {
                                        break 'exit;
                                    }
                                } else if (item.compression_type == CompressionType::Rle)
                                    && (doc_id == item.run_end)
                                {
                                    item.p_run += 1;
                                    if item.p_run == item.p_run_count {
                                        break 'exit;
                                    }
                                    item.p_run_sum += read_u16(
                                        item.byte_array,
                                        item.compressed_doc_id_range
                                            + 4
                                            + (item.p_run << 2) as usize,
                                    ) as i32;
                                }
                            }

                            t2 = 1;
                            if query_list[t2].compression_type != CompressionType::Rle {
                                query_list[t1].docid = doc_id + 1;
                                continue 'restart;
                            }

                            if (doc_id == query_list[t2].run_end) || (query_list.len() > 2) {
                                ushorts2 = cast_byte_ushort_slice(
                                    &query_list[t2].byte_array
                                        [query_list[t2].compressed_doc_id_range..],
                                );
                                runstart2 = ushorts2[(1 + query_list[t2].p_run * 2) as usize];
                                runlength2 = ushorts2[(2 + query_list[t2].p_run * 2) as usize];
                                runend2 = runstart2 + runlength2;
                                query_list[t2].run_end = runend2 as i32;
                                break;
                            }
                        }

                        if query_list[t1].docid - 1 == query_list[t1].run_end {
                            query_list[t1].p_run += 1;
                            if query_list[t1].p_run == query_list[t1].p_run_count {
                                break 'exit;
                            }
                            runstart1 = ushorts1[(1 + query_list[t1].p_run * 2) as usize];
                            runlength1 = ushorts1[(2 + query_list[t1].p_run * 2) as usize];
                            runend1 = runstart1 + runlength1;
                            query_list[t1].p_run_sum += runlength1 as i32;
                            query_list[t1].run_end = runend1 as i32;
                        }
                    }
                }

                break;
            },

            (CompressionType::Rle, CompressionType::Bitmap) => {
                'exit: loop {
                    let ushorts1 = cast_byte_ushort_slice(
                        &query_list[t1].byte_array[query_list[t1].compressed_doc_id_range..],
                    );
                    let mut ulongs2 = cast_byte_ulong_slice(
                        &query_list[t2].byte_array[query_list[t2].compressed_doc_id_range..],
                    );

                    query_list[t1].p_run_count = ushorts1[0] as i32;

                    loop {
                        let mut runstart1 = ushorts1[1 + (query_list[t1].p_run * 2) as usize];
                        let runlength1 = ushorts1[2 + (query_list[t1].p_run * 2) as usize];
                        let runend1 = runstart1 + runlength1;
                        query_list[t1].run_end = runend1 as i32;

                        runstart1 = cmp::max(runstart1, query_list[t1].docid as u16);
                        let mut intersect_mask: u64 = if (query_list[t1].docid as u16) < runstart1 {
                            u64::MAX
                        } else {
                            u64::MAX << (query_list[t1].docid & 63)
                        };

                        let byte_pos_start = runstart1 >> 6;
                        let byte_pos_end = runend1 >> 6;

                        for ulong_pos in byte_pos_start..=byte_pos_end {
                            let mut intersect: u64 = ulongs2[ulong_pos as usize] & intersect_mask;

                            if ulong_pos == byte_pos_start {
                                intersect &= u64::MAX << (runstart1 & 63);
                            }
                            if ulong_pos == byte_pos_end {
                                intersect &= u64::MAX >> (63 - (runend1 & 63));
                            }

                            while intersect != 0 {
                                let bit_pos = unsafe { _mm_tzcnt_64(intersect) };
                                let doc_id = ((ulong_pos as u32) << 6) + bit_pos as u32;

                                if (query_list[t1].docid as u32 != doc_id) && (t2 > 1) {
                                    t2 = 1;
                                    query_list[t1].docid = doc_id as i32;
                                    continue 'restart;
                                }
                                query_list[t1].docid = doc_id as i32;

                                if t2 + 1 < query_list.len() {
                                    for item in ulongs2
                                        .iter()
                                        .take(ulong_pos as usize)
                                        .skip(query_list[t2].p_run as usize)
                                    {
                                        query_list[t2].p_run_sum += item.count_ones() as i32
                                    }
                                    query_list[t2].p_docid = if bit_pos == 0 {
                                        query_list[t2].p_run_sum as usize
                                    } else {
                                        query_list[t2].p_run_sum as usize
                                            + (ulongs2[ulong_pos as usize] << (64 - bit_pos))
                                                .count_ones()
                                                as usize
                                    };
                                    query_list[t2].p_run = ulong_pos as i32;

                                    t2 += 1;
                                    if query_list[t2].compression_type != CompressionType::Bitmap {
                                        query_list[t1].docid = doc_id as i32;
                                        continue 'restart;
                                    } else {
                                        ulongs2 = cast_byte_ulong_slice(
                                            &query_list[t2].byte_array
                                                [query_list[t2].compressed_doc_id_range..],
                                        );
                                        intersect &= ulongs2[ulong_pos as usize];
                                        continue;
                                    }
                                }

                                intersect = unsafe { _blsr_u64(intersect) };

                                query_list[t1].p_docid = query_list[t1].p_run_sum as usize
                                    - runlength1 as usize
                                    + doc_id as usize
                                    - ushorts1[1 + (query_list[t1].p_run * 2) as usize] as usize
                                    + query_list[t1].p_run as usize;

                                for item in ulongs2
                                    .iter()
                                    .take(ulong_pos as usize)
                                    .skip(query_list[t2].p_run as usize)
                                {
                                    query_list[t2].p_run_sum += item.count_ones() as i32
                                }
                                query_list[t2].p_docid = if bit_pos == 0 {
                                    query_list[t2].p_run_sum as usize
                                } else {
                                    query_list[t2].p_run_sum as usize
                                        + (ulongs2[ulong_pos as usize] << (64 - bit_pos))
                                            .count_ones()
                                            as usize
                                };
                                query_list[t2].p_run = ulong_pos as i32;

                                add_result_multiterm_multifield(
                                    index,
                                    (block_id << 16) | doc_id as usize,
                                    result_count,
                                    search_result,
                                    top_k,
                                    result_type,
                                    field_filter_set,
                                    facet_filter,
                                    non_unique_query_list,
                                    query_list,
                                    not_query_list,
                                    phrase_query,
                                    block_score,
                                    all_terms_frequent,
                                );
                                query_list[t1].docid = doc_id as i32 + 1;

                                for item in query_list.iter_mut().skip(1) {
                                    if item.compression_type == CompressionType::Array {
                                        item.p_docid += 1;
                                        if item.p_docid == item.p_docid_count {
                                            break 'exit;
                                        };
                                    } else if (item.compression_type == CompressionType::Rle)
                                        && (doc_id == item.run_end as u32)
                                    {
                                        item.p_run = 1;
                                        if item.p_run == item.p_run_count {
                                            break 'exit;
                                        };

                                        item.p_run_sum += read_u16(
                                            item.byte_array,
                                            item.compressed_doc_id_range
                                                + 4
                                                + (item.p_run << 2) as usize,
                                        )
                                            as i32;
                                    }
                                }

                                t2 = 1;
                                if query_list[t2].compression_type != CompressionType::Bitmap {
                                    // !!! bevor er restart macht muss er docid1 erhöhen und evtl schleife mit exit beenden
                                    if doc_id == query_list[t1].run_end as u32 {
                                        query_list[t1].p_run += 1;
                                        if query_list[t1].p_run == query_list[t1].p_run_count {
                                            break 'exit;
                                        }
                                        query_list[t1].p_run_sum += ushorts1
                                            [2 + (query_list[t1].p_run * 2) as usize]
                                            as i32;
                                    }
                                    query_list[t1].docid = doc_id as i32 + 1;
                                    continue 'restart;
                                }
                                ulongs2 = cast_byte_ulong_slice(
                                    &query_list[t2].byte_array
                                        [query_list[t2].compressed_doc_id_range..],
                                );
                                intersect &= ulongs2[ulong_pos as usize];

                                intersect_mask = u64::MAX;
                            }

                            t2 = 1;
                            if query_list[t2].compression_type != CompressionType::Bitmap {
                                query_list[t1].docid =
                                    cmp::min(((ulong_pos + 1) << 6) as i32, runend1 as i32 + 1);

                                continue 'restart;
                            }
                            ulongs2 = cast_byte_ulong_slice(
                                &query_list[t2].byte_array
                                    [query_list[t2].compressed_doc_id_range..],
                            );
                        }

                        query_list[t1].p_run += 1;
                        if query_list[t1].p_run == query_list[t1].p_run_count {
                            break 'exit;
                        } // !!!!!!! wird p_docid_count nicht überall gesetzt
                        query_list[t1].p_run_sum +=
                            ushorts1[2 + (query_list[t1].p_run * 2) as usize] as i32;
                    }
                }
            }

            (CompressionType::Rle, CompressionType::Array) => 'exit: loop {
                let ushorts1 = cast_byte_ushort_slice(
                    &query_list[t1].byte_array[query_list[t1].compressed_doc_id_range..],
                );
                query_list[t1].p_run_count = ushorts1[0] as i32;
                let mut runstart1 = ushorts1[(1 + query_list[t1].p_run * 2) as usize];
                let mut runlength1 = ushorts1[(2 + query_list[t1].p_run * 2) as usize];
                let mut runend1 = runstart1 + runlength1;
                query_list[t1].run_end = runend1 as i32;

                runstart1 = cmp::max(runstart1, query_list[t1].docid as u16);

                let mut ushorts2 = cast_byte_ushort_slice(
                    &query_list[t2].byte_array[query_list[t2].compressed_doc_id_range..],
                );
                let mut doc_id2 = ushorts2[query_list[t2].p_docid];

                loop {
                    if doc_id2 > runend1 {
                        if t2 > 1 {
                            t2 = 1;
                            if query_list[t2].compression_type != CompressionType::Array {
                                query_list[t1].p_run += 1;
                                if query_list[t1].p_run == query_list[t1].p_run_count {
                                    break;
                                }
                                query_list[t1].p_run_sum += read_u16(
                                    query_list[t1].byte_array,
                                    query_list[t1].compressed_doc_id_range
                                        + 4
                                        + (query_list[t1].p_run << 2) as usize,
                                )
                                    as i32;

                                continue 'restart;
                            } else {
                                ushorts2 = cast_byte_ushort_slice(
                                    &query_list[t2].byte_array
                                        [query_list[t2].compressed_doc_id_range..],
                                );
                                doc_id2 = ushorts2[query_list[t2].p_docid];
                            }
                        }

                        query_list[t1].p_run += 1;
                        if query_list[t1].p_run == query_list[t1].p_run_count {
                            break;
                        }

                        runstart1 = ushorts1[(1 + query_list[t1].p_run * 2) as usize];
                        runlength1 = ushorts1[(2 + query_list[t1].p_run * 2) as usize];
                        runend1 = runstart1 + runlength1;
                        query_list[t1].p_run_sum += runlength1 as i32;
                        query_list[t1].run_end = runend1 as i32;
                    } else if doc_id2 < runstart1 {
                        query_list[t2].p_docid += 1;
                        if query_list[t2].p_docid == query_list[t2].p_docid_count {
                            break;
                        }

                        if true {
                            let mut bound = 2;
                            while (query_list[t2].p_docid + bound < query_list[t2].p_docid_count)
                                && (ushorts2[query_list[t2].p_docid + bound] < runstart1)
                            {
                                query_list[t2].p_docid += bound;
                                bound <<= 1;
                            }
                        }

                        doc_id2 = ushorts2[query_list[t2].p_docid];
                    } else {
                        if t2 + 1 < query_list.len() {
                            t2 += 1;
                            if query_list[t2].compression_type != CompressionType::Array {
                                query_list[t1].docid = doc_id2 as i32;
                                continue 'restart;
                            } else {
                                ushorts2 = cast_byte_ushort_slice(
                                    &query_list[t2].byte_array
                                        [query_list[t2].compressed_doc_id_range..],
                                );
                                doc_id2 = ushorts2[query_list[t2].p_docid];
                                continue;
                            }
                        }

                        query_list[t1].p_docid = query_list[t1].p_run_sum as usize
                            - runlength1 as usize
                            + doc_id2 as usize
                            - runstart1 as usize
                            + query_list[t1].p_run as usize;
                        add_result_multiterm_multifield(
                            index,
                            (block_id << 16) | doc_id2 as usize,
                            result_count,
                            search_result,
                            top_k,
                            result_type,
                            field_filter_set,
                            facet_filter,
                            non_unique_query_list,
                            query_list,
                            not_query_list,
                            phrase_query,
                            block_score,
                            all_terms_frequent,
                        );

                        for item in query_list.iter_mut().skip(1) {
                            if item.compression_type == CompressionType::Array {
                                item.p_docid += 1;
                                if item.p_docid == item.p_docid_count {
                                    break 'exit;
                                }
                            } else if (item.compression_type == CompressionType::Rle)
                                && (doc_id2 == item.run_end as u16)
                            {
                                item.p_run += 1;
                                if item.p_run == item.p_run_count {
                                    break 'exit;
                                }
                                item.p_run_sum += read_u16(
                                    item.byte_array,
                                    item.compressed_doc_id_range + 4 + (item.p_run << 2) as usize,
                                ) as i32;
                            }
                        }

                        t2 = 1;
                        if query_list[t2].compression_type != CompressionType::Array {
                            query_list[t1].docid = doc_id2 as i32 + 1;

                            continue 'restart;
                        }

                        if doc_id2 == query_list[t1].run_end as u16 {
                            query_list[t1].p_run += 1;
                            if query_list[t1].p_run == query_list[t1].p_run_count {
                                break 'exit;
                            }
                            runstart1 = ushorts1[(1 + query_list[t1].p_run * 2) as usize];
                            runlength1 = ushorts1[(2 + query_list[t1].p_run * 2) as usize];
                            runend1 = runstart1 + runlength1;
                            query_list[t1].p_run_sum += runlength1 as i32;
                            query_list[t1].run_end = runend1 as i32;
                        }
                        doc_id2 = ushorts2[query_list[t2].p_docid];
                    }
                }

                break;
            },

            (CompressionType::Bitmap, CompressionType::Array) => {}

            (CompressionType::Bitmap, CompressionType::Rle) => {
                'exit: loop {
                    let ulongs1 = cast_byte_ulong_slice(
                        &query_list[t1].byte_array[query_list[t1].compressed_doc_id_range..],
                    );
                    let mut ushorts2 = cast_byte_ushort_slice(
                        &query_list[t2].byte_array[query_list[t2].compressed_doc_id_range..],
                    );
                    query_list[t2].p_docid_count = ushorts2[0] as usize;

                    loop {
                        let mut run_start2 = ushorts2[1 + (query_list[t2].p_docid * 2)];
                        let run_length2 = ushorts2[2 + (query_list[t2].p_docid * 2)];
                        let run_end2 = run_start2 + run_length2;
                        query_list[t2].run_end = run_end2 as i32;

                        run_start2 = cmp::max(run_start2, query_list[t1].docid as u16);
                        let mut intersect_mask: u64 = if (query_list[t1].docid as u16) < run_start2
                        {
                            u64::MAX
                        } else {
                            u64::MAX << (query_list[t1].docid & 63)
                        };

                        let byte_pos_start = run_start2 >> 6;
                        let byte_pos_end = run_end2 >> 6;

                        for ulong_pos in byte_pos_start..=byte_pos_end {
                            let mut intersect: u64 = ulongs1[ulong_pos as usize] & intersect_mask;

                            if ulong_pos == byte_pos_start {
                                intersect &= u64::MAX << (run_start2 & 63);
                            }
                            if ulong_pos == byte_pos_end {
                                intersect &= u64::MAX >> (63 - (run_end2 & 63));
                            }

                            while intersect != 0 {
                                let bit_pos = unsafe { _mm_tzcnt_64(intersect) } as u16;
                                let doc_id = ((ulong_pos as u32) << 6) + bit_pos as u32;

                                if t2 + 1 < query_list.len() {
                                    query_list[t2].p_docid = query_list[t2].p_run_sum as usize
                                        - run_length2 as usize
                                        + doc_id as usize
                                        - run_start2 as usize
                                        + query_list[t2].p_run as usize;

                                    t2 += 1;
                                    if query_list[t2].compression_type != CompressionType::Rle {
                                        query_list[t1].docid = doc_id as i32;
                                        continue 'restart;
                                    } else {
                                        continue;
                                    }
                                }

                                intersect = unsafe { _blsr_u64(intersect) };
                                query_list[t2].p_docid = query_list[t2].p_run_sum as usize
                                    - run_length2 as usize
                                    + doc_id as usize
                                    - run_start2 as usize
                                    + query_list[t2].p_run as usize;
                                add_result_multiterm_multifield(
                                    index,
                                    (block_id << 16) | doc_id as usize,
                                    result_count,
                                    search_result,
                                    top_k,
                                    result_type,
                                    field_filter_set,
                                    facet_filter,
                                    non_unique_query_list,
                                    query_list,
                                    not_query_list,
                                    phrase_query,
                                    block_score,
                                    all_terms_frequent,
                                );

                                for item in query_list.iter_mut().skip(1) {
                                    if item.compression_type != CompressionType::Rle {
                                        if item.compression_type != CompressionType::Bitmap {
                                            item.p_docid += 1;
                                            if item.p_docid == item.p_docid_count {
                                                break 'exit;
                                            }
                                        }
                                    } else if doc_id == item.run_end as u32 {
                                        item.p_run += 1;
                                        if item.p_run == item.p_run_count {
                                            break 'exit;
                                        }
                                        item.p_run_sum += read_u16(
                                            item.byte_array,
                                            item.compressed_doc_id_range
                                                + 4
                                                + (item.p_run << 2) as usize,
                                        )
                                            as i32;
                                    }
                                }

                                t2 = 1;
                                if query_list[t2].compression_type != CompressionType::Rle {
                                    query_list[t1].docid = doc_id as i32 + 1;
                                    continue 'restart;
                                }
                                intersect &= ulongs1[ulong_pos as usize];

                                intersect_mask = u64::MAX;
                            }

                            t2 = 1;
                            if query_list[t2].compression_type != CompressionType::Rle {
                                query_list[t1].docid = ((ulong_pos + 1) as i32) << 6;
                                continue 'restart;
                            }
                            ushorts2 = cast_byte_ushort_slice(
                                &query_list[t2].byte_array
                                    [query_list[t2].compressed_doc_id_range..],
                            );
                        }

                        query_list[t2].p_docid += 1;

                        if query_list[t2].p_docid == query_list[t2].p_docid_count {
                            break 'exit;
                        } // !!!!!!! wird p_docid_count nicht überall gesetzt?
                    }
                }
            }

            _ => {
                println!("forbidden compression combination:  block: {}  t1: {} {} {} {:?}   t2: {} {} {} {:?} {} ",  block_id , 
                t1, query_list[t1].term , query_list[t1].blocks[query_list[t1].p_block as usize].posting_count , query_list[t1].compression_type,
                t2, query_list[t2].term , query_list[t2].blocks[query_list[t2].p_block as usize].posting_count , query_list[t2].compression_type , query_list.len());
            }
        }

        break;
    }
}

pub(crate) struct BlockObject {
    pub block_id: usize,
    pub block_score: f32,
    pub p_block_vec: Vec<i32>,
}

/// Intersection between blocks of 64k docids of a posting list
#[allow(clippy::too_many_arguments)]
pub(crate) async fn intersection_blockid<'a>(
    index: &'a Index,
    non_unique_query_list: &mut Vec<NonUniquePostingListObjectQuery<'a>>,
    query_list: &mut Vec<PostingListObjectQuery<'a>>,
    not_query_list: &mut [PostingListObjectQuery<'a>],
    result_count_arc: &Arc<AtomicUsize>,
    search_result: &mut SearchResult,
    top_k: usize,
    result_type: &ResultType,
    field_filter_set: &AHashSet<u16>,
    facet_filter: &[FilterSparse],
    matching_blocks: &mut i32,
    phrase_query: bool,
) {
    let item_0 = &query_list[0];
    let enable_inter_query_threading_multi =
        if !index.enable_search_quality_test && index.enable_inter_query_threading_auto {
            item_0.posting_count / item_0.p_block_max as u32 > 10
        } else {
            index.enable_inter_query_threading
        };

    let mut task_list = Vec::new();

    let t1: i32 = 0;
    let mut t2: i32 = 1;

    let item_1 = &query_list[t1 as usize];
    let item_2 = &query_list[t2 as usize];
    let mut block_id1 = item_1.blocks[item_1.p_block as usize].block_id;
    let mut block_id2 = item_2.blocks[item_2.p_block as usize].block_id;

    let mut block_vec: Vec<BlockObject> = Vec::new();

    'exit: loop {
        match block_id1.cmp(&block_id2) {
            std::cmp::Ordering::Less => {
                let item_1 = &mut query_list[t1 as usize];
                item_1.p_block += 1;
                if item_1.p_block == item_1.p_block_max {
                    break;
                }
                block_id1 = item_1.blocks[item_1.p_block as usize].block_id;

                t2 = 1;
                let item_2 = &query_list[t2 as usize];
                block_id2 = item_2.blocks[item_2.p_block as usize].block_id;
            }
            std::cmp::Ordering::Greater => {
                let item_2 = &mut query_list[t2 as usize];
                item_2.p_block += 1;
                if item_2.p_block == item_2.p_block_max {
                    break;
                }
                block_id2 = item_2.blocks[item_2.p_block as usize].block_id;
            }
            std::cmp::Ordering::Equal => {
                if t2 + 1 < query_list.len() as i32 {
                    t2 += 1;
                    let item_2 = &query_list[t2 as usize];
                    block_id2 = item_2.blocks[item_2.p_block as usize].block_id;

                    continue;
                }

                if !enable_inter_query_threading_multi {
                    let mut block_score = 0.0;
                    if SPEEDUP_FLAG && result_type != &ResultType::Count {
                        for query_list_item_mut in query_list.iter_mut() {
                            block_score += query_list_item_mut.blocks
                                [query_list_item_mut.p_block as usize]
                                .max_block_score;
                        }
                    }

                    if SPEEDUP_FLAG && SORT_FLAG && result_type != &ResultType::Count {
                        let mut p_block_vec: Vec<i32> = vec![0; 10];
                        for i in 0..query_list.len() {
                            p_block_vec[query_list[i].term_index_unique] = query_list[i].p_block
                        }
                        let block_object = BlockObject {
                            block_id: block_id1 as usize,
                            block_score,
                            p_block_vec,
                        };
                        block_vec.push(block_object);
                    } else if !SPEEDUP_FLAG
                        || result_type == &ResultType::Count
                        || search_result.topk_candidates.current_heap_size < top_k
                        || block_score > search_result.topk_candidates._elements[0].score
                    {
                        if index.meta.access_type == AccessType::Mmap {
                            for query_list_item_mut in query_list.iter_mut() {
                                let segment =
                                    &index.segments_index[query_list_item_mut.key0 as usize];
                                query_list_item_mut.byte_array = &index.index_file_mmap[segment
                                    .byte_array_blocks_pointer
                                    [block_id1 as usize]
                                    .0
                                    ..segment.byte_array_blocks_pointer[block_id1 as usize].0
                                        + segment.byte_array_blocks_pointer[block_id1 as usize].1];
                            }
                            for nonunique_query_list_item_mut in non_unique_query_list.iter_mut() {
                                let segment = &index.segments_index
                                    [nonunique_query_list_item_mut.key0 as usize];
                                nonunique_query_list_item_mut.byte_array = &index.index_file_mmap
                                    [segment.byte_array_blocks_pointer[block_id1 as usize].0
                                        ..segment.byte_array_blocks_pointer[block_id1 as usize].0
                                            + segment.byte_array_blocks_pointer
                                                [block_id1 as usize]
                                                .1];
                            }
                            for not_query_list_item_mut in not_query_list.iter_mut() {
                                let segment =
                                    &index.segments_index[not_query_list_item_mut.key0 as usize];
                                not_query_list_item_mut.byte_array = &index.index_file_mmap[segment
                                    .byte_array_blocks_pointer
                                    [block_id1 as usize]
                                    .0
                                    ..segment.byte_array_blocks_pointer[block_id1 as usize].0
                                        + segment.byte_array_blocks_pointer[block_id1 as usize].1];
                            }
                        } else {
                            for query_list_item_mut in query_list.iter_mut() {
                                query_list_item_mut.byte_array = &index.segments_index
                                    [query_list_item_mut.key0 as usize]
                                    .byte_array_blocks[block_id1 as usize];
                            }
                            for nonunique_query_list_item_mut in non_unique_query_list.iter_mut() {
                                nonunique_query_list_item_mut.byte_array = &index.segments_index
                                    [nonunique_query_list_item_mut.key0 as usize]
                                    .byte_array_blocks[block_id1 as usize];
                            }
                            for not_query_list_item_mut in not_query_list.iter_mut() {
                                not_query_list_item_mut.byte_array = &index.segments_index
                                    [not_query_list_item_mut.key0 as usize]
                                    .byte_array_blocks[block_id1 as usize];
                            }
                        }

                        let mut result_count_local = 0;
                        intersection_docid(
                            index,
                            non_unique_query_list,
                            query_list,
                            not_query_list,
                            block_id1 as usize,
                            &mut result_count_local,
                            search_result,
                            top_k,
                            result_type,
                            field_filter_set,
                            facet_filter,
                            phrase_query,
                            block_score,
                        )
                        .await;

                        result_count_arc.fetch_add(result_count_local as usize, Ordering::Relaxed);
                    }
                } else {
                    let mut query_list_copy: Vec<PostingListObjectQuery> = Vec::new();
                    let mut non_unique_query_list_copy: Vec<NonUniquePostingListObjectQuery> =
                        Vec::new();

                    for x in &mut *query_list {
                        query_list_copy.push(x.clone());
                    }

                    for x in &mut *non_unique_query_list {
                        let y = x.clone();
                        non_unique_query_list_copy.push(y);
                    }

                    let result_count_clone = result_count_arc.clone();
                    task_list.push(tokio::spawn(async move {
                        let result_count_local = 1;
                        result_count_clone.fetch_add(result_count_local, Ordering::Relaxed);
                    }));
                }

                *matching_blocks += 1;

                t2 = 1;

                for item in query_list.iter_mut() {
                    item.p_block += 1;
                    if item.p_block == item.p_block_max {
                        break 'exit;
                    }
                }

                let item_1 = &query_list[t1 as usize];
                let item_2 = &query_list[t2 as usize];
                block_id1 = item_1.blocks[item_1.p_block as usize].block_id;
                block_id2 = item_2.blocks[item_2.p_block as usize].block_id;
            }
        }
    }

    if SORT_FLAG && SPEEDUP_FLAG && (result_type != &ResultType::Count) {
        block_vec.sort_unstable_by(|x, y| y.block_score.partial_cmp(&x.block_score).unwrap());
        for block in block_vec {
            if (result_type == &ResultType::Topk)
                && (search_result.topk_candidates.current_heap_size == top_k)
                && (block.block_score <= search_result.topk_candidates._elements[0].score)
            {
                break;
            }

            for item in query_list.iter_mut() {
                item.p_block = block.p_block_vec[item.term_index_unique];
            }

            if index.meta.access_type == AccessType::Mmap {
                for query_list_item_mut in query_list.iter_mut() {
                    let segment = &index.segments_index[query_list_item_mut.key0 as usize];
                    query_list_item_mut.byte_array =
                        &index.index_file_mmap[segment.byte_array_blocks_pointer[block.block_id].0
                            ..segment.byte_array_blocks_pointer[block.block_id].0
                                + segment.byte_array_blocks_pointer[block.block_id].1];
                }
                for nonunique_query_list_item_mut in non_unique_query_list.iter_mut() {
                    let segment =
                        &index.segments_index[nonunique_query_list_item_mut.key0 as usize];
                    nonunique_query_list_item_mut.byte_array =
                        &index.index_file_mmap[segment.byte_array_blocks_pointer[block.block_id].0
                            ..segment.byte_array_blocks_pointer[block.block_id].0
                                + segment.byte_array_blocks_pointer[block.block_id].1];
                }
                for not_query_list_item_mut in not_query_list.iter_mut() {
                    let segment = &index.segments_index[not_query_list_item_mut.key0 as usize];
                    not_query_list_item_mut.byte_array =
                        &index.index_file_mmap[segment.byte_array_blocks_pointer[block.block_id].0
                            ..segment.byte_array_blocks_pointer[block.block_id].0
                                + segment.byte_array_blocks_pointer[block.block_id].1];
                }
            } else {
                for query_list_item_mut in query_list.iter_mut() {
                    query_list_item_mut.byte_array = &index.segments_index
                        [query_list_item_mut.key0 as usize]
                        .byte_array_blocks[block.block_id];
                }
                for nonunique_query_list_item_mut in non_unique_query_list.iter_mut() {
                    nonunique_query_list_item_mut.byte_array = &index.segments_index
                        [nonunique_query_list_item_mut.key0 as usize]
                        .byte_array_blocks[block.block_id];
                }
                for not_query_list_item_mut in not_query_list.iter_mut() {
                    not_query_list_item_mut.byte_array = &index.segments_index
                        [not_query_list_item_mut.key0 as usize]
                        .byte_array_blocks[block.block_id];
                }
            }

            let mut result_count_local = 0;
            intersection_docid(
                index,
                non_unique_query_list,
                query_list,
                not_query_list,
                block.block_id,
                &mut result_count_local,
                search_result,
                top_k,
                result_type,
                field_filter_set,
                facet_filter,
                phrase_query,
                block.block_score,
            )
            .await;

            result_count_arc.fetch_add(result_count_local as usize, Ordering::Relaxed);
        }
    }
}
