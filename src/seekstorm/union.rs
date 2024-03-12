use crate::{
    add_result::{
        add_result_multiterm_multifield, add_result_singleterm_multifield, PostingListObjectSingle,
    },
    index::{
        AccessType, CompressionType, Index, NonUniquePostingListObjectQuery,
        PostingListObjectQuery, QueueObject,
    },
    intersection::intersection_blockid,
    min_heap::MinHeap,
    search::ResultType,
    single::{single_blockid, single_docid},
    utils::{block_copy, cast_byte_ulong_slice, cast_byte_ushort_slice, read_u16, read_u64},
};

use ahash::AHashSet;
use num_traits::FromPrimitive;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::{
    arch::x86_64::{_blsr_u64, _mm_tzcnt_64},
    sync::Arc,
};

use async_recursion::async_recursion;

pub(crate) fn get_next_docid(plo: &mut PostingListObjectQuery) {
    unsafe {
        if plo.compression_type == CompressionType::Array {
            plo.docid = read_u16(
                plo.byte_array,
                plo.compressed_doc_id_range + (plo.p_docid << 1),
            ) as i32
        } else if plo.compression_type == CompressionType::Bitmap {
            while plo.intersect == 0 {
                plo.intersect = read_u64(
                    plo.byte_array,
                    plo.compressed_doc_id_range + (plo.ulong_pos << 3),
                );
                plo.ulong_pos += 1;
            }
            let bit_pos = _mm_tzcnt_64(plo.intersect);
            plo.intersect = _blsr_u64(plo.intersect);
            plo.docid = (((plo.ulong_pos - 1) << 6) + bit_pos as usize) as i32
        } else if plo.docid < plo.run_end {
            plo.docid += 1;
        } else {
            plo.p_run += 4;
            let startdocid = read_u16(
                plo.byte_array,
                plo.compressed_doc_id_range + plo.p_run as usize,
            );
            let runlength = read_u16(
                plo.byte_array,
                plo.compressed_doc_id_range + (plo.p_run + 2) as usize,
            );
            plo.run_end = (startdocid + runlength) as i32;
            plo.docid = startdocid as i32;
        };
    }
}

/// Horizontal scan algorithm
/// scan for minimal docid, increase p_docid for terms with minimal docid
/// allows unlimited number of terms
#[allow(clippy::too_many_arguments)]
pub(crate) fn union_scan(
    result_count: &mut i32,
    block_id: usize,
    index: &Index,
    result_candidates: &mut MinHeap,
    top_k: usize,
    result_type: &ResultType,
    field_filter_set: &AHashSet<u16>,
    non_unique_query_list: &mut [NonUniquePostingListObjectQuery],
    query_list: &mut [PostingListObjectQuery],
    not_query_list: &mut [PostingListObjectQuery],
) {
    for plo in query_list.iter_mut() {
        if !plo.end_flag {
            get_next_docid(plo);
        }
    }

    loop {
        let mut break_loop = true;
        let mut docid_min = u16::MAX;

        for plo in query_list.iter_mut() {
            if !plo.end_flag && (plo.docid as u16) < docid_min {
                docid_min = plo.docid as u16;
            }
        }

        if result_type != &ResultType::Count {
            let mut term_match_count = 0;
            let mut term_index = 0;
            for (i, plo) in query_list.iter_mut().enumerate() {
                if !plo.end_flag && (plo.docid as u16 == docid_min) {
                    plo.bm25_flag = true;
                    term_match_count += 1;
                    term_index = i;
                } else {
                    plo.bm25_flag = false;
                }
            }

            if term_match_count == 1 {
                let query_list_item_mut = &mut query_list[term_index];
                let plo = PostingListObjectSingle {
                    rank_position_pointer_range: query_list_item_mut.rank_position_pointer_range,
                    pointer_pivot_p_docid: query_list_item_mut.pointer_pivot_p_docid,
                    byte_array: query_list_item_mut.byte_array,
                    p_docid: query_list_item_mut.p_docid as i32,
                    idf: query_list_item_mut.idf,
                    idf_bigram1: query_list_item_mut.idf_bigram1,
                    idf_bigram2: query_list_item_mut.idf_bigram2,
                    is_bigram: query_list_item_mut.is_bigram,
                };

                add_result_singleterm_multifield(
                    index,
                    (block_id << 16) | docid_min as usize,
                    result_count,
                    result_candidates,
                    top_k,
                    result_type,
                    field_filter_set,
                    &plo,
                    not_query_list,
                    query_list_item_mut.blocks[query_list_item_mut.p_block as usize]
                        .max_block_score,
                );
                if not_query_list.is_empty() && result_type != &ResultType::Topk {
                    *result_count += 1;
                }
            } else {
                add_result_multiterm_multifield(
                    index,
                    (block_id << 16) | docid_min as usize,
                    result_count,
                    result_candidates,
                    top_k,
                    result_type,
                    field_filter_set,
                    non_unique_query_list,
                    query_list,
                    not_query_list,
                    false,
                    f32::MAX,
                    false,
                );
            }
        } else {
            *result_count += 1;
        }

        for plo in query_list.iter_mut() {
            if !plo.end_flag {
                let doc_id = plo.docid as u16;
                if doc_id == docid_min {
                    if plo.p_docid < plo.p_docid_count - 1 {
                        plo.p_docid += 1;
                        get_next_docid(plo);
                        break_loop = false; // !!!! auch bei denen docid > docid_min
                    } else {
                        plo.end_flag = true;
                    }
                } else {
                    break_loop = false;
                }
            }
        }

        if break_loop {
            break;
        }
    }
}

/// Union for a single block
#[allow(clippy::too_many_arguments)]
pub(crate) async fn union_docid<'a>(
    index: &'a Index,
    non_unique_query_list: &mut [NonUniquePostingListObjectQuery<'a>],
    query_list: &mut Vec<PostingListObjectQuery<'a>>,
    not_query_list: &mut [PostingListObjectQuery<'a>],
    block_id: usize,
    result_count: &mut i32,
    topk_candidates: &mut MinHeap,
    top_k: usize,
    result_type: &ResultType,
    field_filter_set: &AHashSet<u16>,
) {
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
        query_list_item_mut.p_docid_count =
            query_list_item_mut.blocks[query_list_item_mut.p_block as usize].posting_count as usize
                + 1;

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

    let mut valid_term_count = 0;
    let mut term_index = 0;
    let mut single_term_index = 0;

    for query_list_item_mut in query_list.iter_mut() {
        query_list_item_mut.end_flag = query_list_item_mut.end_flag_block
            || (query_list_item_mut.blocks[query_list_item_mut.p_block as usize].block_id
                != block_id as u32);

        if query_list_item_mut.end_flag {
            term_index += 1;
            continue;
        }

        valid_term_count += 1;
        single_term_index = term_index;
        term_index += 1;

        query_list_item_mut.p_docid = 0;
        query_list_item_mut.p_docid_count =
            query_list_item_mut.blocks[query_list_item_mut.p_block as usize].posting_count as usize
                + 1;

        query_list_item_mut.compression_type = FromPrimitive::from_u8(
            (query_list_item_mut.blocks[query_list_item_mut.p_block as usize]
                .compression_type_pointer
                >> 30) as u8,
        )
        .unwrap();

        query_list_item_mut.rank_position_pointer_range = query_list_item_mut.blocks
            [query_list_item_mut.p_block as usize]
            .compression_type_pointer
            & 0b0011_1111_1111_1111_1111_1111_1111_1111;

        query_list_item_mut.pointer_pivot_p_docid =
            query_list_item_mut.blocks[query_list_item_mut.p_block as usize].pointer_pivot_p_docid;

        let posting_pointer_size_sum = query_list_item_mut.blocks
            [query_list_item_mut.p_block as usize]
            .pointer_pivot_p_docid as usize
            * 2
            + if (query_list_item_mut.blocks[query_list_item_mut.p_block as usize]
                .pointer_pivot_p_docid as usize)
                <= query_list_item_mut.blocks[query_list_item_mut.p_block as usize].posting_count
                    as usize
            {
                ((query_list_item_mut.blocks[query_list_item_mut.p_block as usize].posting_count
                    as usize
                    + 1)
                    - query_list_item_mut.blocks[query_list_item_mut.p_block as usize]
                        .pointer_pivot_p_docid as usize)
                    * 3
            } else {
                0
            };
        query_list_item_mut.compressed_doc_id_range =
            query_list_item_mut.rank_position_pointer_range as usize + posting_pointer_size_sum;
        query_list_item_mut.docid = 0;
        query_list_item_mut.intersect = 0;
        query_list_item_mut.ulong_pos = 0;
        query_list_item_mut.p_run = -2;
        query_list_item_mut.run_end = 0;
    }

    if valid_term_count == 0 {
        return;
    }

    if valid_term_count == 1 {
        if result_type == &ResultType::Count {
            *result_count += query_list[single_term_index].p_docid_count as i32;
        } else {
            single_docid(
                index,
                query_list,
                not_query_list,
                &query_list[single_term_index].blocks
                    [query_list[single_term_index].p_block as usize],
                single_term_index,
                result_count,
                topk_candidates,
                top_k,
                result_type,
                field_filter_set,
            )
            .await;
        }
        return;
    };

    if result_type == &ResultType::Count {
        union_count(result_count, query_list, not_query_list).await;
        return;
    }

    union_scan(
        result_count,
        block_id,
        index,
        topk_candidates,
        top_k,
        result_type,
        field_filter_set,
        non_unique_query_list,
        query_list,
        not_query_list,
    );
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn union_blockid<'a>(
    index: &'a Index,
    non_unique_query_list: &mut Vec<NonUniquePostingListObjectQuery<'a>>,
    query_list: &mut Vec<PostingListObjectQuery<'a>>,
    not_query_list: &mut [PostingListObjectQuery<'a>],
    result_count_arc: &Arc<AtomicUsize>,
    topk_candidates: &mut MinHeap,
    top_k: usize,
    result_type: &ResultType,
    field_filter_set: &AHashSet<u16>,
) {
    let item_0 = &query_list[0];
    let enable_inter_query_threading_multi =
        if !index.enable_search_quality_test && index.enable_inter_query_threading_auto {
            item_0.posting_count / item_0.p_block_max as u32 > 10
        } else {
            index.enable_inter_query_threading
        };
    let mut task_list = Vec::new();

    loop {
        let mut break_loop = true;
        let mut block_id_min = usize::MAX;

        for plo in query_list.iter_mut() {
            if !plo.end_flag_block {
                let block_id = plo.blocks[plo.p_block as usize].block_id as usize;

                if block_id < block_id_min {
                    block_id_min = block_id;
                }
            }
        }

        if !enable_inter_query_threading_multi {
            if index.meta.access_type == AccessType::Mmap {
                for query_list_item_mut in query_list.iter_mut() {
                    let segment = &index.segments_index[query_list_item_mut.key0 as usize];
                    query_list_item_mut.byte_array =
                        &index.index_file_mmap[segment.byte_array_blocks_pointer[block_id_min].0
                            ..segment.byte_array_blocks_pointer[block_id_min].0
                                + segment.byte_array_blocks_pointer[block_id_min].1];
                }
                for nonunique_query_list_item_mut in non_unique_query_list.iter_mut() {
                    let segment =
                        &index.segments_index[nonunique_query_list_item_mut.key0 as usize];
                    nonunique_query_list_item_mut.byte_array =
                        &index.index_file_mmap[segment.byte_array_blocks_pointer[block_id_min].0
                            ..segment.byte_array_blocks_pointer[block_id_min].0
                                + segment.byte_array_blocks_pointer[block_id_min].1];
                }
                for not_query_list_item_mut in not_query_list.iter_mut() {
                    let segment = &index.segments_index[not_query_list_item_mut.key0 as usize];
                    not_query_list_item_mut.byte_array =
                        &index.index_file_mmap[segment.byte_array_blocks_pointer[block_id_min].0
                            ..segment.byte_array_blocks_pointer[block_id_min].0
                                + segment.byte_array_blocks_pointer[block_id_min].1];
                }
            } else {
                for query_list_item_mut in query_list.iter_mut() {
                    query_list_item_mut.byte_array = &index.segments_index
                        [query_list_item_mut.key0 as usize]
                        .byte_array_blocks[block_id_min];
                }
                for nonunique_query_list_item_mut in non_unique_query_list.iter_mut() {
                    nonunique_query_list_item_mut.byte_array = &index.segments_index
                        [nonunique_query_list_item_mut.key0 as usize]
                        .byte_array_blocks[block_id_min];
                }
                for not_query_list_item_mut in not_query_list.iter_mut() {
                    not_query_list_item_mut.byte_array = &index.segments_index
                        [not_query_list_item_mut.key0 as usize]
                        .byte_array_blocks[block_id_min];
                }
            }

            let mut result_count_local = 0;

            union_docid(
                index,
                non_unique_query_list,
                query_list,
                not_query_list,
                block_id_min,
                &mut result_count_local,
                topk_candidates,
                top_k,
                result_type,
                field_filter_set,
            )
            .await;

            result_count_arc.fetch_add(result_count_local as usize, Ordering::Relaxed);
        } else {
            let mut query_list_copy: Vec<PostingListObjectQuery> = Vec::new();
            let mut non_unique_query_list_copy: Vec<NonUniquePostingListObjectQuery> = Vec::new();

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

        for plo in query_list.iter_mut() {
            if !plo.end_flag_block {
                let block_id = plo.blocks[plo.p_block as usize].block_id as usize;
                if block_id == block_id_min {
                    if plo.p_block < plo.p_block_max - 1 {
                        plo.p_block += 1;
                        break_loop = false; // !!!! auch bei denen docid > docid_min
                    } else {
                        plo.end_flag_block = true;
                    }
                } else {
                    break_loop = false;
                }
            }
        }

        if break_loop {
            break;
        }
    }
}

pub(crate) async fn union_count<'a>(
    result_count: &mut i32,
    query_list: &mut [PostingListObjectQuery<'a>],
    not_query_list: &mut [PostingListObjectQuery<'a>],
) {
    query_list.sort_by(|a, b| b.p_docid_count.partial_cmp(&a.p_docid_count).unwrap());

    let mut result_count_local =
        query_list[0].blocks[query_list[0].p_block as usize].posting_count as u32 + 1;
    let mut bitmap_0: [u8; 8192] = [0u8; 8192];

    for (i, plo) in query_list.iter_mut().enumerate() {
        if plo.end_flag {
            continue;
        }

        if plo.compression_type == CompressionType::Bitmap {
            if i == 0 {
                block_copy(
                    plo.byte_array,
                    plo.compressed_doc_id_range,
                    &mut bitmap_0,
                    0,
                    8192,
                );
            } else {
                let ulongs0 = cast_byte_ulong_slice(&bitmap_0);
                let ulongs1 = cast_byte_ulong_slice(
                    &plo.byte_array
                        [plo.compressed_doc_id_range..plo.compressed_doc_id_range + 8192],
                );

                ulongs0.iter_mut().zip(ulongs1.iter()).for_each(|(x1, x2)| {
                    result_count_local += u64::count_ones(!*x1 & *x2);
                    *x1 |= *x2;
                });
            }
        } else if plo.compression_type == CompressionType::Array {
            let ushorts1 = cast_byte_ushort_slice(&plo.byte_array[plo.compressed_doc_id_range..]);

            if i == 0 {
                for item in ushorts1.iter().take(plo.p_docid_count) {
                    let docid = *item as usize;
                    let byte_index = docid >> 3;
                    let bit_index = docid & 7;

                    bitmap_0[byte_index] |= 1 << bit_index;
                }
            } else {
                for item in ushorts1.iter().take(plo.p_docid_count) {
                    let docid = *item as usize;
                    let byte_index = docid >> 3;
                    let bit_index = docid & 7;

                    if bitmap_0[byte_index] & (1 << bit_index) == 0 {
                        bitmap_0[byte_index] |= 1 << bit_index;
                        result_count_local += 1;
                    }
                }
            }
        } else {
            let ushorts1 = cast_byte_ushort_slice(&plo.byte_array[plo.compressed_doc_id_range..]);
            let runs_count = ushorts1[0] as i32;

            if i == 0 {
                for ii in (1..(runs_count << 1) + 1).step_by(2) {
                    let startdocid = ushorts1[ii as usize] as usize;
                    let runlength = ushorts1[(ii + 1) as usize] as usize;

                    for j in 0..=runlength {
                        let docid = (startdocid + j) as usize;
                        let byte_index = docid >> 3;
                        let bit_index = docid & 7;

                        bitmap_0[byte_index] |= 1 << bit_index;
                    }
                }
            } else {
                for ii in (1..(runs_count << 1) + 1).step_by(2) {
                    let startdocid = ushorts1[ii as usize] as usize;
                    let runlength = ushorts1[(ii + 1) as usize] as usize;

                    for j in 0..=runlength {
                        let docid = (startdocid + j) as usize;
                        let byte_index = docid >> 3;
                        let bit_index = docid & 7;

                        if bitmap_0[byte_index] & (1 << bit_index) == 0 {
                            bitmap_0[byte_index] |= 1 << bit_index;
                            result_count_local += 1;
                        }
                    }
                }
            }
        }
    }

    for plo in not_query_list.iter_mut() {
        if !plo.bm25_flag {
            continue;
        }

        match plo.compression_type {
            CompressionType::Array => {
                let ushorts1 =
                    cast_byte_ushort_slice(&plo.byte_array[plo.compressed_doc_id_range..]);

                for item in ushorts1.iter().take(plo.p_docid_count) {
                    let docid = *item as usize;
                    let byte_index = docid >> 3;
                    let bit_index = docid & 7;
                    if bitmap_0[byte_index] & (1 << bit_index) != 0 {
                        bitmap_0[byte_index] &= !(1 << bit_index);
                        result_count_local -= 1;
                    }
                }
            }

            CompressionType::Rle => {
                let ushorts1 =
                    cast_byte_ushort_slice(&plo.byte_array[plo.compressed_doc_id_range..]);
                let runs_count = ushorts1[0] as i32;

                plo.p_docid = 0;
                for i in (1..(runs_count << 1) + 1).step_by(2) {
                    let startdocid: u16 = ushorts1[i as usize];
                    let runlength = ushorts1[(i + 1) as usize];

                    for j in 0..=runlength {
                        let docid = (startdocid + j) as usize;

                        let byte_index = docid >> 3;
                        let bit_index = docid & 7;
                        if bitmap_0[byte_index] & (1 << bit_index) != 0 {
                            bitmap_0[byte_index] &= !(1 << bit_index);
                            result_count_local -= 1;
                        }
                    }
                }
            }

            CompressionType::Bitmap => {
                let ulongs0 = cast_byte_ulong_slice(&bitmap_0);
                let ulongs1 = cast_byte_ulong_slice(
                    &plo.byte_array
                        [plo.compressed_doc_id_range..plo.compressed_doc_id_range + 8192],
                );

                ulongs0.iter_mut().zip(ulongs1.iter()).for_each(|(x1, x2)| {
                    result_count_local -= u64::count_ones(*x1 & *x2);
                    *x1 &= !x2;
                });
            }

            _ => {}
        }
    }

    *result_count += result_count_local as i32;
}

#[allow(clippy::too_many_arguments)]
#[allow(clippy::ptr_arg)]
pub(crate) async fn union_docid_2<'a>(
    index: &'a Index,
    non_unique_query_list: &mut Vec<NonUniquePostingListObjectQuery<'a>>,
    query_list: &mut Vec<PostingListObjectQuery<'a>>,
    not_query_list: &mut Vec<PostingListObjectQuery<'a>>,
    result_count_arc: &Arc<AtomicUsize>,
    topk_candidates: &mut MinHeap,
    top_k: usize,
    result_type: &ResultType,
    field_filter_set: &AHashSet<u16>,
    matching_blocks: &mut i32,
) {
    let filtered = !not_query_list.is_empty() || field_filter_set.len() > 0;
    let mut count = 0;
    if filtered {
        single_blockid(
            index,
            non_unique_query_list,
            &mut query_list[0..1].to_vec(),
            not_query_list,
            result_count_arc,
            topk_candidates,
            top_k,
            &ResultType::Count,
            field_filter_set,
            matching_blocks,
        )
        .await;

        single_blockid(
            index,
            non_unique_query_list,
            &mut query_list[1..2].to_vec(),
            not_query_list,
            result_count_arc,
            topk_candidates,
            top_k,
            &ResultType::Count,
            field_filter_set,
            matching_blocks,
        )
        .await;

        count = result_count_arc.load(Ordering::Relaxed);
        result_count_arc.store(0, Ordering::Relaxed);
    }

    intersection_blockid(
        index,
        non_unique_query_list,
        query_list,
        not_query_list,
        result_count_arc,
        topk_candidates,
        top_k,
        result_type,
        field_filter_set,
        matching_blocks,
        false,
    )
    .await;

    let result_count_local = if filtered {
        count
    } else {
        (query_list[0].posting_count + query_list[1].posting_count) as usize
    } - result_count_arc.load(Ordering::Relaxed);

    if result_type == &ResultType::Count {
        result_count_arc.store(result_count_local, Ordering::Relaxed);
        return;
    }

    if (topk_candidates.current_heap_size < top_k)
        || (query_list[0].max_list_score > topk_candidates._elements[0].score)
    {
        for i in 0..topk_candidates.current_heap_size {
            topk_candidates.docid_hashset.insert(
                topk_candidates._elements[i].doc_id,
                topk_candidates._elements[i].score,
            );
        }

        single_blockid(
            index,
            non_unique_query_list,
            &mut query_list[0..1].to_vec(),
            not_query_list,
            result_count_arc,
            topk_candidates,
            top_k,
            &ResultType::Topk,
            field_filter_set,
            matching_blocks,
        )
        .await;
    }

    if (topk_candidates.current_heap_size < top_k)
        || (query_list[1].max_list_score > topk_candidates._elements[0].score)
    {
        for i in 0..topk_candidates.current_heap_size {
            topk_candidates.docid_hashset.insert(
                topk_candidates._elements[i].doc_id,
                topk_candidates._elements[i].score,
            );
        }

        single_blockid(
            index,
            non_unique_query_list,
            &mut query_list[1..2].to_vec(),
            not_query_list,
            result_count_arc,
            topk_candidates,
            top_k,
            &ResultType::Topk,
            field_filter_set,
            matching_blocks,
        )
        .await;
    }

    result_count_arc.store(result_count_local, Ordering::Relaxed);
}

#[allow(clippy::too_many_arguments)]
#[async_recursion]
pub(crate) async fn union_docid_3<'a>(
    index: &'a Index,
    non_unique_query_list: &mut Vec<NonUniquePostingListObjectQuery<'a>>,
    query_queue: &'a mut Vec<QueueObject<'a>>,
    not_query_list: &mut Vec<PostingListObjectQuery<'a>>,

    result_count_arc: &Arc<AtomicUsize>,
    topk_candidates: &mut MinHeap,
    top_k: usize,
    result_type: &ResultType,
    field_filter_set: &AHashSet<u16>,
    matching_blocks: &mut i32,
) {
    let queue_object = query_queue.remove(0);

    let mut query_list = queue_object.query_list;

    if result_type == &ResultType::Topk || result_type == &ResultType::TopkCount {
        if query_list.len() >= 3 {
            intersection_blockid(
                index,
                non_unique_query_list,
                &mut query_list,
                not_query_list,
                result_count_arc,
                topk_candidates,
                top_k,
                &ResultType::Topk,
                field_filter_set,
                matching_blocks,
                false,
            )
            .await;

            for j in 0..topk_candidates.current_heap_size {
                topk_candidates.docid_hashset.insert(
                    topk_candidates._elements[j].doc_id,
                    topk_candidates._elements[j].score,
                );
            }

            {
                for i in queue_object.query_index..query_list.len() {
                    let ii = query_list.len() - 1 - i;

                    for plo in query_list.iter_mut() {
                        plo.p_block = 0;
                    }

                    let list = if ii == 0 {
                        query_list[1..query_list.len()].to_vec()
                    } else if ii == query_list.len() - 1 {
                        query_list[0..query_list.len() - 1].to_vec()
                    } else {
                        [&query_list[0..ii], &query_list[ii + 1..query_list.len()]].concat()
                    };

                    let mut max_score = 0.0;
                    for term in list.iter() {
                        max_score += term.max_list_score;
                    }

                    if topk_candidates.current_heap_size < top_k
                        || max_score > topk_candidates._elements[0].score
                    {
                        if !query_queue.is_empty()
                            && max_score > query_queue[query_queue.len() - 1].max_score
                        {
                            let pos = query_queue
                                .binary_search_by(|e| {
                                    e.max_score
                                        .partial_cmp(&max_score)
                                        .expect("Couldn't compare values")
                                        .reverse()
                                })
                                .unwrap_or_else(|e| e);
                            query_queue.insert(
                                pos,
                                QueueObject {
                                    query_list: list,
                                    query_index: i,
                                    max_score,
                                },
                            );
                        } else {
                            query_queue.push(QueueObject {
                                query_list: list,
                                query_index: i,
                                max_score,
                            });
                        }
                    };
                }
            }
        } else {
            union_docid_2(
                index,
                non_unique_query_list,
                &mut query_list,
                not_query_list,
                result_count_arc,
                topk_candidates,
                top_k,
                &ResultType::Topk,
                field_filter_set,
                matching_blocks,
            )
            .await;
        }

        if !query_queue.is_empty()
            && (topk_candidates.current_heap_size < top_k
                || query_queue.first().unwrap().max_score > topk_candidates._elements[0].score)
        {
            for i in 0..topk_candidates.current_heap_size {
                topk_candidates.docid_hashset.insert(
                    topk_candidates._elements[i].doc_id,
                    topk_candidates._elements[i].score,
                );
            }

            union_docid_3(
                index,
                non_unique_query_list,
                query_queue,
                not_query_list,
                result_count_arc,
                topk_candidates,
                top_k,
                &ResultType::Topk,
                field_filter_set,
                matching_blocks,
            )
            .await;
        }
    }

    if result_type == &ResultType::Count || result_type == &ResultType::TopkCount {
        for plo in query_list.iter_mut() {
            plo.p_block = 0;
        }

        result_count_arc.store(0, Ordering::Relaxed);

        union_blockid(
            index,
            non_unique_query_list,
            &mut query_list,
            not_query_list,
            result_count_arc,
            topk_candidates,
            top_k,
            &ResultType::Count,
            field_filter_set,
        )
        .await;
    }
}
