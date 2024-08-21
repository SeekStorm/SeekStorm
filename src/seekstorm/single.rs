use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use crate::{
    add_result::{add_result_singleterm_multifield, PostingListObjectSingle},
    compatible::{_blsr_u64, _mm_tzcnt_64},
    index::{
        AccessType, BlockObjectIndex, CompressionType, Index, NonUniquePostingListObjectQuery,
        PostingListObjectQuery, SORT_FLAG, SPEEDUP_FLAG,
    },
    intersection::{bitpacking32_get_delta, BlockObject},
    search::{FilterSparse, ResultType, SearchResult},
    utils::{cast_byte_ulong_slice, cast_byte_ushort_slice, read_u16},
};

use ahash::AHashSet;
use num_traits::FromPrimitive;

#[allow(clippy::too_many_arguments)]
#[allow(clippy::ptr_arg)]
#[allow(non_snake_case)]
pub(crate) async fn single_docid<'a>(
    index: &'a Index,
    query_list: &mut Vec<PostingListObjectQuery<'a>>,
    not_query_list: &mut [PostingListObjectQuery<'a>],
    blo: &BlockObjectIndex,
    term_index: usize,
    result_count: &mut i32,
    search_result: &mut SearchResult,
    top_k: usize,
    result_type: &ResultType,
    field_filter_set: &AHashSet<u16>,
    facet_filter: &[FilterSparse],
) {
    let block_score = blo.max_block_score;
    let filtered = !not_query_list.is_empty()
        || !field_filter_set.is_empty()
        || (!search_result.query_facets.is_empty() || !facet_filter.is_empty())
            && result_type != &ResultType::Topk;
    if SPEEDUP_FLAG
        && search_result.topk_candidates.current_heap_size == top_k
        && block_score <= search_result.topk_candidates._elements[0].score
        && (!filtered || result_type == &ResultType::Topk)
    {
        return;
    }

    let block_id = blo.block_id;
    for plo in not_query_list.iter_mut() {
        let query_list_item_mut = plo;

        let result = query_list_item_mut
            .blocks
            .binary_search_by(|block| block.block_id.cmp(&block_id));
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

        if index.meta.access_type == AccessType::Mmap {
            let segment = &index.segments_index[query_list_item_mut.key0 as usize];
            query_list_item_mut.byte_array =
                &index.index_file_mmap[segment.byte_array_blocks_pointer[blo.block_id as usize].0
                    ..segment.byte_array_blocks_pointer[blo.block_id as usize].0
                        + segment.byte_array_blocks_pointer[blo.block_id as usize].1];
        } else {
            query_list_item_mut.byte_array = &index.segments_index
                [query_list_item_mut.key0 as usize]
                .byte_array_blocks[blo.block_id as usize];
        }

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

    let compression_type: CompressionType =
        FromPrimitive::from_i32((blo.compression_type_pointer >> 30) as i32).unwrap();

    let rank_position_pointer_range: u32 =
        blo.compression_type_pointer & 0b0011_1111_1111_1111_1111_1111_1111_1111;

    let posting_pointer_size_sum = blo.pointer_pivot_p_docid as u32 * 2
        + if (blo.pointer_pivot_p_docid as usize) <= blo.posting_count as usize {
            ((blo.posting_count as u32 + 1) - blo.pointer_pivot_p_docid as u32) * 3
        } else {
            0
        };
    let compressed_doc_id_range: u32 = rank_position_pointer_range + posting_pointer_size_sum;

    let query_list_item_mut = &mut query_list[term_index];

    let byte_array = if index.meta.access_type == AccessType::Mmap {
        let segment = &index.segments_index[query_list_item_mut.key0 as usize];
        &index.index_file_mmap[segment.byte_array_blocks_pointer[blo.block_id as usize].0
            ..segment.byte_array_blocks_pointer[blo.block_id as usize].0
                + segment.byte_array_blocks_pointer[blo.block_id as usize].1]
    } else {
        &index.segments_index[query_list_item_mut.key0 as usize].byte_array_blocks
            [blo.block_id as usize]
    };

    let mut plo = PostingListObjectSingle {
        rank_position_pointer_range,
        pointer_pivot_p_docid: blo.pointer_pivot_p_docid,
        byte_array,
        p_docid: 0,
        idf: query_list_item_mut.idf,
        idf_bigram1: query_list_item_mut.idf_bigram1,
        idf_bigram2: query_list_item_mut.idf_bigram2,
        is_bigram: query_list_item_mut.is_bigram,
    };

    match compression_type {
        CompressionType::Array => {
            let ushorts1 = cast_byte_ushort_slice(&byte_array[compressed_doc_id_range as usize..]);

            for i in 0..=blo.posting_count {
                plo.p_docid = i as i32;

                add_result_singleterm_multifield(
                    index,
                    ((blo.block_id as usize) << 16) | ushorts1[i as usize] as usize,
                    result_count,
                    search_result,
                    top_k,
                    result_type,
                    field_filter_set,
                    facet_filter,
                    &plo,
                    not_query_list,
                    block_score,
                );
            }
        }

        CompressionType::Delta => {
            let deltasizebits = 4;
            let rangebits: i32 =
                byte_array[compressed_doc_id_range as usize] as i32 >> (8 - deltasizebits);

            let mut docid_old: i32 = -1;
            let mut bitposition: u32 = (compressed_doc_id_range << 3) + deltasizebits;

            for i in 0..=blo.posting_count {
                plo.p_docid = i as i32;
                let delta = bitpacking32_get_delta(byte_array, bitposition, rangebits as u32);
                bitposition += rangebits as u32;

                let doc_id: u16 = (docid_old + delta as i32 + 1) as u16;
                docid_old = doc_id as i32;

                add_result_singleterm_multifield(
                    index,
                    ((blo.block_id as usize) << 16) | doc_id as usize,
                    result_count,
                    search_result,
                    top_k,
                    result_type,
                    field_filter_set,
                    facet_filter,
                    &plo,
                    not_query_list,
                    block_score,
                );
            }
        }

        CompressionType::Rle => {
            let ushorts1 = cast_byte_ushort_slice(&byte_array[compressed_doc_id_range as usize..]);
            let runs_count = ushorts1[0] as i32;

            plo.p_docid = 0;
            for i in (1..(runs_count << 1) + 1).step_by(2) {
                let startdocid: u16 = ushorts1[i as usize];
                let runlength = ushorts1[(i + 1) as usize];

                for j in 0..=runlength {
                    add_result_singleterm_multifield(
                        index,
                        ((blo.block_id as usize) << 16) | (startdocid + j) as usize,
                        result_count,
                        search_result,
                        top_k,
                        result_type,
                        field_filter_set,
                        facet_filter,
                        &plo,
                        not_query_list,
                        block_score,
                    );

                    plo.p_docid += 1;
                }
            }
        }

        CompressionType::Bitmap => {
            let ulongs1 = cast_byte_ulong_slice(&byte_array[compressed_doc_id_range as usize..]);
            plo.p_docid = 0;
            let block_id_msb = (blo.block_id as usize) << 16;

            for ulong_pos in 0u64..1024 {
                let mut intersect: u64 = ulongs1[ulong_pos as usize];

                while intersect != 0 {
                    let bit_pos = unsafe { _mm_tzcnt_64(intersect) } as u64;

                    intersect = unsafe { _blsr_u64(intersect) };

                    add_result_singleterm_multifield(
                        index,
                        block_id_msb | ((ulong_pos << 6) + bit_pos) as usize,
                        result_count,
                        search_result,
                        top_k,
                        result_type,
                        field_filter_set,
                        facet_filter,
                        &plo,
                        not_query_list,
                        block_score,
                    );

                    plo.p_docid += 1;
                }
            }
        }

        _ => {}
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn single_blockid<'a>(
    index: &'a Index,
    non_unique_query_list: &mut [NonUniquePostingListObjectQuery<'a>],
    query_list: &mut Vec<PostingListObjectQuery<'a>>,
    not_query_list: &mut [PostingListObjectQuery<'a>],
    result_count_arc: &Arc<AtomicUsize>,
    search_result: &mut SearchResult,
    top_k: usize,
    result_type: &ResultType,
    field_filter_set: &AHashSet<u16>,
    facet_filter: &[FilterSparse],
    matching_blocks: &mut i32,
) {
    let term_index = 0;

    let filtered = !not_query_list.is_empty()
        || !field_filter_set.is_empty()
        || !index.delete_hashset.is_empty()
        || (!search_result.query_facets.is_empty() || !facet_filter.is_empty())
            && result_type != &ResultType::Topk;

    if (index.enable_single_term_topk || (result_type == &ResultType::Count))
        && (non_unique_query_list.len() <= 1)
        && !filtered
    {
        result_count_arc.fetch_add(
            query_list[term_index].posting_count as usize,
            Ordering::Relaxed,
        );

        return;
    }

    let mut result_count_local = 0;

    let enable_inter_query_threading_single =
        if !index.enable_search_quality_test && index.enable_inter_query_threading_auto {
            query_list[term_index].posting_count / query_list[term_index].p_block_max as u32 > 10
        } else {
            index.enable_inter_query_threading
        };

    let mut block_vec: Vec<BlockObject> = Vec::new();

    for (p_block, blo) in query_list[term_index].blocks.iter().enumerate() {
        if !enable_inter_query_threading_single {
            let block_score = blo.max_block_score;

            if SPEEDUP_FLAG && SORT_FLAG {
                let p_block_vec: Vec<i32> = vec![p_block as i32];
                let block_object = BlockObject {
                    block_id: blo.block_id as usize,
                    block_score,
                    p_block_vec,
                };

                block_vec.push(block_object);
            } else if !SPEEDUP_FLAG
                || (filtered && result_type != &ResultType::Topk)
                || search_result.topk_candidates.current_heap_size < top_k
                || block_score > search_result.topk_candidates._elements[0].score
            {
                single_docid(
                    index,
                    query_list,
                    not_query_list,
                    blo,
                    term_index,
                    &mut result_count_local,
                    search_result,
                    top_k,
                    result_type,
                    field_filter_set,
                    facet_filter,
                )
                .await;
            }
        }
    }

    if SORT_FLAG && SPEEDUP_FLAG {
        block_vec.sort_unstable_by(|x, y| y.block_score.partial_cmp(&x.block_score).unwrap());
        let mut block_index = 0;
        for block in block_vec {
            block_index += 1;
            let blo = &query_list[term_index].blocks[block.p_block_vec[0] as usize];

            single_docid(
                index,
                query_list,
                not_query_list,
                blo,
                term_index,
                &mut result_count_local,
                search_result,
                top_k,
                result_type,
                field_filter_set,
                facet_filter,
            )
            .await;

            if (!filtered || result_type == &ResultType::Topk)
                && (search_result.topk_candidates.current_heap_size == top_k)
                && ((block.block_score <= search_result.topk_candidates._elements[0].score)
                    || (block_index == top_k))
            {
                break;
            }
        }
    }

    result_count_arc.fetch_add(
        if !filtered {
            query_list[term_index].posting_count as usize
        } else {
            result_count_local as usize
        },
        Ordering::Relaxed,
    );

    *matching_blocks = query_list[term_index].blocks.len() as i32;
}
