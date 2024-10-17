use ahash::AHashSet;
use smallvec::{smallvec, SmallVec};
use std::cmp::Ordering;

use crate::{
    geo_search::{decode_morton_2_d, euclidian_distance},
    index::{
        get_document_length_compressed_mmap, AccessType, CompressionType, Index,
        NonUniquePostingListObjectQuery, PostingListObjectQuery, SimilarityType, FIELD_STOP_BIT_1,
        FIELD_STOP_BIT_2, SPEEDUP_FLAG, STOP_BIT,
    },
    min_heap,
    search::{FilterSparse, Ranges, ResultType, SearchResult},
    utils::{
        read_f32, read_f64, read_i16, read_i32, read_i64, read_i8, read_u16, read_u32, read_u64,
        read_u8,
    },
};

pub(crate) const K: f32 = 1.2;
pub(crate) const B: f32 = 0.75;
pub(crate) const SIGMA: f32 = 0.0;

pub(crate) const DOCUMENT_LENGTH_COMPRESSION: [u32; 256] = [
    0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0x9, 0xa, 0xb, 0xc, 0xd, 0xe, 0xf, 0x10, 0x11,
    0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x20, 0x21,
    0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28, 0x2a, 0x2c, 0x2e, 0x30, 0x32, 0x34, 0x36, 0x38, 0x3c,
    0x40, 0x44, 0x48, 0x4c, 0x50, 0x54, 0x58, 0x60, 0x68, 0x70, 0x78, 0x80, 0x88, 0x90, 0x98, 0xa8,
    0xb8, 0xc8, 0xd8, 0xe8, 0xf8, 0x108, 0x118, 0x138, 0x158, 0x178, 0x198, 0x1b8, 0x1d8, 0x1f8,
    0x218, 0x258, 0x298, 0x2d8, 0x318, 0x358, 0x398, 0x3d8, 0x418, 0x498, 0x518, 0x598, 0x618,
    0x698, 0x718, 0x798, 0x818, 0x918, 0xa18, 0xb18, 0xc18, 0xd18, 0xe18, 0xf18, 0x1018, 0x1218,
    0x1418, 0x1618, 0x1818, 0x1a18, 0x1c18, 0x1e18, 0x2018, 0x2418, 0x2818, 0x2c18, 0x3018, 0x3418,
    0x3818, 0x3c18, 0x4018, 0x4818, 0x5018, 0x5818, 0x6018, 0x6818, 0x7018, 0x7818, 0x8018, 0x9018,
    0xa018, 0xb018, 0xc018, 0xd018, 0xe018, 0xf018, 0x10018, 0x12018, 0x14018, 0x16018, 0x18018,
    0x1a018, 0x1c018, 0x1e018, 0x20018, 0x24018, 0x28018, 0x2c018, 0x30018, 0x34018, 0x38018,
    0x3c018, 0x40018, 0x48018, 0x50018, 0x58018, 0x60018, 0x68018, 0x70018, 0x78018, 0x80018,
    0x90018, 0xa0018, 0xb0018, 0xc0018, 0xd0018, 0xe0018, 0xf0018, 0x100018, 0x120018, 0x140018,
    0x160018, 0x180018, 0x1a0018, 0x1c0018, 0x1e0018, 0x200018, 0x240018, 0x280018, 0x2c0018,
    0x300018, 0x340018, 0x380018, 0x3c0018, 0x400018, 0x480018, 0x500018, 0x580018, 0x600018,
    0x680018, 0x700018, 0x780018, 0x800018, 0x900018, 0xa00018, 0xb00018, 0xc00018, 0xd00018,
    0xe00018, 0xf00018, 0x1000018, 0x1200018, 0x1400018, 0x1600018, 0x1800018, 0x1a00018,
    0x1c00018, 0x1e00018, 0x2000018, 0x2400018, 0x2800018, 0x2c00018, 0x3000018, 0x3400018,
    0x3800018, 0x3c00018, 0x4000018, 0x4800018, 0x5000018, 0x5800018, 0x6000018, 0x6800018,
    0x7000018, 0x7800018, 0x8000018, 0x9000018, 0xa000018, 0xb000018, 0xc000018, 0xd000018,
    0xe000018, 0xf000018, 0x10000018, 0x12000018, 0x14000018, 0x16000018, 0x18000018, 0x1a000018,
    0x1c000018, 0x1e000018, 0x20000018, 0x24000018, 0x28000018, 0x2c000018, 0x30000018, 0x34000018,
    0x38000018, 0x3c000018, 0x40000018, 0x48000018, 0x50000018, 0x58000018, 0x60000018, 0x68000018,
    0x70000018, 0x78000018,
];

pub(crate) struct PostingListObjectSingle<'a> {
    pub rank_position_pointer_range: u32,
    pub pointer_pivot_p_docid: u16,
    pub byte_array: &'a [u8],
    pub p_docid: i32,
    pub idf: f32,

    pub idf_bigram1: f32,
    pub idf_bigram2: f32,
    pub is_bigram: bool,
}

#[inline(always)]
pub(crate) fn get_next_position_singlefield(plo: &mut NonUniquePostingListObjectQuery) -> u32 {
    if plo.is_embedded {
        return plo.embedded_positions[plo.p_pos as usize];
    }

    if (plo.byte_array[plo.positions_pointer] & STOP_BIT) != 0 {
        let position = (plo.byte_array[plo.positions_pointer] & 0b0111_1111) as u32;
        plo.positions_pointer += 1;
        position
    } else if (plo.byte_array[plo.positions_pointer + 1] & STOP_BIT) != 0 {
        let position = ((plo.byte_array[plo.positions_pointer] as u32) << 7)
            | (plo.byte_array[plo.positions_pointer + 1] & 0b0111_1111) as u32;
        plo.positions_pointer += 2;
        position
    } else {
        let position = ((plo.byte_array[plo.positions_pointer] as u32) << 13)
            | ((plo.byte_array[plo.positions_pointer + 1] as u32) << 7)
            | (plo.byte_array[plo.positions_pointer + 2] & 0b0111_1111) as u32;
        plo.positions_pointer += 3;
        position
    }
}

#[inline(always)]
pub(crate) fn get_next_position_multifield(plo: &mut NonUniquePostingListObjectQuery) -> u32 {
    if plo.is_embedded {
        return plo.embedded_positions[if plo.p_field == 0 {
            plo.p_pos as usize
        } else {
            plo.field_vec[plo.p_field - 1].1 + plo.p_pos as usize
        }];
    }

    if (plo.byte_array[plo.positions_pointer] & STOP_BIT) != 0 {
        let position = (plo.byte_array[plo.positions_pointer] & 0b0111_1111) as u32;
        plo.positions_pointer += 1;
        position
    } else if (plo.byte_array[plo.positions_pointer + 1] & STOP_BIT) != 0 {
        let position = ((plo.byte_array[plo.positions_pointer] as u32) << 7)
            | (plo.byte_array[plo.positions_pointer + 1] & 0b0111_1111) as u32;
        plo.positions_pointer += 2;
        position
    } else {
        let position = ((plo.byte_array[plo.positions_pointer] as u32) << 13)
            | ((plo.byte_array[plo.positions_pointer + 1] as u32) << 7)
            | (plo.byte_array[plo.positions_pointer + 2] & 0b0111_1111) as u32;
        plo.positions_pointer += 3;
        position
    }
}

/// Post processing after AND intersection candidates have been found
/// Phrase intersection
/// BM25 ranking vs. seekstorm ranking (implicit phrase search, term proximity, field type boost, source reputation)
/// BM25 is default baseline in IR academics, but exhibits inferior relevance for practical use
#[allow(clippy::too_many_arguments)]
#[inline(always)]
pub(crate) fn add_result_singleterm_multifield(
    index: &Index,
    docid: usize,
    result_count: &mut i32,
    search_result: &mut SearchResult,

    top_k: usize,
    result_type: &ResultType,
    field_filter_set: &AHashSet<u16>,
    facet_filter: &[FilterSparse],

    plo_single: &PostingListObjectSingle,
    not_query_list: &mut [PostingListObjectQuery],
    block_score: f32,
) {
    if index.indexed_field_vec.len() == 1 {
        add_result_singleterm_singlefield(
            index,
            docid,
            result_count,
            search_result,
            top_k,
            result_type,
            field_filter_set,
            facet_filter,
            plo_single,
            not_query_list,
            block_score,
        );
        return;
    }

    if !index.delete_hashset.is_empty() && index.delete_hashset.contains(&docid) {
        return;
    }

    for plo in not_query_list.iter_mut() {
        if !plo.bm25_flag {
            continue;
        }

        let local_docid = docid & 0b11111111_11111111;

        match &plo.compression_type {
            CompressionType::Array => {
                while plo.p_docid < plo.p_docid_count
                    && (plo.p_docid == 0 || (plo.docid as usize) < local_docid)
                {
                    plo.docid = read_u16(
                        plo.byte_array,
                        plo.compressed_doc_id_range + (plo.p_docid << 1),
                    ) as i32;
                    plo.p_docid += 1;
                }
                if (plo.docid as usize) == local_docid {
                    return;
                }
            }
            CompressionType::Bitmap => {
                if (plo.byte_array[plo.compressed_doc_id_range + (local_docid >> 3)]
                    & (1 << (local_docid & 7)))
                    > 0
                {
                    return;
                }
            }
            CompressionType::Rle => {
                if local_docid >= plo.docid as usize && local_docid <= plo.run_end as usize {
                    return;
                } else {
                    while (plo.p_run_sum as usize) < plo.p_docid_count
                        && local_docid > plo.run_end as usize
                    {
                        let startdocid = read_u16(
                            plo.byte_array,
                            plo.compressed_doc_id_range + plo.p_run as usize,
                        );
                        let runlength = read_u16(
                            plo.byte_array,
                            plo.compressed_doc_id_range + plo.p_run as usize + 2,
                        );
                        plo.docid = startdocid as i32;
                        plo.run_end = (startdocid + runlength) as i32;
                        plo.p_run_sum += runlength as i32;
                        plo.p_run += 4;

                        if local_docid >= startdocid as usize && local_docid <= plo.run_end as usize
                        {
                            return;
                        }
                    }
                }
            }
            _ => {}
        }
    }

    if !facet_filter.is_empty() && is_facet_filter(index, facet_filter, docid) {
        return;
    };

    let mut field_vec: SmallVec<[(u16, usize); 2]> = SmallVec::new();
    let mut field_vec_bigram1: SmallVec<[(u16, usize); 2]> = SmallVec::new();
    let mut field_vec_bigram2: SmallVec<[(u16, usize); 2]> = SmallVec::new();

    match *result_type {
        ResultType::Count => {
            if !field_filter_set.is_empty() {
                decode_positions_singleterm_multifield(
                    index,
                    plo_single,
                    &mut field_vec,
                    &mut field_vec_bigram1,
                    &mut field_vec_bigram2,
                );

                if field_vec.len() + field_filter_set.len() <= index.indexed_field_vec.len() {
                    let mut match_flag = false;
                    for field in field_vec.iter() {
                        if field_filter_set.contains(&field.0) {
                            match_flag = true;
                        }
                    }
                    if !match_flag {
                        return;
                    }
                }
            }

            facet_count(index, search_result, docid);

            *result_count += 1;

            return;
        }
        ResultType::Topk => {
            if SPEEDUP_FLAG
                && search_result.topk_candidates.result_sort.is_empty()
                && search_result.topk_candidates.current_heap_size >= top_k
                && block_score <= search_result.topk_candidates._elements[0].score
            {
                return;
            }

            if !field_filter_set.is_empty() {
                decode_positions_singleterm_multifield(
                    index,
                    plo_single,
                    &mut field_vec,
                    &mut field_vec_bigram1,
                    &mut field_vec_bigram2,
                );

                if field_vec.len() + field_filter_set.len() <= index.indexed_field_vec.len() {
                    let mut match_flag = false;
                    for field in field_vec.iter() {
                        if field_filter_set.contains(&field.0) {
                            match_flag = true;
                        }
                    }
                    if !match_flag {
                        return;
                    }
                }
            }
        }
        ResultType::TopkCount => {
            if !field_filter_set.is_empty() {
                decode_positions_singleterm_multifield(
                    index,
                    plo_single,
                    &mut field_vec,
                    &mut field_vec_bigram1,
                    &mut field_vec_bigram2,
                );

                if field_vec.len() + field_filter_set.len() <= index.indexed_field_vec.len() {
                    let mut match_flag = false;
                    for field in field_vec.iter() {
                        if field_filter_set.contains(&field.0) {
                            match_flag = true;
                        }
                    }
                    if !match_flag {
                        return;
                    }
                }
            }

            facet_count(index, search_result, docid);

            *result_count += 1;

            if SPEEDUP_FLAG
                && search_result.topk_candidates.result_sort.is_empty()
                && search_result.topk_candidates.current_heap_size >= top_k
                && block_score <= search_result.topk_candidates._elements[0].score
            {
                return;
            }
        }
    }

    if field_filter_set.is_empty() {
        decode_positions_singleterm_multifield(
            index,
            plo_single,
            &mut field_vec,
            &mut field_vec_bigram1,
            &mut field_vec_bigram2,
        );
    }

    let bm25f = get_bm25f_singleterm_multifield(
        index,
        docid,
        plo_single,
        field_vec,
        field_vec_bigram1,
        field_vec_bigram2,
    );

    search_result.topk_candidates.add_topk(
        min_heap::Result {
            doc_id: docid,
            score: bm25f,
        },
        top_k,
    );
}

#[inline]
pub(crate) fn is_facet_filter(index: &Index, facet_filter: &[FilterSparse], docid: usize) -> bool {
    for (i, facet) in index.facets.iter().enumerate() {
        match &facet_filter[i] {
            FilterSparse::U8(range) => {
                let facet_value_id = read_u8(
                    &index.facets_file_mmap,
                    (index.facets_size_sum * docid) + facet.offset,
                );
                if !range.contains(&facet_value_id) {
                    return true;
                }
            }
            FilterSparse::U16(range) => {
                let facet_value_id = read_u16(
                    &index.facets_file_mmap,
                    (index.facets_size_sum * docid) + facet.offset,
                );
                if !range.contains(&facet_value_id) {
                    return true;
                }
            }
            FilterSparse::U32(range) => {
                let facet_value_id = read_u32(
                    &index.facets_file_mmap,
                    (index.facets_size_sum * docid) + facet.offset,
                );
                if !range.contains(&facet_value_id) {
                    return true;
                }
            }
            FilterSparse::U64(range) => {
                let facet_value_id = read_u64(
                    &index.facets_file_mmap,
                    (index.facets_size_sum * docid) + facet.offset,
                );
                if !range.contains(&facet_value_id) {
                    return true;
                }
            }
            FilterSparse::I8(range) => {
                let facet_value_id = read_i8(
                    &index.facets_file_mmap,
                    (index.facets_size_sum * docid) + facet.offset,
                );
                if !range.contains(&facet_value_id) {
                    return true;
                }
            }
            FilterSparse::I16(range) => {
                let facet_value_id = read_i16(
                    &index.facets_file_mmap,
                    (index.facets_size_sum * docid) + facet.offset,
                );
                if !range.contains(&facet_value_id) {
                    return true;
                }
            }
            FilterSparse::I32(range) => {
                let facet_value_id = read_i32(
                    &index.facets_file_mmap,
                    (index.facets_size_sum * docid) + facet.offset,
                );
                if !range.contains(&facet_value_id) {
                    return true;
                }
            }
            FilterSparse::I64(range) => {
                let facet_value_id = read_i64(
                    &index.facets_file_mmap,
                    (index.facets_size_sum * docid) + facet.offset,
                );
                if !range.contains(&facet_value_id) {
                    return true;
                }
            }
            FilterSparse::F32(range) => {
                let facet_value_id = read_f32(
                    &index.facets_file_mmap,
                    (index.facets_size_sum * docid) + facet.offset,
                );
                if !range.contains(&facet_value_id) {
                    return true;
                }
            }
            FilterSparse::F64(range) => {
                let facet_value_id = read_f64(
                    &index.facets_file_mmap,
                    (index.facets_size_sum * docid) + facet.offset,
                );
                if !range.contains(&facet_value_id) {
                    return true;
                }
            }
            FilterSparse::String(values) => {
                let facet_value_id = read_u16(
                    &index.facets_file_mmap,
                    (index.facets_size_sum * docid) + facet.offset,
                );
                if !values.contains(&facet_value_id) {
                    return true;
                }
            }

            FilterSparse::Point(point, distance_range, unit, range) => {
                let morton_code = read_u64(
                    &index.facets_file_mmap,
                    (index.facets_size_sum * docid) + facet.offset,
                );
                if range.contains(&morton_code) {
                    if !distance_range.contains(&euclidian_distance(
                        point,
                        &decode_morton_2_d(morton_code),
                        unit,
                    )) {
                        return true;
                    }
                } else {
                    return true;
                }
            }

            FilterSparse::None => {}
        }
    }
    false
}

#[inline]
pub(crate) fn facet_count(index: &Index, search_result: &mut SearchResult, docid: usize) {
    if !search_result.query_facets.is_empty() && !search_result.skip_facet_count {
        for (i, facet) in index.facets.iter().enumerate() {
            if search_result.query_facets[i].length == 0 {
                continue;
            }

            let facet_value_id = match &search_result.query_facets[i].ranges {
                Ranges::U8(_range_type, ranges) => {
                    let facet_value =
                        index.facets_file_mmap[(index.facets_size_sum * docid) + facet.offset];
                    ranges
                        .binary_search_by_key(&facet_value, |range| range.1)
                        .map_or_else(|idx| idx as u16 - 1, |idx| idx as u16)
                }
                Ranges::U16(_range_type, ranges) => {
                    let facet_value = read_u16(
                        &index.facets_file_mmap,
                        (index.facets_size_sum * docid) + facet.offset,
                    );
                    ranges
                        .binary_search_by_key(&facet_value, |range| range.1)
                        .map_or_else(|idx| idx as u16 - 1, |idx| idx as u16)
                }
                Ranges::U32(_range_type, ranges) => {
                    let facet_value = read_u32(
                        &index.facets_file_mmap,
                        (index.facets_size_sum * docid) + facet.offset,
                    );
                    ranges
                        .binary_search_by_key(&facet_value, |range| range.1)
                        .map_or_else(|idx| idx as u16 - 1, |idx| idx as u16)
                }
                Ranges::U64(_range_type, ranges) => {
                    let facet_value = read_u64(
                        &index.facets_file_mmap,
                        (index.facets_size_sum * docid) + facet.offset,
                    );
                    ranges
                        .binary_search_by_key(&facet_value, |range| range.1)
                        .map_or_else(|idx| idx as u16 - 1, |idx| idx as u16)
                }
                Ranges::I8(_range_type, ranges) => {
                    let facet_value = read_i8(
                        &index.facets_file_mmap,
                        (index.facets_size_sum * docid) + facet.offset,
                    );
                    ranges
                        .binary_search_by_key(&facet_value, |range| range.1)
                        .map_or_else(|idx| idx as u16 - 1, |idx| idx as u16)
                }
                Ranges::I16(_range_type, ranges) => {
                    let facet_value = read_i16(
                        &index.facets_file_mmap,
                        (index.facets_size_sum * docid) + facet.offset,
                    );
                    ranges
                        .binary_search_by_key(&facet_value, |range| range.1)
                        .map_or_else(|idx| idx as u16 - 1, |idx| idx as u16)
                }
                Ranges::I32(_range_type, ranges) => {
                    let facet_value = read_i32(
                        &index.facets_file_mmap,
                        (index.facets_size_sum * docid) + facet.offset,
                    );
                    ranges
                        .binary_search_by_key(&facet_value, |range| range.1)
                        .map_or_else(|idx| idx as u16 - 1, |idx| idx as u16)
                }

                Ranges::I64(_range_type, ranges) => {
                    let facet_value = read_i64(
                        &index.facets_file_mmap,
                        (index.facets_size_sum * docid) + facet.offset,
                    );
                    ranges
                        .binary_search_by_key(&facet_value, |range| range.1)
                        .map_or_else(|idx| idx as u16 - 1, |idx| idx as u16)
                }
                Ranges::F32(_range_type, ranges) => {
                    let facet_value = read_f32(
                        &index.facets_file_mmap,
                        (index.facets_size_sum * docid) + facet.offset,
                    );
                    ranges
                        .binary_search_by(|range| range.1.partial_cmp(&facet_value).unwrap())
                        .map_or_else(|idx| idx as u16 - 1, |idx| idx as u16)
                }
                Ranges::F64(_range_type, ranges) => {
                    let facet_value = read_f64(
                        &index.facets_file_mmap,
                        (index.facets_size_sum * docid) + facet.offset,
                    );
                    ranges
                        .binary_search_by(|range| range.1.partial_cmp(&facet_value).unwrap())
                        .map_or_else(|idx| idx as u16 - 1, |idx| idx as u16)
                }

                Ranges::Point(_range_type, ranges, base, unit) => {
                    let facet_value = read_u64(
                        &index.facets_file_mmap,
                        (index.facets_size_sum * docid) + facet.offset,
                    );
                    let facet_value_distance =
                        euclidian_distance(base, &decode_morton_2_d(facet_value), unit);
                    ranges
                        .binary_search_by(|range| {
                            range.1.partial_cmp(&facet_value_distance).unwrap()
                        })
                        .map_or_else(|idx| idx as u16 - 1, |idx| idx as u16)
                }

                _ => read_u16(
                    &index.facets_file_mmap,
                    (index.facets_size_sum * docid) + facet.offset,
                ),
            };

            *search_result.query_facets[i]
                .values
                .entry(facet_value_id)
                .or_insert(0) += 1;
        }
    }
}

#[allow(clippy::too_many_arguments)]
#[inline(always)]
pub(crate) fn add_result_singleterm_singlefield(
    index: &Index,
    docid: usize,
    result_count: &mut i32,
    search_result: &mut SearchResult,
    top_k: usize,
    result_type: &ResultType,
    field_filter_set: &AHashSet<u16>,
    facet_filter: &[FilterSparse],

    plo_single: &PostingListObjectSingle,
    not_query_list: &mut [PostingListObjectQuery],
    block_score: f32,
) {
    if !index.delete_hashset.is_empty() && index.delete_hashset.contains(&docid) {
        return;
    }

    for plo in not_query_list.iter_mut() {
        if !plo.bm25_flag {
            continue;
        }

        let local_docid = docid & 0b11111111_11111111;

        match &plo.compression_type {
            CompressionType::Array => {
                while plo.p_docid < plo.p_docid_count
                    && (plo.p_docid == 0 || (plo.docid as usize) < local_docid)
                {
                    plo.docid = read_u16(
                        plo.byte_array,
                        plo.compressed_doc_id_range + (plo.p_docid << 1),
                    ) as i32;
                    plo.p_docid += 1;
                }
                if (plo.docid as usize) == local_docid {
                    return;
                }
            }
            CompressionType::Bitmap => {
                if (plo.byte_array[plo.compressed_doc_id_range + (local_docid >> 3)]
                    & (1 << (local_docid & 7)))
                    > 0
                {
                    return;
                }
            }
            CompressionType::Rle => {
                if local_docid >= plo.docid as usize && local_docid <= plo.run_end as usize {
                    return;
                } else {
                    while (plo.p_run_sum as usize) < plo.p_docid_count
                        && local_docid > plo.run_end as usize
                    {
                        let startdocid = read_u16(
                            plo.byte_array,
                            plo.compressed_doc_id_range + plo.p_run as usize,
                        );
                        let runlength = read_u16(
                            plo.byte_array,
                            plo.compressed_doc_id_range + plo.p_run as usize + 2,
                        );
                        plo.docid = startdocid as i32;
                        plo.run_end = (startdocid + runlength) as i32;
                        plo.p_run_sum += runlength as i32;
                        plo.p_run += 4;

                        if local_docid >= startdocid as usize && local_docid <= plo.run_end as usize
                        {
                            return;
                        }
                    }
                }
            }
            _ => {}
        }
    }

    if !facet_filter.is_empty() && is_facet_filter(index, facet_filter, docid) {
        return;
    };

    let mut tf_bigram1 = 0;
    let mut tf_bigram2 = 0;
    let mut positions_count = 0;
    let field_id = 0u16;

    match *result_type {
        ResultType::Count => {
            if !field_filter_set.is_empty() {
                decode_positions_singleterm_singlefield(
                    plo_single,
                    &mut tf_bigram1,
                    &mut tf_bigram2,
                    &mut positions_count,
                );

                if field_filter_set.len() < index.indexed_field_vec.len() {
                    let mut match_flag = false;

                    if field_filter_set.contains(&field_id) {
                        match_flag = true;
                    }

                    if !match_flag {
                        return;
                    }
                }
            }
            facet_count(index, search_result, docid);

            *result_count += 1;

            return;
        }
        ResultType::Topk => {
            if SPEEDUP_FLAG
                && search_result.topk_candidates.result_sort.is_empty()
                && search_result.topk_candidates.current_heap_size >= top_k
                && block_score <= search_result.topk_candidates._elements[0].score
            {
                return;
            }

            if !field_filter_set.is_empty() {
                decode_positions_singleterm_singlefield(
                    plo_single,
                    &mut tf_bigram1,
                    &mut tf_bigram2,
                    &mut positions_count,
                );

                if field_filter_set.len() < index.indexed_field_vec.len() {
                    let mut match_flag = false;
                    if field_filter_set.contains(&field_id) {
                        match_flag = true;
                    }

                    if !match_flag {
                        return;
                    }
                }
            }
        }
        ResultType::TopkCount => {
            if !field_filter_set.is_empty() {
                decode_positions_singleterm_singlefield(
                    plo_single,
                    &mut tf_bigram1,
                    &mut tf_bigram2,
                    &mut positions_count,
                );

                if field_filter_set.len() < index.indexed_field_vec.len() {
                    let mut match_flag = false;
                    if field_filter_set.contains(&field_id) {
                        match_flag = true;
                    }
                    if !match_flag {
                        return;
                    }
                }
            }

            facet_count(index, search_result, docid);

            *result_count += 1;

            if SPEEDUP_FLAG
                && search_result.topk_candidates.result_sort.is_empty()
                && search_result.topk_candidates.current_heap_size >= top_k
                && block_score <= search_result.topk_candidates._elements[0].score
            {
                return;
            }
        }
    }

    if field_filter_set.is_empty() {
        decode_positions_singleterm_singlefield(
            plo_single,
            &mut tf_bigram1,
            &mut tf_bigram2,
            &mut positions_count,
        );
    }

    let bm25f = get_bm25f_singleterm_singlefield(
        index,
        docid,
        plo_single,
        tf_bigram1,
        tf_bigram2,
        positions_count,
    );

    search_result.topk_candidates.add_topk(
        min_heap::Result {
            doc_id: docid,
            score: bm25f,
        },
        top_k,
    );
}

#[inline(always)]
pub(crate) fn get_bm25f_singleterm_multifield(
    index: &Index,
    docid: usize,
    plo_single: &PostingListObjectSingle,
    field_vec: SmallVec<[(u16, usize); 2]>,
    field_vec_bigram1: SmallVec<[(u16, usize); 2]>,
    field_vec_bigram2: SmallVec<[(u16, usize); 2]>,
) -> f32 {
    let mut bm25f = 0.0;
    let block_id = docid >> 16;

    if index.indexed_field_vec.len() == 1 {
        let bm25_component =
            index.bm25_component_cache[if index.meta.access_type == AccessType::Mmap {
                get_document_length_compressed_mmap(index, 0, block_id, docid & 0b11111111_11111111)
            } else {
                index.level_index[block_id].document_length_compressed_array[0]
                    [docid & 0b11111111_11111111]
            } as usize];

        if !plo_single.is_bigram {
            let tf = field_vec[0].1 as f32;

            bm25f = plo_single.idf * ((tf * (K + 1.0) / (tf + bm25_component)) + SIGMA);
        } else {
            let tf_bigram1 = field_vec_bigram1[0].1 as f32;
            let tf_bigram2 = field_vec_bigram2[0].1 as f32;

            bm25f = plo_single.idf_bigram1
                * ((tf_bigram1 * (K + 1.0) / (tf_bigram1 + bm25_component)) + SIGMA)
                + plo_single.idf_bigram2
                    * ((tf_bigram2 * (K + 1.0) / (tf_bigram2 + bm25_component)) + SIGMA);
        }
    } else if !plo_single.is_bigram || index.meta.similarity == SimilarityType::Bm25fProximity {
        for field in field_vec.iter() {
            let field_id = field.0 as usize;

            let bm25_component =
                index.bm25_component_cache[if index.meta.access_type == AccessType::Mmap {
                    get_document_length_compressed_mmap(
                        index,
                        field_id,
                        block_id,
                        docid & 0b11111111_11111111,
                    )
                } else {
                    index.level_index[block_id].document_length_compressed_array[field_id]
                        [docid & 0b11111111_11111111]
                } as usize] as f32;

            let tf = field.1 as f32;

            let weight = index.indexed_schema_vec[field.0 as usize].boost;

            bm25f += weight * plo_single.idf * ((tf * (K + 1.0) / (tf + bm25_component)) + SIGMA);
        }
    } else {
        for field in field_vec_bigram1.iter() {
            let field_id = field.0 as usize;

            let bm25_component =
                index.bm25_component_cache[if index.meta.access_type == AccessType::Mmap {
                    get_document_length_compressed_mmap(
                        index,
                        field_id,
                        block_id,
                        docid & 0b11111111_11111111,
                    )
                } else {
                    index.level_index[block_id].document_length_compressed_array[field_id]
                        [docid & 0b11111111_11111111]
                } as usize] as f32;

            let tf_bigram1 = field.1 as f32;

            let weight = index.indexed_schema_vec[field.0 as usize].boost;

            bm25f += weight
                * plo_single.idf_bigram1
                * ((tf_bigram1 * (K + 1.0) / (tf_bigram1 + bm25_component)) + SIGMA);
        }

        for field in field_vec_bigram2.iter() {
            let field_id = field.0 as usize;

            let bm25_component =
                index.bm25_component_cache[if index.meta.access_type == AccessType::Mmap {
                    get_document_length_compressed_mmap(
                        index,
                        field_id,
                        block_id,
                        docid & 0b11111111_11111111,
                    )
                } else {
                    index.level_index[block_id].document_length_compressed_array[field_id]
                        [docid & 0b11111111_11111111]
                } as usize] as f32;

            let tf_bigram2 = field.1 as f32;

            let weight = index.indexed_schema_vec[field.0 as usize].boost;

            bm25f += weight
                * plo_single.idf_bigram2
                * ((tf_bigram2 * (K + 1.0) / (tf_bigram2 + bm25_component)) + SIGMA);
        }
    }

    bm25f
}

#[inline(always)]
pub(crate) fn get_bm25f_singleterm_singlefield(
    index: &Index,
    docid: usize,
    plo_single: &PostingListObjectSingle,
    tf_bigram1: u32,
    tf_bigram2: u32,
    positions_count: u32,
) -> f32 {
    let bm25f;
    let block_id = docid >> 16;

    if index.indexed_field_vec.len() == 1 {
        let bm25_component =
            index.bm25_component_cache[if index.meta.access_type == AccessType::Mmap {
                get_document_length_compressed_mmap(index, 0, block_id, docid & 0b11111111_11111111)
            } else {
                index.level_index[block_id].document_length_compressed_array[0]
                    [docid & 0b11111111_11111111]
            } as usize];

        if !plo_single.is_bigram {
            let tf = positions_count as f32;

            bm25f = plo_single.idf * ((tf * (K + 1.0) / (tf + bm25_component)) + SIGMA);
        } else {
            bm25f = plo_single.idf_bigram1
                * ((tf_bigram1 as f32 * (K + 1.0) / (tf_bigram1 as f32 + bm25_component)) + SIGMA)
                + plo_single.idf_bigram2
                    * ((tf_bigram2 as f32 * (K + 1.0) / (tf_bigram2 as f32 + bm25_component))
                        + SIGMA);
        }
    } else {
        let field_id = 0;

        let bm25_component =
            index.bm25_component_cache[if index.meta.access_type == AccessType::Mmap {
                get_document_length_compressed_mmap(
                    index,
                    field_id,
                    block_id,
                    docid & 0b11111111_11111111,
                )
            } else {
                index.level_index[block_id].document_length_compressed_array[field_id]
                    [docid & 0b11111111_11111111]
            } as usize];

        if !plo_single.is_bigram {
            let tf = positions_count as f32;

            bm25f = plo_single.idf * ((tf * (K + 1.0) / (tf + bm25_component)) + SIGMA);
        } else {
            bm25f = plo_single.idf_bigram1
                * ((tf_bigram1 as f32 * (K + 1.0) / (tf_bigram1 as f32 + bm25_component)) + SIGMA)
                + plo_single.idf_bigram2
                    * ((tf_bigram2 as f32 * (K + 1.0) / (tf_bigram2 as f32 + bm25_component))
                        + SIGMA);
        }
    }

    bm25f
}

#[inline(always)]
pub(crate) fn get_bm25f_multiterm_multifield(
    index: &Index,
    docid: usize,
    query_list: &mut [PostingListObjectQuery],
) -> f32 {
    let mut bm25f = 0.0;
    let block_id = docid >> 16;

    if index.indexed_field_vec.len() == 1 {
        let bm25_component =
            index.bm25_component_cache[if index.meta.access_type == AccessType::Mmap {
                get_document_length_compressed_mmap(index, 0, block_id, docid & 0b11111111_11111111)
            } else {
                index.level_index[block_id].document_length_compressed_array[0]
                    [docid & 0b11111111_11111111]
            } as usize];

        for plo in query_list.iter() {
            if !plo.bm25_flag {
                continue;
            }

            if !plo.is_bigram {
                let tf = plo.field_vec[0].1 as f32;

                bm25f += plo.idf * ((tf * (K + 1.0) / (tf + bm25_component)) + SIGMA);
            } else {
                bm25f += plo.idf_bigram1
                    * ((plo.tf_bigram1 as f32 * (K + 1.0)
                        / (plo.tf_bigram1 as f32 + bm25_component))
                        + SIGMA)
                    + plo.idf_bigram2
                        * ((plo.tf_bigram2 as f32 * (K + 1.0)
                            / (plo.tf_bigram2 as f32 + bm25_component))
                            + SIGMA);
            }
        }
    } else {
        let mut bm25_component_vec: SmallVec<[f32; 2]> = smallvec![0.0; 2];
        for plo in query_list.iter() {
            if !plo.bm25_flag {
                continue;
            }

            if !plo.is_bigram {
                for field in plo.field_vec.iter() {
                    let field_id = field.0 as usize;
                    if bm25_component_vec[field_id] == 0.0 {
                        bm25_component_vec[field_id] = index.bm25_component_cache[if index
                            .meta
                            .access_type
                            == AccessType::Mmap
                        {
                            get_document_length_compressed_mmap(
                                index,
                                field_id,
                                block_id,
                                docid & 0b11111111_11111111,
                            )
                        } else {
                            index.level_index[block_id].document_length_compressed_array[field_id]
                                [docid & 0b11111111_11111111]
                        }
                            as usize];
                    }

                    let tf = field.1 as f32;

                    let weight = index.indexed_schema_vec[field.0 as usize].boost;

                    bm25f += weight
                        * plo.idf
                        * ((tf * (K + 1.0)
                            / (tf + (K * (1.0 - B + (B * bm25_component_vec[field_id])))))
                            + SIGMA);
                }
            } else {
                for field in plo.field_vec_bigram1.iter() {
                    let field_id = field.0 as usize;
                    if bm25_component_vec[field_id] == 0.0 {
                        bm25_component_vec[field_id] = index.bm25_component_cache[if index
                            .meta
                            .access_type
                            == AccessType::Mmap
                        {
                            get_document_length_compressed_mmap(
                                index,
                                field_id,
                                block_id,
                                docid & 0b11111111_11111111,
                            )
                        } else {
                            index.level_index[block_id].document_length_compressed_array[field_id]
                                [docid & 0b11111111_11111111]
                        }
                            as usize];
                    }

                    let tf_bigram1 = field.1 as f32;

                    let weight = index.indexed_schema_vec[field.0 as usize].boost;

                    bm25f += weight
                        * plo.idf_bigram1
                        * ((tf_bigram1 * (K + 1.0)
                            / (tf_bigram1 + (K * (1.0 - B + (B * bm25_component_vec[field_id])))))
                            + SIGMA);
                }

                for field in plo.field_vec_bigram2.iter() {
                    let field_id = field.0 as usize;
                    if bm25_component_vec[field_id] == 0.0 {
                        bm25_component_vec[field_id] = index.bm25_component_cache[if index
                            .meta
                            .access_type
                            == AccessType::Mmap
                        {
                            get_document_length_compressed_mmap(
                                index,
                                field_id,
                                block_id,
                                docid & 0b11111111_11111111,
                            )
                        } else {
                            index.level_index[block_id].document_length_compressed_array[field_id]
                                [docid & 0b11111111_11111111]
                        }
                            as usize] as f32;
                    }

                    let tf_bigram2 = field.1 as f32;

                    let weight = index.indexed_schema_vec[field.0 as usize].boost;

                    bm25f += weight
                        * plo.idf_bigram2
                        * ((tf_bigram2 * (K + 1.0)
                            / (tf_bigram2 + (K * (1.0 - B + (B * bm25_component_vec[field_id])))))
                            + SIGMA);
                }
            }
        }
    }

    bm25f
}

#[inline(always)]
pub(crate) fn get_bm25f_multiterm_singlefield(
    index: &Index,
    docid: usize,
    query_list: &mut [PostingListObjectQuery],
) -> f32 {
    let mut bm25f = 0.0;
    let block_id = docid >> 16;

    let bm25_component = index.bm25_component_cache[if index.meta.access_type == AccessType::Mmap {
        get_document_length_compressed_mmap(index, 0, block_id, docid & 0b11111111_11111111)
    } else {
        index.level_index[block_id].document_length_compressed_array[0][docid & 0b11111111_11111111]
    } as usize];

    for plo in query_list.iter() {
        if !plo.bm25_flag {
            continue;
        }

        if !plo.is_bigram {
            let tf = plo.positions_count as f32;

            bm25f += plo.idf * ((tf * (K + 1.0) / (tf + bm25_component)) + SIGMA);
        } else {
            bm25f += plo.idf_bigram1
                * ((plo.tf_bigram1 as f32 * (K + 1.0) / (plo.tf_bigram1 as f32 + bm25_component))
                    + SIGMA)
                + plo.idf_bigram2
                    * ((plo.tf_bigram2 as f32 * (K + 1.0)
                        / (plo.tf_bigram2 as f32 + bm25_component))
                        + SIGMA);
        }
    }

    bm25f
}

#[inline(always)]
pub(crate) fn decode_positions_multiterm_multifield(
    index: &Index,
    plo: &mut PostingListObjectQuery,
    facet_filtered: bool,
    phrase_query: bool,
    all_terms_frequent: bool,
) -> bool {
    let mut field_vec: SmallVec<[(u16, usize); 2]> = SmallVec::new();

    let posting_pointer_size_sum = if plo.p_docid < plo.pointer_pivot_p_docid as usize {
        plo.p_docid as u32 * 2
    } else {
        (plo.p_docid as u32) * 3 - plo.pointer_pivot_p_docid as u32
    };

    let mut positions_pointer =
        (plo.rank_position_pointer_range + posting_pointer_size_sum) as usize;

    let rank_position_pointer = if plo.p_docid < plo.pointer_pivot_p_docid as usize {
        read_u16(plo.byte_array, positions_pointer) as u32
    } else {
        read_u32(plo.byte_array, positions_pointer)
    };

    if (rank_position_pointer
        & (if plo.p_docid < plo.pointer_pivot_p_docid as usize {
            0b10000000_00000000
        } else {
            0b10000000_00000000_00000000
        }))
        == 0
    {
        plo.is_embedded = false;

        let pointer_value = if plo.p_docid < plo.pointer_pivot_p_docid as usize {
            rank_position_pointer & 0b01111111_11111111
        } else {
            rank_position_pointer & 0b01111111_11111111_11111111
        } as usize;

        positions_pointer = plo.rank_position_pointer_range as usize - pointer_value;

        if plo.is_bigram {
            plo.field_vec_bigram1.clear();
            plo.field_vec_bigram2.clear();
            read_multifield_vec(
                index.indexed_field_vec.len(),
                index.indexed_field_id_bits,
                index.indexed_field_id_mask,
                index.longest_field_id,
                &mut plo.field_vec_bigram1,
                plo.byte_array,
                &mut positions_pointer,
            );
            read_multifield_vec(
                index.indexed_field_vec.len(),
                index.indexed_field_id_bits,
                index.indexed_field_id_mask,
                index.longest_field_id,
                &mut plo.field_vec_bigram2,
                plo.byte_array,
                &mut positions_pointer,
            );
        }
        read_multifield_vec(
            index.indexed_field_vec.len(),
            index.indexed_field_id_bits,
            index.indexed_field_id_mask,
            index.longest_field_id,
            &mut field_vec,
            plo.byte_array,
            &mut positions_pointer,
        );

        if SPEEDUP_FLAG
            && all_terms_frequent
            && !phrase_query
            && !facet_filtered
            && field_vec[0].1 < 10
        {
            return true;
        }
    } else {
        plo.is_embedded = true;

        if SPEEDUP_FLAG && all_terms_frequent && !phrase_query && !facet_filtered {
            return true;
        }

        let field_id;

        if plo.p_docid < plo.pointer_pivot_p_docid as usize {
            match (
                index.indexed_field_vec.len() == 1,
                rank_position_pointer >> 12,
            ) {
                (true, 0b1000..=0b1011) => {
                    if phrase_query {
                        plo.embedded_positions =
                            [rank_position_pointer & 0b00111111_11111111, 0, 0, 0];
                    };
                    field_vec.push((0, 1));
                }
                (true, 0b1100..=0b1111) => {
                    if phrase_query {
                        plo.embedded_positions = [
                            (rank_position_pointer >> 7) & 0b00000000_01111111,
                            rank_position_pointer & 0b00000000_01111111,
                            0,
                            0,
                        ];
                    };
                    field_vec.push((0, 2));
                }

                (false, 0b1100 | 0b1101) => {
                    if phrase_query {
                        plo.embedded_positions =
                            [rank_position_pointer & 0b00011111_11111111, 0, 0, 0];
                    };
                    field_id = index.longest_field_id as u16;
                    field_vec.push((field_id, 1));
                }
                (false, 0b1110 | 0b1111) => {
                    if phrase_query {
                        plo.embedded_positions = [
                            (rank_position_pointer >> 7) & 0b00000000_00111111,
                            rank_position_pointer & 0b00000000_01111111,
                            0,
                            0,
                        ];
                    };
                    field_id = index.longest_field_id as u16;
                    field_vec.push((field_id, 2));
                }

                (false, 0b1000) => {
                    let position_bits = 12 - index.indexed_field_id_bits;
                    field_id = ((rank_position_pointer >> position_bits)
                        & index.indexed_field_id_mask as u32) as u16;
                    field_vec.push((field_id, 1));
                    if phrase_query {
                        plo.embedded_positions = [
                            (rank_position_pointer & ((1 << position_bits) - 1)),
                            0,
                            0,
                            0,
                        ];
                    };
                }
                (false, 0b1001) => {
                    let position_bits = 12 - index.indexed_field_id_bits;
                    field_id = ((rank_position_pointer >> position_bits)
                        & index.indexed_field_id_mask as u32) as u16;
                    field_vec.push((field_id, 2));
                    if phrase_query {
                        let position_bits_1 = position_bits >> 1;
                        let position_bits_2 = position_bits - position_bits_1;
                        plo.embedded_positions = [
                            ((rank_position_pointer >> position_bits_2)
                                & ((1 << position_bits_1) - 1)),
                            (rank_position_pointer & ((1 << position_bits_2) - 1)),
                            0,
                            0,
                        ];
                    };
                }
                (false, 0b1010) => {
                    let position_bits = 12 - index.indexed_field_id_bits;
                    field_id = ((rank_position_pointer >> position_bits)
                        & index.indexed_field_id_mask as u32) as u16;
                    field_vec.push((field_id, 3));
                    if phrase_query {
                        let position_bits_1 = position_bits / 3;
                        let position_bits_2 = (position_bits - position_bits_1) >> 1;
                        let position_bits_3 = position_bits - position_bits_1 - position_bits_2;
                        plo.embedded_positions = [
                            ((rank_position_pointer >> (position_bits_2 + position_bits_3))
                                & ((1 << position_bits_1) - 1)),
                            ((rank_position_pointer >> position_bits_3)
                                & ((1 << position_bits_2) - 1)),
                            (rank_position_pointer & ((1 << position_bits_3) - 1)),
                            0,
                        ];
                    };
                }
                (false, 0b1011) => {
                    let position_bits =
                        12 - index.indexed_field_id_bits - index.indexed_field_id_bits;
                    field_id = ((rank_position_pointer
                        >> (position_bits + index.indexed_field_id_bits))
                        & index.indexed_field_id_mask as u32) as u16;
                    let field_id_2 = ((rank_position_pointer >> position_bits)
                        & index.indexed_field_id_mask as u32)
                        as u16;
                    field_vec.extend([(field_id, 1), (field_id_2, 1)]);
                    if phrase_query {
                        let position_bits_1 = position_bits >> 1;
                        let position_bits_2 = position_bits - position_bits_1;
                        plo.embedded_positions = [
                            ((rank_position_pointer >> position_bits_2)
                                & ((1 << position_bits_1) - 1)),
                            (rank_position_pointer & ((1 << position_bits_2) - 1)),
                            0,
                            0,
                        ];
                    };
                }

                (_, _) => {
                    if phrase_query {
                        println!("unsupported 2 byte pointer embedded");
                        plo.embedded_positions = [0, 0, 0, 0]
                    };
                }
            }
        } else {
            match (
                index.indexed_field_vec.len() == 1,
                (rank_position_pointer & 0b11111111_11111111_11111111) >> 19,
            ) {
                (true, 0b10000..=0b10011) => {
                    if phrase_query {
                        plo.embedded_positions = [
                            rank_position_pointer & 0b00011111_11111111_11111111,
                            0,
                            0,
                            0,
                        ];
                    };
                    field_vec.push((0, 1));
                }
                (true, 0b10100..=0b10111) => {
                    if phrase_query {
                        plo.embedded_positions = [
                            (rank_position_pointer >> 11) & 0b00000000_00000011_11111111,
                            rank_position_pointer & 0b00000000_00000111_11111111,
                            0,
                            0,
                        ];
                    };
                    field_vec.push((0, 2));
                }
                (true, 0b11000..=0b11011) => {
                    if phrase_query {
                        plo.embedded_positions = [
                            (rank_position_pointer >> 14) & 0b00000000_00000000_01111111,
                            (rank_position_pointer >> 7) & 0b00000000_00000000_01111111,
                            rank_position_pointer & 0b00000000_00000000_01111111,
                            0,
                        ];
                    };
                    field_vec.push((0, 3));
                }
                (true, 0b11100..=0b11111) => {
                    if phrase_query {
                        plo.embedded_positions = [
                            (rank_position_pointer >> 16) & 0b00000000_00000000_00011111,
                            (rank_position_pointer >> 11) & 0b00000000_00000000_00011111,
                            (rank_position_pointer >> 6) & 0b00000000_00000000_00011111,
                            rank_position_pointer & 0b00000000_00000000_00111111,
                        ];
                    };
                    field_vec.push((0, 4));
                }

                (false, 0b11000 | 0b11001) => {
                    field_id = index.longest_field_id as u16;
                    field_vec.push((field_id, 1));
                    if phrase_query {
                        plo.embedded_positions = [
                            rank_position_pointer & 0b00001111_11111111_11111111,
                            0,
                            0,
                            0,
                        ];
                    };
                }
                (false, 0b11010 | 0b11011) => {
                    field_id = index.longest_field_id as u16;
                    field_vec.push((field_id, 2));
                    if phrase_query {
                        plo.embedded_positions = [
                            (rank_position_pointer >> 10) & 0b00000000_00000011_11111111,
                            rank_position_pointer & 0b00000000_00000011_11111111,
                            0,
                            0,
                        ];
                    };
                }
                (false, 0b11100 | 0b11101) => {
                    field_id = index.longest_field_id as u16;
                    field_vec.push((field_id, 3));
                    if phrase_query {
                        plo.embedded_positions = [
                            (rank_position_pointer >> 14) & 0b00000000_00000000_00111111,
                            (rank_position_pointer >> 7) & 0b00000000_00000000_01111111,
                            rank_position_pointer & 0b00000000_00000000_01111111,
                            0,
                        ];
                    };
                }
                (false, 0b11110 | 0b11111) => {
                    field_id = index.longest_field_id as u16;
                    field_vec.push((field_id, 4));
                    if phrase_query {
                        plo.embedded_positions = [
                            (rank_position_pointer >> 15) & 0b00000000_00000000_00011111,
                            (rank_position_pointer >> 10) & 0b00000000_00000000_00011111,
                            (rank_position_pointer >> 5) & 0b00000000_00000000_00011111,
                            rank_position_pointer & 0b00000000_00000000_00011111,
                        ];
                    };
                }

                (false, 0b10000) => {
                    let position_bits = 19 - index.indexed_field_id_bits;
                    field_id = ((rank_position_pointer >> position_bits)
                        & index.indexed_field_id_mask as u32) as u16;
                    field_vec.push((field_id, 1));
                    if phrase_query {
                        plo.embedded_positions = [
                            (rank_position_pointer & ((1 << position_bits) - 1)),
                            0,
                            0,
                            0,
                        ];
                    };
                }

                (false, 0b10001) => {
                    let position_bits = 19 - index.indexed_field_id_bits;
                    field_id = ((rank_position_pointer >> position_bits)
                        & index.indexed_field_id_mask as u32) as u16;
                    field_vec.push((field_id, 2));
                    if phrase_query {
                        let position_bits_1 = position_bits >> 1;
                        let position_bits_2 = position_bits - position_bits_1;
                        plo.embedded_positions = [
                            ((rank_position_pointer >> position_bits_2)
                                & ((1 << position_bits_1) - 1)),
                            (rank_position_pointer & ((1 << position_bits_2) - 1)),
                            0,
                            0,
                        ];
                    };
                }
                (false, 0b10010) => {
                    let position_bits = 19 - index.indexed_field_id_bits;
                    field_id = ((rank_position_pointer >> position_bits)
                        & index.indexed_field_id_mask as u32) as u16;
                    field_vec.push((field_id, 3));
                    if phrase_query {
                        let position_bits_1 = position_bits / 3;
                        let position_bits_2 = (position_bits - position_bits_1) >> 1;
                        let position_bits_3 = position_bits - position_bits_1 - position_bits_2;
                        plo.embedded_positions = [
                            ((rank_position_pointer >> (position_bits_2 + position_bits_3))
                                & ((1 << position_bits_1) - 1)),
                            ((rank_position_pointer >> position_bits_3)
                                & ((1 << position_bits_2) - 1)),
                            (rank_position_pointer & ((1 << position_bits_3) - 1)),
                            0,
                        ];
                    };
                }
                (false, 0b10011) => {
                    let position_bits = 19 - index.indexed_field_id_bits;
                    field_id = ((rank_position_pointer >> position_bits)
                        & index.indexed_field_id_mask as u32) as u16;
                    field_vec.push((field_id, 4));
                    if phrase_query {
                        let position_bits_1 = position_bits >> 2;
                        let position_bits_2 = (position_bits - position_bits_1) / 3;
                        let position_bits_3 =
                            (position_bits - position_bits_1 - position_bits_2) >> 1;
                        let position_bits_4 =
                            position_bits - position_bits_1 - position_bits_2 - position_bits_3;
                        plo.embedded_positions = [
                            ((rank_position_pointer
                                >> (position_bits_2 + position_bits_3 + position_bits_4))
                                & ((1 << position_bits_1) - 1)),
                            ((rank_position_pointer >> (position_bits_3 + position_bits_4))
                                & ((1 << position_bits_2) - 1)),
                            ((rank_position_pointer >> position_bits_4)
                                & ((1 << position_bits_3) - 1)),
                            (rank_position_pointer & ((1 << position_bits_4) - 1)),
                        ];
                    };
                }
                (false, 0b10100) => {
                    let position_bits =
                        19 - index.indexed_field_id_bits - index.indexed_field_id_bits;
                    field_id = ((rank_position_pointer
                        >> (position_bits + index.indexed_field_id_bits))
                        & index.indexed_field_id_mask as u32) as u16;
                    let field_id_2 = ((rank_position_pointer >> position_bits)
                        & index.indexed_field_id_mask as u32)
                        as u16;
                    field_vec.extend([(field_id, 1), (field_id_2, 1)]);
                    if phrase_query {
                        let position_bits_1 = position_bits >> 1;
                        let position_bits_2 = position_bits - position_bits_1;
                        plo.embedded_positions = [
                            ((rank_position_pointer >> position_bits_2)
                                & ((1 << position_bits_1) - 1)),
                            (rank_position_pointer & ((1 << position_bits_2) - 1)),
                            0,
                            0,
                        ];
                    };
                }
                (false, 0b10101) => {
                    let position_bits =
                        19 - index.indexed_field_id_bits - index.indexed_field_id_bits;
                    field_id = ((rank_position_pointer
                        >> (position_bits + index.indexed_field_id_bits))
                        & index.indexed_field_id_mask as u32) as u16;
                    let field_id_2 = ((rank_position_pointer >> position_bits)
                        & index.indexed_field_id_mask as u32)
                        as u16;
                    field_vec.extend([(field_id, 1), (field_id_2, 2)]);
                    if phrase_query {
                        let position_bits_1 = position_bits / 3;
                        let position_bits_2 = (position_bits - position_bits_1) >> 1;
                        let position_bits_3 = position_bits - position_bits_1 - position_bits_2;
                        plo.embedded_positions = [
                            ((rank_position_pointer >> (position_bits_2 + position_bits_3))
                                & ((1 << position_bits_1) - 1)),
                            ((rank_position_pointer >> position_bits_3)
                                & ((1 << position_bits_2) - 1)),
                            (rank_position_pointer & ((1 << position_bits_3) - 1)),
                            0,
                        ];
                    };
                }
                (false, 0b10110) => {
                    let position_bits =
                        19 - index.indexed_field_id_bits - index.indexed_field_id_bits;
                    field_id = ((rank_position_pointer
                        >> (position_bits + index.indexed_field_id_bits))
                        & index.indexed_field_id_mask as u32) as u16;
                    let field_id_2 = ((rank_position_pointer >> position_bits)
                        & index.indexed_field_id_mask as u32)
                        as u16;
                    field_vec.extend([(field_id, 2), (field_id_2, 1)]);
                    if phrase_query {
                        let position_bits_1 = position_bits / 3;
                        let position_bits_2 = (position_bits - position_bits_1) >> 1;
                        let position_bits_3 = position_bits - position_bits_1 - position_bits_2;
                        plo.embedded_positions = [
                            ((rank_position_pointer >> (position_bits_2 + position_bits_3))
                                & ((1 << position_bits_1) - 1)),
                            ((rank_position_pointer >> position_bits_3)
                                & ((1 << position_bits_2) - 1)),
                            (rank_position_pointer & ((1 << position_bits_3) - 1)),
                            0,
                        ];
                    };
                }
                (false, 0b10111) => {
                    if phrase_query {
                        println!(
                            "unsupported 3 byte pointer embedded 111 {} {:032b}",
                            index.indexed_field_vec.len() == 1,
                            (rank_position_pointer & 0b11111111_11111111_11111111) >> 19
                        );
                        plo.embedded_positions = [0, 0, 0, 0]
                    };
                }

                (_, _) => {
                    if phrase_query {
                        println!(
                            "unsupported 3 byte pointer embedded {} {:032b}",
                            index.indexed_field_vec.len() == 1,
                            (rank_position_pointer & 0b11111111_11111111_11111111) >> 19
                        );
                        plo.embedded_positions = [0, 0, 0, 0]
                    };
                }
            }
        };
    }

    plo.positions_pointer = positions_pointer as u32;
    plo.positions_count = field_vec[0].1 as u32;
    plo.field_vec = field_vec;

    false
}

#[inline(always)]
pub(crate) fn decode_positions_multiterm_singlefield(
    plo: &mut PostingListObjectQuery,
    facet_filtered: bool,
    phrase_query: bool,
    all_terms_frequent: bool,
) -> bool {
    let mut positions_count = 0;

    let posting_pointer_size_sum = if plo.p_docid < plo.pointer_pivot_p_docid as usize {
        plo.p_docid as u32 * 2
    } else {
        (plo.p_docid as u32) * 3 - plo.pointer_pivot_p_docid as u32
    };

    let mut positions_pointer = plo.rank_position_pointer_range + posting_pointer_size_sum;

    let rank_position_pointer = if plo.p_docid < plo.pointer_pivot_p_docid as usize {
        read_u16(plo.byte_array, positions_pointer as usize) as u32
    } else {
        read_u32(plo.byte_array, positions_pointer as usize)
    };

    if (rank_position_pointer
        & (if plo.p_docid < plo.pointer_pivot_p_docid as usize {
            0b10000000_00000000
        } else {
            0b10000000_00000000_00000000
        }))
        == 0
    {
        plo.is_embedded = false;

        let pointer_value = if plo.p_docid < plo.pointer_pivot_p_docid as usize {
            rank_position_pointer & 0b01111111_11111111
        } else {
            rank_position_pointer & 0b01111111_11111111_11111111
        } as usize;

        positions_pointer = plo.rank_position_pointer_range - pointer_value as u32;

        if plo.is_bigram {
            read_singlefield_value(&mut plo.tf_bigram1, plo.byte_array, &mut positions_pointer);
            read_singlefield_value(&mut plo.tf_bigram2, plo.byte_array, &mut positions_pointer);
        }
        read_singlefield_value(&mut positions_count, plo.byte_array, &mut positions_pointer);

        if SPEEDUP_FLAG
            && all_terms_frequent
            && !phrase_query
            && !facet_filtered
            && positions_count < 10
        {
            return true;
        }
    } else {
        plo.is_embedded = true;

        if SPEEDUP_FLAG && all_terms_frequent && !phrase_query && !facet_filtered {
            return true;
        }

        if plo.p_docid < plo.pointer_pivot_p_docid as usize {
            match rank_position_pointer >> 14 {
                0b10 => {
                    if phrase_query {
                        plo.embedded_positions =
                            [rank_position_pointer & 0b00111111_11111111, 0, 0, 0];
                    };
                    positions_count = 1;
                }
                0b11 => {
                    if phrase_query {
                        plo.embedded_positions = [
                            (rank_position_pointer >> 7) & 0b00000000_01111111,
                            rank_position_pointer & 0b00000000_01111111,
                            0,
                            0,
                        ];
                    };
                    positions_count = 2;
                }

                _ => {
                    if phrase_query {
                        println!("unsupported 2 byte pointer embedded");
                        plo.embedded_positions = [0, 0, 0, 0]
                    };
                    positions_count = 0;
                }
            }
        } else {
            match (rank_position_pointer & 0b11111111_11111111_11111111) >> 21 {
                0b100 => {
                    if phrase_query {
                        plo.embedded_positions = [
                            rank_position_pointer & 0b00011111_11111111_11111111,
                            0,
                            0,
                            0,
                        ];
                    };
                    positions_count = 1;
                }
                0b101 => {
                    if phrase_query {
                        plo.embedded_positions = [
                            (rank_position_pointer >> 11) & 0b00000000_00000011_11111111,
                            rank_position_pointer & 0b00000000_00000111_11111111,
                            0,
                            0,
                        ];
                    };
                    positions_count = 2;
                }
                0b110 => {
                    if phrase_query {
                        plo.embedded_positions = [
                            (rank_position_pointer >> 14) & 0b00000000_00000000_01111111,
                            (rank_position_pointer >> 7) & 0b00000000_00000000_01111111,
                            rank_position_pointer & 0b00000000_00000000_01111111,
                            0,
                        ];
                    };
                    positions_count = 3;
                }
                0b111 => {
                    if phrase_query {
                        plo.embedded_positions = [
                            (rank_position_pointer >> 16) & 0b00000000_00000000_00011111,
                            (rank_position_pointer >> 11) & 0b00000000_00000000_00011111,
                            (rank_position_pointer >> 6) & 0b00000000_00000000_00011111,
                            rank_position_pointer & 0b00000000_00000000_00111111,
                        ];
                    };
                    positions_count = 4;
                }

                _ => {
                    if phrase_query {
                        println!("unsupported 3 byte pointer embedded");
                        plo.embedded_positions = [0, 0, 0, 0]
                    };
                    positions_count = 0;
                }
            }
        };
    }

    plo.positions_pointer = positions_pointer;
    plo.positions_count = positions_count;

    false
}

#[inline(always)]
pub(crate) fn read_multifield_vec(
    indexed_field_vec_len: usize,
    indexed_field_id_bits: usize,
    indexed_field_id_mask: usize,
    longest_field_id: usize,
    field_vec: &mut SmallVec<[(u16, usize); 2]>,
    byte_array: &[u8],
    positions_pointer: &mut usize,
) {
    let mut positions_count;
    if indexed_field_vec_len == 1 {
        positions_count = byte_array[*positions_pointer] as u32;
        *positions_pointer += 1;
        if (positions_count & STOP_BIT as u32) > 0 {
            positions_count &= 0b01111111
        } else {
            positions_count = (positions_count & 0b01111111) << 7;
            let positions_count2 = byte_array[*positions_pointer] as u32;
            *positions_pointer += 1;
            if (positions_count2 & STOP_BIT as u32) > 0 {
                positions_count |= positions_count2 & 0b01111111
            } else {
                positions_count = (positions_count << 7)
                    | (positions_count2 & 0b01111111) << 7
                    | (byte_array[*positions_pointer] & 0b01111111) as u32;
                *positions_pointer += 1;
            }
        };
        field_vec.push((0, positions_count as usize));
    } else if byte_array[*positions_pointer] & 0b01000000 > 0 {
        positions_count = byte_array[*positions_pointer] as u32;
        *positions_pointer += 1;
        if (positions_count & STOP_BIT as u32) > 0 {
            positions_count &= 0b00111111
        } else {
            positions_count = (positions_count & 0b00111111) << 7;
            let positions_count2 = byte_array[*positions_pointer] as u32;
            *positions_pointer += 1;
            if (positions_count2 & STOP_BIT as u32) > 0 {
                positions_count |= positions_count2 & 0b01111111
            } else {
                positions_count = (positions_count << 7)
                    | (positions_count2 & 0b01111111) << 7
                    | (byte_array[*positions_pointer] & 0b01111111) as u32;
                *positions_pointer += 1;
            }
        };
        field_vec.push((longest_field_id as u16, positions_count as usize));
    } else {
        let mut first = true;
        loop {
            let mut byte = byte_array[*positions_pointer];
            *positions_pointer += 1;

            let field_stop = {
                byte & if first {
                    FIELD_STOP_BIT_1
                } else {
                    FIELD_STOP_BIT_2
                } > 0
            };

            let mut field_id_position_count =
                byte as usize & if first { 0b0001_1111 } else { 0b0011_1111 };

            if (byte & STOP_BIT) == 0 {
                byte = byte_array[*positions_pointer];
                *positions_pointer += 1;

                field_id_position_count =
                    field_id_position_count << 7 | (byte as usize & 0b01111111);

                if (byte & STOP_BIT) == 0 {
                    byte = byte_array[*positions_pointer];
                    *positions_pointer += 1;

                    field_id_position_count =
                        field_id_position_count << 7 | (byte as usize & 0b01111111);
                }
            }

            let field_id = (field_id_position_count & indexed_field_id_mask) as u16;
            positions_count = (field_id_position_count >> indexed_field_id_bits) as u32;

            field_vec.push((field_id, positions_count as usize));

            first = false;

            if (byte & STOP_BIT) > 0 && field_stop {
                break;
            }
        }
    }
}

#[inline(always)]
pub(crate) fn decode_positions_singleterm_multifield(
    index: &Index,
    plo: &PostingListObjectSingle,
    field_vec: &mut SmallVec<[(u16, usize); 2]>,
    field_vec_bigram1: &mut SmallVec<[(u16, usize); 2]>,
    field_vec_bigram2: &mut SmallVec<[(u16, usize); 2]>,
) {
    let posting_pointer_size_sum = if (plo.p_docid as usize) < plo.pointer_pivot_p_docid as usize {
        plo.p_docid as u32 * 2
    } else {
        (plo.p_docid as u32) * 3 - plo.pointer_pivot_p_docid as u32
    };

    let mut positions_pointer =
        (plo.rank_position_pointer_range + posting_pointer_size_sum) as usize;

    let rank_position_pointer = if plo.p_docid < plo.pointer_pivot_p_docid as i32 {
        read_u16(plo.byte_array, positions_pointer) as u32
    } else {
        read_u32(plo.byte_array, positions_pointer)
    };

    if (rank_position_pointer
        & (if plo.p_docid < plo.pointer_pivot_p_docid as i32 {
            0b10000000_00000000
        } else {
            0b10000000_00000000_00000000
        }))
        == 0
    {
        let pointer_value = if plo.p_docid < plo.pointer_pivot_p_docid as i32 {
            rank_position_pointer & 0b01111111_11111111
        } else {
            rank_position_pointer & 0b01111111_11111111_11111111
        } as usize;

        positions_pointer = plo.rank_position_pointer_range as usize - pointer_value;

        if plo.is_bigram {
            read_multifield_vec(
                index.indexed_field_vec.len(),
                index.indexed_field_id_bits,
                index.indexed_field_id_mask,
                index.longest_field_id,
                field_vec_bigram1,
                plo.byte_array,
                &mut positions_pointer,
            );
            read_multifield_vec(
                index.indexed_field_vec.len(),
                index.indexed_field_id_bits,
                index.indexed_field_id_mask,
                index.longest_field_id,
                field_vec_bigram2,
                plo.byte_array,
                &mut positions_pointer,
            );
        }
        read_multifield_vec(
            index.indexed_field_vec.len(),
            index.indexed_field_id_bits,
            index.indexed_field_id_mask,
            index.longest_field_id,
            field_vec,
            plo.byte_array,
            &mut positions_pointer,
        );
    } else {
        let field_id;

        if plo.p_docid < plo.pointer_pivot_p_docid as i32 {
            match (
                index.indexed_field_vec.len() == 1,
                rank_position_pointer >> 12,
            ) {
                (true, 0b1000..=0b1011) => {
                    field_vec.push((0, 1));
                }
                (true, 0b1100..=0b1111) => {
                    field_vec.push((0, 2));
                }

                (false, 0b1100 | 0b1101) => {
                    field_id = index.longest_field_id as u16;
                    field_vec.push((field_id, 1));
                }
                (false, 0b1110 | 0b1111) => {
                    field_id = index.longest_field_id as u16;
                    field_vec.push((field_id, 2));
                }

                (false, 0b1000) => {
                    let position_bits = 12 - index.indexed_field_id_bits;
                    field_id = ((rank_position_pointer >> position_bits)
                        & index.indexed_field_id_mask as u32) as u16;
                    field_vec.push((field_id, 1));
                }
                (false, 0b1001) => {
                    let position_bits = 12 - index.indexed_field_id_bits;
                    field_id = ((rank_position_pointer >> position_bits)
                        & index.indexed_field_id_mask as u32) as u16;
                    field_vec.push((field_id, 2));
                }
                (false, 0b1010) => {
                    let position_bits = 12 - index.indexed_field_id_bits;
                    field_id = ((rank_position_pointer >> position_bits)
                        & index.indexed_field_id_mask as u32) as u16;
                    field_vec.push((field_id, 3));
                }
                (false, 0b1011) => {
                    let position_bits =
                        12 - index.indexed_field_id_bits - index.indexed_field_id_bits;
                    field_id = ((rank_position_pointer
                        >> (position_bits + index.indexed_field_id_bits))
                        & index.indexed_field_id_mask as u32) as u16;
                    let field_id_2 = ((rank_position_pointer >> position_bits)
                        & index.indexed_field_id_mask as u32)
                        as u16;
                    field_vec.extend([(field_id, 1), (field_id_2, 1)]);
                }

                (_, _) => {
                    println!(
                        "unsupported single 2 byte pointer embedded {} {:032b}",
                        index.indexed_field_vec.len() == 1,
                        rank_position_pointer >> 12
                    );
                }
            }
        } else {
            match (
                index.indexed_field_vec.len() == 1,
                (rank_position_pointer & 0b11111111_11111111_11111111) >> 19,
            ) {
                (true, 0b10000..=0b10011) => {
                    field_vec.push((0, 1));
                }
                (true, 0b10100..=0b10111) => {
                    field_vec.push((0, 2));
                }
                (true, 0b11000..=0b11011) => {
                    field_vec.push((0, 3));
                }
                (true, 0b11100..=0b11111) => {
                    field_vec.push((0, 4));
                }

                (false, 0b11000 | 0b11001) => {
                    field_id = index.longest_field_id as u16;
                    field_vec.push((field_id, 1));
                }
                (false, 0b11010 | 0b11011) => {
                    field_id = index.longest_field_id as u16;
                    field_vec.push((field_id, 2));
                }
                (false, 0b11100 | 0b11101) => {
                    field_id = index.longest_field_id as u16;
                    field_vec.push((field_id, 3));
                }
                (false, 0b11110 | 0b11111) => {
                    field_id = index.longest_field_id as u16;
                    field_vec.push((field_id, 4));
                }

                (false, 0b10000) => {
                    let position_bits = 19 - index.indexed_field_id_bits;
                    field_id = ((rank_position_pointer >> position_bits)
                        & index.indexed_field_id_mask as u32) as u16;
                    field_vec.push((field_id, 1));
                }

                (false, 0b10001) => {
                    let position_bits = 19 - index.indexed_field_id_bits;
                    field_id = ((rank_position_pointer >> position_bits)
                        & index.indexed_field_id_mask as u32) as u16;
                    field_vec.push((field_id, 2));
                }
                (false, 0b10010) => {
                    let position_bits = 19 - index.indexed_field_id_bits;
                    field_id = ((rank_position_pointer >> position_bits)
                        & index.indexed_field_id_mask as u32) as u16;
                    field_vec.push((field_id, 3));
                }
                (false, 0b10011) => {
                    let position_bits = 19 - index.indexed_field_id_bits;
                    field_id = ((rank_position_pointer >> position_bits)
                        & index.indexed_field_id_mask as u32) as u16;
                    field_vec.push((field_id, 4));
                }
                (false, 0b10100) => {
                    let position_bits =
                        19 - index.indexed_field_id_bits - index.indexed_field_id_bits;
                    field_id = ((rank_position_pointer
                        >> (position_bits + index.indexed_field_id_bits))
                        & index.indexed_field_id_mask as u32) as u16;
                    let field_id_2 = ((rank_position_pointer >> position_bits)
                        & index.indexed_field_id_mask as u32)
                        as u16;
                    field_vec.extend([(field_id, 1), (field_id_2, 1)]);
                }
                (false, 0b10101) => {
                    let position_bits =
                        19 - index.indexed_field_id_bits - index.indexed_field_id_bits;
                    field_id = ((rank_position_pointer
                        >> (position_bits + index.indexed_field_id_bits))
                        & index.indexed_field_id_mask as u32) as u16;
                    let field_id_2 = ((rank_position_pointer >> position_bits)
                        & index.indexed_field_id_mask as u32)
                        as u16;
                    field_vec.extend([(field_id, 1), (field_id_2, 2)]);
                }
                (false, 0b10110) => {
                    let position_bits =
                        19 - index.indexed_field_id_bits - index.indexed_field_id_bits;
                    field_id = ((rank_position_pointer
                        >> (position_bits + index.indexed_field_id_bits))
                        & index.indexed_field_id_mask as u32) as u16;
                    let field_id_2 = ((rank_position_pointer >> position_bits)
                        & index.indexed_field_id_mask as u32)
                        as u16;
                    field_vec.extend([(field_id, 2), (field_id_2, 1)]);
                }
                (false, 0b10111) => {
                    println!(
                        "unsupported single 3 byte pointer embedded 111 {} {:032b}",
                        index.indexed_field_vec.len() == 1,
                        (rank_position_pointer & 0b11111111_11111111_11111111) >> 19
                    );
                }

                (_, _) => {
                    println!(
                        "unsupported single 3 byte pointer embedded {} {:032b}",
                        index.indexed_field_vec.len() == 1,
                        (rank_position_pointer & 0b11111111_11111111_11111111) >> 19
                    );
                }
            }
        };
    }
}

#[inline(always)]
pub(crate) fn read_singlefield_value(
    positions_count: &mut u32,
    byte_array: &[u8],
    positions_pointer: &mut u32,
) {
    let mut positions_count_internal = byte_array[*positions_pointer as usize] as u32;
    *positions_pointer += 1;
    if (positions_count_internal & STOP_BIT as u32) > 0 {
        positions_count_internal &= 0b01111111
    } else {
        positions_count_internal = (positions_count_internal & 0b01111111) << 7;
        let positions_count2 = byte_array[*positions_pointer as usize] as u32;
        *positions_pointer += 1;
        if (positions_count2 & STOP_BIT as u32) > 0 {
            positions_count_internal |= positions_count2 & 0b01111111
        } else {
            positions_count_internal = (positions_count_internal << 7)
                | (positions_count2 & 0b01111111) << 7
                | (byte_array[*positions_pointer as usize] & 0b01111111) as u32;
        }
    };
    *positions_count = positions_count_internal;
}

#[inline(always)]
pub(crate) fn decode_positions_singleterm_singlefield(
    plo: &PostingListObjectSingle,
    tf_bigram1: &mut u32,
    tf_bigram2: &mut u32,
    positions_count: &mut u32,
) {
    let posting_pointer_size_sum = if (plo.p_docid as usize) < plo.pointer_pivot_p_docid as usize {
        plo.p_docid as u32 * 2
    } else {
        (plo.p_docid as u32) * 3 - plo.pointer_pivot_p_docid as u32
    };

    let mut positions_pointer = plo.rank_position_pointer_range + posting_pointer_size_sum;

    let rank_position_pointer = if plo.p_docid < plo.pointer_pivot_p_docid as i32 {
        read_u16(plo.byte_array, positions_pointer as usize) as u32
    } else {
        read_u32(plo.byte_array, positions_pointer as usize)
    };

    if (rank_position_pointer
        & (if plo.p_docid < plo.pointer_pivot_p_docid as i32 {
            0b10000000_00000000
        } else {
            0b10000000_00000000_00000000
        }))
        == 0
    {
        let pointer_value = if plo.p_docid < plo.pointer_pivot_p_docid as i32 {
            rank_position_pointer & 0b01111111_11111111
        } else {
            rank_position_pointer & 0b01111111_11111111_11111111
        } as usize;

        positions_pointer = plo.rank_position_pointer_range - pointer_value as u32;

        if plo.is_bigram {
            read_singlefield_value(tf_bigram1, plo.byte_array, &mut positions_pointer);
            read_singlefield_value(tf_bigram2, plo.byte_array, &mut positions_pointer);
        }
        read_singlefield_value(positions_count, plo.byte_array, &mut positions_pointer);
    } else if plo.p_docid < plo.pointer_pivot_p_docid as i32 {
        match rank_position_pointer >> 14 {
            0b10 => {
                *positions_count = 1;
            }
            0b11 => {
                *positions_count = 2;
            }

            _ => {
                println!(
                    "unsupported single 2 byte pointer embedded {:032b}",
                    rank_position_pointer >> 14
                );
            }
        }
    } else {
        match (rank_position_pointer & 0b11111111_11111111_11111111) >> 21 {
            0b100 => {
                *positions_count = 1;
            }
            0b101 => {
                *positions_count = 2;
            }
            0b110 => {
                *positions_count = 3;
            }
            0b111 => {
                *positions_count = 4;
            }

            _ => {
                println!(
                    "unsupported single 3 byte pointer embedded {:032b}",
                    (rank_position_pointer & 0b11111111_11111111_11111111) >> 21
                );
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
#[inline(always)]
pub(crate) fn decode_positions_commit(
    posting_pointer_size: u8,
    embed_flag: bool,
    byte_array: &[u8],
    pointer: usize,

    is_bigram: bool,
    indexed_field_vec_len: usize,
    indexed_field_id_bits: usize,
    indexed_field_id_mask: usize,
    longest_field_id: u16,

    field_vec: &mut SmallVec<[(u16, usize); 2]>,
    field_vec_bigram1: &mut SmallVec<[(u16, usize); 2]>,
    field_vec_bigram2: &mut SmallVec<[(u16, usize); 2]>,
) {
    let mut positions_pointer = pointer;

    if !embed_flag {
        if is_bigram {
            read_multifield_vec(
                indexed_field_vec_len,
                indexed_field_id_bits,
                indexed_field_id_mask,
                longest_field_id as usize,
                field_vec_bigram1,
                byte_array,
                &mut positions_pointer,
            );
            read_multifield_vec(
                indexed_field_vec_len,
                indexed_field_id_bits,
                indexed_field_id_mask,
                longest_field_id as usize,
                field_vec_bigram2,
                byte_array,
                &mut positions_pointer,
            );
        }
        read_multifield_vec(
            indexed_field_vec_len,
            indexed_field_id_bits,
            indexed_field_id_mask,
            longest_field_id as usize,
            field_vec,
            byte_array,
            &mut positions_pointer,
        );
    } else {
        let rank_position_pointer = if posting_pointer_size == 2 {
            read_u16(byte_array, positions_pointer) as u32
        } else {
            read_u32(byte_array, positions_pointer)
        };

        let field_id;

        if posting_pointer_size == 2 {
            match (indexed_field_vec_len == 1, rank_position_pointer >> 12) {
                (true, 0b1000..=0b1011) => {
                    field_vec.push((0, 1));
                }
                (true, 0b1100..=0b1111) => {
                    field_vec.push((0, 2));
                }

                (false, 0b1100 | 0b1101) => {
                    field_id = longest_field_id;
                    field_vec.push((field_id, 1));
                }
                (false, 0b1110 | 0b1111) => {
                    field_id = longest_field_id;
                    field_vec.push((field_id, 2));
                }

                (false, 0b1000) => {
                    let position_bits = 12 - indexed_field_id_bits;
                    field_id = ((rank_position_pointer >> position_bits)
                        & indexed_field_id_mask as u32) as u16;
                    field_vec.push((field_id, 1));
                }
                (false, 0b1001) => {
                    let position_bits = 12 - indexed_field_id_bits;
                    field_id = ((rank_position_pointer >> position_bits)
                        & indexed_field_id_mask as u32) as u16;
                    field_vec.push((field_id, 2));
                }
                (false, 0b1010) => {
                    let position_bits = 12 - indexed_field_id_bits;
                    field_id = ((rank_position_pointer >> position_bits)
                        & indexed_field_id_mask as u32) as u16;
                    field_vec.push((field_id, 3));
                }
                (false, 0b1011) => {
                    let position_bits = 12 - indexed_field_id_bits - indexed_field_id_bits;
                    field_id = ((rank_position_pointer >> (position_bits + indexed_field_id_bits))
                        & indexed_field_id_mask as u32) as u16;
                    let field_id_2 = ((rank_position_pointer >> position_bits)
                        & indexed_field_id_mask as u32) as u16;
                    field_vec.extend([(field_id, 1), (field_id_2, 1)]);
                }

                (_, _) => {
                    println!(
                        "unsupported single 2 byte pointer embedded {} {:032b}",
                        indexed_field_vec_len == 1,
                        rank_position_pointer >> 12
                    );
                }
            }
        } else {
            match (
                indexed_field_vec_len == 1,
                (rank_position_pointer & 0b11111111_11111111_11111111) >> 19,
            ) {
                (true, 0b10000..=0b10011) => {
                    field_vec.push((0, 1));
                }
                (true, 0b10100..=0b10111) => {
                    field_vec.push((0, 2));
                }
                (true, 0b11000..=0b11011) => {
                    field_vec.push((0, 3));
                }
                (true, 0b11100..=0b11111) => {
                    field_vec.push((0, 4));
                }

                (false, 0b11000 | 0b11001) => {
                    field_id = longest_field_id;
                    field_vec.push((field_id, 1));
                }
                (false, 0b11010 | 0b11011) => {
                    field_id = longest_field_id;
                    field_vec.push((field_id, 2));
                }
                (false, 0b11100 | 0b11101) => {
                    field_id = longest_field_id;
                    field_vec.push((field_id, 3));
                }
                (false, 0b11110 | 0b11111) => {
                    field_id = longest_field_id;
                    field_vec.push((field_id, 4));
                }

                (false, 0b10000) => {
                    let position_bits = 19 - indexed_field_id_bits;
                    field_id = ((rank_position_pointer >> position_bits)
                        & indexed_field_id_mask as u32) as u16;
                    field_vec.push((field_id, 1));
                }

                (false, 0b10001) => {
                    let position_bits = 19 - indexed_field_id_bits;
                    field_id = ((rank_position_pointer >> position_bits)
                        & indexed_field_id_mask as u32) as u16;
                    field_vec.push((field_id, 2));
                }
                (false, 0b10010) => {
                    let position_bits = 19 - indexed_field_id_bits;
                    field_id = ((rank_position_pointer >> position_bits)
                        & indexed_field_id_mask as u32) as u16;
                    field_vec.push((field_id, 3));
                }
                (false, 0b10011) => {
                    let position_bits = 19 - indexed_field_id_bits;
                    field_id = ((rank_position_pointer >> position_bits)
                        & indexed_field_id_mask as u32) as u16;
                    field_vec.push((field_id, 4));
                }
                (false, 0b10100) => {
                    let position_bits = 19 - indexed_field_id_bits - indexed_field_id_bits;
                    field_id = ((rank_position_pointer >> (position_bits + indexed_field_id_bits))
                        & indexed_field_id_mask as u32) as u16;
                    let field_id_2 = ((rank_position_pointer >> position_bits)
                        & indexed_field_id_mask as u32) as u16;
                    field_vec.extend([(field_id, 1), (field_id_2, 1)]);
                }
                (false, 0b10101) => {
                    let position_bits = 19 - indexed_field_id_bits - indexed_field_id_bits;
                    field_id = ((rank_position_pointer >> (position_bits + indexed_field_id_bits))
                        & indexed_field_id_mask as u32) as u16;
                    let field_id_2 = ((rank_position_pointer >> position_bits)
                        & indexed_field_id_mask as u32) as u16;
                    field_vec.extend([(field_id, 1), (field_id_2, 2)]);
                }
                (false, 0b10110) => {
                    let position_bits = 19 - indexed_field_id_bits - indexed_field_id_bits;
                    field_id = ((rank_position_pointer >> (position_bits + indexed_field_id_bits))
                        & indexed_field_id_mask as u32) as u16;
                    let field_id_2 = ((rank_position_pointer >> position_bits)
                        & indexed_field_id_mask as u32) as u16;
                    field_vec.extend([(field_id, 2), (field_id_2, 1)]);
                }
                (false, 0b10111) => {
                    println!(
                        "unsupported single 3 byte pointer embedded 111 {} {:032b}",
                        indexed_field_vec_len == 1,
                        (rank_position_pointer & 0b11111111_11111111_11111111) >> 19
                    );
                }

                (_, _) => {
                    println!(
                        "unsupported single 3 byte pointer embedded {} {:032b}",
                        indexed_field_vec_len == 1,
                        (rank_position_pointer & 0b11111111_11111111_11111111) >> 19
                    );
                }
            }
        };
    }
}

/// Post processing after AND intersection candidates have been found
/// Phrase intersection
/// BM25 ranking vs. seekstorm ranking (implicit phrase search, term proximity, field type boost, source reputation)
/// BM25 is default baseline in IR academics, but exhibits inferior relevance for practical use
#[allow(clippy::too_many_arguments)]
#[inline(always)]
pub(crate) fn add_result_multiterm_multifield(
    index: &Index,
    docid: usize,
    result_count: &mut i32,
    search_result: &mut SearchResult,
    top_k: usize,
    result_type: &ResultType,
    field_filter_set: &AHashSet<u16>,
    facet_filter: &[FilterSparse],
    non_unique_query_list: &mut [NonUniquePostingListObjectQuery],
    query_list: &mut [PostingListObjectQuery],
    not_query_list: &mut [PostingListObjectQuery],
    phrase_query: bool,
    block_score: f32,
    all_terms_frequent: bool,
) {
    if index.indexed_field_vec.len() == 1 {
        add_result_multiterm_singlefield(
            index,
            docid,
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
        return;
    }

    if !index.delete_hashset.is_empty() && index.delete_hashset.contains(&docid) {
        return;
    }

    let local_docid = docid & 0b11111111_11111111;
    for plo in not_query_list.iter_mut() {
        if !plo.bm25_flag {
            continue;
        }

        match &plo.compression_type {
            CompressionType::Array => {
                while plo.p_docid < plo.p_docid_count
                    && (plo.p_docid == 0 || (plo.docid as usize) < local_docid)
                {
                    plo.docid = read_u16(
                        plo.byte_array,
                        plo.compressed_doc_id_range + (plo.p_docid << 1),
                    ) as i32;
                    plo.p_docid += 1;
                }
                if (plo.docid as usize) == local_docid {
                    return;
                }
            }
            CompressionType::Bitmap => {
                if (plo.byte_array[plo.compressed_doc_id_range + (local_docid >> 3)]
                    & (1 << (local_docid & 7)))
                    > 0
                {
                    return;
                }
            }
            CompressionType::Rle => {
                if local_docid >= plo.docid as usize && local_docid <= plo.run_end as usize {
                    return;
                } else {
                    while (plo.p_run_sum as usize) < plo.p_docid_count
                        && local_docid > plo.run_end as usize
                    {
                        let startdocid = read_u16(
                            plo.byte_array,
                            plo.compressed_doc_id_range + plo.p_run as usize,
                        );
                        let runlength = read_u16(
                            plo.byte_array,
                            plo.compressed_doc_id_range + plo.p_run as usize + 2,
                        );
                        plo.docid = startdocid as i32;
                        plo.run_end = (startdocid + runlength) as i32;
                        plo.p_run_sum += runlength as i32;
                        plo.p_run += 4;

                        if local_docid >= startdocid as usize && local_docid <= plo.run_end as usize
                        {
                            return;
                        }
                    }
                }
            }
            _ => {}
        }
    }

    if !facet_filter.is_empty() && is_facet_filter(index, facet_filter, docid) {
        return;
    };

    match *result_type {
        ResultType::Count => {
            if !phrase_query && field_filter_set.is_empty() {
                facet_count(index, search_result, docid);

                *result_count += 1;
                return;
            }
        }
        ResultType::Topk => {
            if SPEEDUP_FLAG
                && search_result.topk_candidates.result_sort.is_empty()
                && search_result.topk_candidates.current_heap_size >= top_k
                && block_score <= search_result.topk_candidates._elements[0].score
            {
                return;
            }
        }
        ResultType::TopkCount => {
            if SPEEDUP_FLAG
                && search_result.topk_candidates.result_sort.is_empty()
                && !phrase_query
                && field_filter_set.is_empty()
                && search_result.topk_candidates.current_heap_size >= top_k
                && block_score <= search_result.topk_candidates._elements[0].score
            {
                facet_count(index, search_result, docid);

                *result_count += 1;
                return;
            }
        }
    }

    let mut bm25: f32 = 0.0;

    for plo in query_list.iter_mut() {
        if !plo.bm25_flag {
            continue;
        }

        if decode_positions_multiterm_multifield(
            index,
            plo,
            !facet_filter.is_empty(),
            phrase_query,
            all_terms_frequent && field_filter_set.is_empty(),
        ) {
            facet_count(index, search_result, docid);

            *result_count += 1;
            return;
        }

        if !field_filter_set.is_empty()
            && plo.field_vec.len() + field_filter_set.len() <= index.indexed_field_vec.len()
        {
            let mut match_flag = false;
            for field in plo.field_vec.iter() {
                if field_filter_set.contains(&field.0) {
                    match_flag = true;
                }
            }
            if !match_flag {
                return;
            }
        }
    }

    if result_type == &ResultType::Topk && phrase_query {
        bm25 = get_bm25f_multiterm_multifield(index, docid, query_list);

        if SPEEDUP_FLAG
            && search_result.topk_candidates.result_sort.is_empty()
            && search_result.topk_candidates.current_heap_size >= top_k
            && bm25 <= search_result.topk_candidates._elements[0].score
        {
            return;
        }
    }

    if phrase_query {
        let len = query_list.len();
        let mut index_transpose = vec![0; len];
        for i in 0..len {
            index_transpose[query_list[i].term_index_unique] = i;
        }

        let mut phrasematch_count = 0;
        if index.indexed_field_vec.len() == 1 {
            for plo in non_unique_query_list.iter_mut() {
                plo.p_pos = 0;
                let item = &query_list[index_transpose[plo.term_index_unique]];
                plo.positions_pointer = item.positions_pointer as usize;
                plo.positions_count = item.positions_count;

                plo.is_embedded = item.is_embedded;
                plo.embedded_positions = item.embedded_positions;

                plo.pos = get_next_position_singlefield(plo);
            }

            non_unique_query_list.sort_unstable_by(|x, y| {
                x.positions_count.partial_cmp(&y.positions_count).unwrap()
            });

            let t1 = 0;
            let mut t2 = 1;
            let mut pos1 = non_unique_query_list[t1].pos;
            let mut pos2 = non_unique_query_list[t2].pos;

            loop {
                match (pos1 + non_unique_query_list[t2].term_index_nonunique as u32)
                    .cmp(&(pos2 + non_unique_query_list[t1].term_index_nonunique as u32))
                {
                    Ordering::Less => {
                        if t2 > 1 {
                            t2 = 1;
                            pos2 = non_unique_query_list[t2].pos;
                        }

                        non_unique_query_list[t1].p_pos += 1;
                        if non_unique_query_list[t1].p_pos
                            == non_unique_query_list[t1].positions_count as i32
                        {
                            break;
                        }
                        pos1 += get_next_position_singlefield(&mut non_unique_query_list[t1]) + 1;
                    }

                    Ordering::Greater => {
                        non_unique_query_list[t2].p_pos += 1;
                        if non_unique_query_list[t2].p_pos
                            == non_unique_query_list[t2].positions_count as i32
                        {
                            break;
                        }
                        pos2 = non_unique_query_list[t2].pos
                            + get_next_position_singlefield(&mut non_unique_query_list[t2])
                            + 1;
                        non_unique_query_list[t2].pos = pos2;
                    }
                    Ordering::Equal => {
                        if t2 + 1 < non_unique_query_list.len() {
                            t2 += 1;
                            pos2 = non_unique_query_list[t2].pos;
                            continue;
                        }

                        phrasematch_count += 1;
                        if phrasematch_count >= 1 {
                            break;
                        }

                        t2 = 1;
                        non_unique_query_list[t1].p_pos += 1;
                        if non_unique_query_list[t1].p_pos
                            == non_unique_query_list[t1].positions_count as i32
                        {
                            break;
                        }
                        non_unique_query_list[t2].p_pos += 1;
                        if non_unique_query_list[t2].p_pos
                            == non_unique_query_list[t2].positions_count as i32
                        {
                            break;
                        }

                        pos1 += get_next_position_singlefield(&mut non_unique_query_list[t1]) + 1;
                        pos2 = non_unique_query_list[t2].pos
                            + get_next_position_singlefield(&mut non_unique_query_list[t2])
                            + 1;
                        non_unique_query_list[t2].pos = pos2;
                    }
                }
            }
        } else {
            for plo in non_unique_query_list.iter_mut() {
                let item = &query_list[index_transpose[plo.term_index_unique]];
                plo.positions_pointer = item.positions_pointer as usize;
                plo.is_embedded = item.is_embedded;
                plo.embedded_positions = item.embedded_positions;
                plo.field_vec.clone_from(&item.field_vec);
                plo.p_pos = 0;
                plo.positions_count = item.positions_count;
                plo.p_field = 0;
            }

            'main: for i in 0..index.indexed_field_vec.len() as u16 {
                for plo in non_unique_query_list.iter_mut() {
                    while plo.field_vec[plo.p_field].0 < i {
                        if !plo.is_embedded {
                            for _ in plo.p_pos..plo.field_vec[plo.p_field].1 as i32 {
                                get_next_position_multifield(plo);
                            }
                        }
                        if plo.p_field < plo.field_vec.len() - 1 {
                            plo.p_field += 1;
                            plo.p_pos = 0;
                        } else {
                            break 'main;
                        }
                    }
                    if plo.field_vec[plo.p_field].0 > i {
                        continue 'main;
                    }
                }

                for plo in non_unique_query_list.iter_mut() {
                    plo.p_pos = 0;
                    plo.positions_count = plo.field_vec[plo.p_field].1 as u32;
                    plo.pos = get_next_position_multifield(plo);
                }

                if !field_filter_set.is_empty() && !field_filter_set.contains(&i) {
                    continue;
                }

                non_unique_query_list.sort_unstable_by(|x, y| {
                    x.positions_count.partial_cmp(&y.positions_count).unwrap()
                });

                let t1 = 0;
                let mut t2 = 1;
                let mut pos1 = non_unique_query_list[t1].pos;
                let mut pos2 = non_unique_query_list[t2].pos;

                loop {
                    match (pos1 + non_unique_query_list[t2].term_index_nonunique as u32)
                        .cmp(&(pos2 + non_unique_query_list[t1].term_index_nonunique as u32))
                    {
                        Ordering::Less => {
                            if t2 > 1 {
                                t2 = 1;
                                pos2 = non_unique_query_list[t2].pos;
                            }

                            non_unique_query_list[t1].p_pos += 1;
                            if non_unique_query_list[t1].p_pos
                                == non_unique_query_list[t1].positions_count as i32
                            {
                                if (i as usize) < index.indexed_field_vec.len() - 1 {
                                    for item in non_unique_query_list.iter_mut().skip(1) {
                                        item.p_pos += 1
                                    }
                                }
                                break;
                            }
                            pos1 +=
                                get_next_position_multifield(&mut non_unique_query_list[t1]) + 1;
                        }
                        Ordering::Greater => {
                            non_unique_query_list[t2].p_pos += 1;
                            if non_unique_query_list[t2].p_pos
                                == non_unique_query_list[t2].positions_count as i32
                            {
                                if (i as usize) < index.indexed_field_vec.len() - 1 {
                                    for (j, item) in non_unique_query_list.iter_mut().enumerate() {
                                        if j != t2 {
                                            item.p_pos += 1
                                        }
                                    }
                                }
                                break;
                            }
                            pos2 = non_unique_query_list[t2].pos
                                + get_next_position_multifield(&mut non_unique_query_list[t2])
                                + 1;
                            non_unique_query_list[t2].pos = pos2;
                        }
                        Ordering::Equal => {
                            if t2 + 1 < non_unique_query_list.len() {
                                t2 += 1;
                                pos2 = non_unique_query_list[t2].pos;
                                continue;
                            }

                            phrasematch_count += 1;
                            if phrasematch_count >= 1 {
                                break 'main;
                            }

                            t2 = 1;
                            non_unique_query_list[t1].p_pos += 1;
                            if non_unique_query_list[t1].p_pos
                                == non_unique_query_list[t1].positions_count as i32
                            {
                                if (i as usize) < index.indexed_field_vec.len() - 1 {
                                    for item in non_unique_query_list.iter_mut().skip(1) {
                                        item.p_pos += 1
                                    }
                                }
                                break;
                            }
                            non_unique_query_list[t2].p_pos += 1;
                            if non_unique_query_list[t2].p_pos
                                == non_unique_query_list[t2].positions_count as i32
                            {
                                if (i as usize) < index.indexed_field_vec.len() - 1 {
                                    for item in non_unique_query_list.iter_mut().skip(2) {
                                        item.p_pos += 1
                                    }
                                }
                                break;
                            }

                            pos1 +=
                                get_next_position_multifield(&mut non_unique_query_list[t1]) + 1;
                            pos2 = non_unique_query_list[t2].pos
                                + get_next_position_multifield(&mut non_unique_query_list[t2])
                                + 1;
                            non_unique_query_list[t2].pos = pos2;
                        }
                    }
                }
            }
        }

        if phrase_query && (phrasematch_count == 0) {
            return;
        }
    }

    facet_count(index, search_result, docid);

    *result_count += 1;
    if result_type == &ResultType::Count {
        return;
    }

    if result_type != &ResultType::Topk || !phrase_query {
        bm25 = get_bm25f_multiterm_multifield(index, docid, query_list);
    }

    search_result.topk_candidates.add_topk(
        min_heap::Result {
            doc_id: docid,
            score: bm25,
        },
        top_k,
    );
}

#[allow(clippy::too_many_arguments)]
#[inline(always)]
pub(crate) fn add_result_multiterm_singlefield(
    index: &Index,
    docid: usize,
    result_count: &mut i32,
    search_result: &mut SearchResult,
    top_k: usize,
    result_type: &ResultType,
    field_filter_set: &AHashSet<u16>,
    facet_filter: &[FilterSparse],
    non_unique_query_list: &mut [NonUniquePostingListObjectQuery],
    query_list: &mut [PostingListObjectQuery],
    not_query_list: &mut [PostingListObjectQuery],
    phrase_query: bool,

    block_score: f32,
    all_terms_frequent: bool,
) {
    if !index.delete_hashset.is_empty() && index.delete_hashset.contains(&docid) {
        return;
    }

    let local_docid = docid & 0b11111111_11111111;
    for plo in not_query_list.iter_mut() {
        if !plo.bm25_flag {
            continue;
        }

        match &plo.compression_type {
            CompressionType::Array => {
                while plo.p_docid < plo.p_docid_count
                    && (plo.p_docid == 0 || (plo.docid as usize) < local_docid)
                {
                    plo.docid = read_u16(
                        plo.byte_array,
                        plo.compressed_doc_id_range + (plo.p_docid << 1),
                    ) as i32;
                    plo.p_docid += 1;
                }
                if (plo.docid as usize) == local_docid {
                    return;
                }
            }
            CompressionType::Bitmap => {
                if (plo.byte_array[plo.compressed_doc_id_range + (local_docid >> 3)]
                    & (1 << (local_docid & 7)))
                    > 0
                {
                    return;
                }
            }
            CompressionType::Rle => {
                if local_docid >= plo.docid as usize && local_docid <= plo.run_end as usize {
                    return;
                } else {
                    while (plo.p_run_sum as usize) < plo.p_docid_count
                        && local_docid > plo.run_end as usize
                    {
                        let startdocid = read_u16(
                            plo.byte_array,
                            plo.compressed_doc_id_range + plo.p_run as usize,
                        );
                        let runlength = read_u16(
                            plo.byte_array,
                            plo.compressed_doc_id_range + plo.p_run as usize + 2,
                        );
                        plo.docid = startdocid as i32;
                        plo.run_end = (startdocid + runlength) as i32;
                        plo.p_run_sum += runlength as i32;
                        plo.p_run += 4;

                        if local_docid >= startdocid as usize && local_docid <= plo.run_end as usize
                        {
                            return;
                        }
                    }
                }
            }
            _ => {}
        }
    }

    if !facet_filter.is_empty() && is_facet_filter(index, facet_filter, docid) {
        return;
    };

    match *result_type {
        ResultType::Count => {
            if !phrase_query && field_filter_set.is_empty() {
                facet_count(index, search_result, docid);

                *result_count += 1;
                return;
            }
        }
        ResultType::Topk => {
            if SPEEDUP_FLAG
                && search_result.topk_candidates.result_sort.is_empty()
                && search_result.topk_candidates.current_heap_size >= top_k
                && block_score <= search_result.topk_candidates._elements[0].score
            {
                return;
            }
        }
        ResultType::TopkCount => {
            if SPEEDUP_FLAG
                && search_result.topk_candidates.result_sort.is_empty()
                && !phrase_query
                && field_filter_set.is_empty()
                && search_result.topk_candidates.current_heap_size >= top_k
                && block_score <= search_result.topk_candidates._elements[0].score
            {
                facet_count(index, search_result, docid);

                *result_count += 1;
                return;
            }
        }
    }

    let mut bm25: f32 = 0.0;

    for plo in query_list.iter_mut() {
        if !plo.bm25_flag {
            continue;
        }

        if decode_positions_multiterm_singlefield(
            plo,
            !facet_filter.is_empty(),
            phrase_query,
            all_terms_frequent && field_filter_set.is_empty(),
        ) {
            facet_count(index, search_result, docid);

            *result_count += 1;
            return;
        }

        if !field_filter_set.is_empty()
            && plo.field_vec.len() + field_filter_set.len() <= index.indexed_field_vec.len()
        {
            let mut match_flag = false;
            for field in plo.field_vec.iter() {
                if field_filter_set.contains(&field.0) {
                    match_flag = true;
                }
            }
            if !match_flag {
                return;
            }
        }
    }

    if result_type == &ResultType::Topk && phrase_query {
        bm25 = get_bm25f_multiterm_singlefield(index, docid, query_list);

        if SPEEDUP_FLAG
            && search_result.topk_candidates.result_sort.is_empty()
            && search_result.topk_candidates.current_heap_size >= top_k
            && bm25 <= search_result.topk_candidates._elements[0].score
        {
            return;
        }
    }

    if phrase_query {
        let len = query_list.len();
        let mut index_transpose = vec![0; len];
        for i in 0..len {
            index_transpose[query_list[i].term_index_unique] = i;
        }

        let mut phrasematch_count = 0;

        for plo in non_unique_query_list.iter_mut() {
            plo.p_pos = 0;
            let item = &query_list[index_transpose[plo.term_index_unique]];
            plo.positions_pointer = item.positions_pointer as usize;
            plo.positions_count = item.positions_count;

            plo.is_embedded = item.is_embedded;
            plo.embedded_positions = item.embedded_positions;

            plo.pos = get_next_position_singlefield(plo);
        }

        non_unique_query_list
            .sort_unstable_by(|x, y| x.positions_count.partial_cmp(&y.positions_count).unwrap());

        let t1 = 0;
        let mut t2 = 1;
        let mut pos1 = non_unique_query_list[t1].pos;
        let mut pos2 = non_unique_query_list[t2].pos;

        loop {
            match (pos1 + non_unique_query_list[t2].term_index_nonunique as u32)
                .cmp(&(pos2 + non_unique_query_list[t1].term_index_nonunique as u32))
            {
                Ordering::Less => {
                    if t2 > 1 {
                        t2 = 1;
                        pos2 = non_unique_query_list[t2].pos;
                    }

                    non_unique_query_list[t1].p_pos += 1;
                    if non_unique_query_list[t1].p_pos
                        == non_unique_query_list[t1].positions_count as i32
                    {
                        break;
                    }
                    pos1 += get_next_position_singlefield(&mut non_unique_query_list[t1]) + 1;
                }
                Ordering::Greater => {
                    non_unique_query_list[t2].p_pos += 1;
                    if non_unique_query_list[t2].p_pos
                        == non_unique_query_list[t2].positions_count as i32
                    {
                        break;
                    }
                    pos2 = non_unique_query_list[t2].pos
                        + get_next_position_singlefield(&mut non_unique_query_list[t2])
                        + 1;
                    non_unique_query_list[t2].pos = pos2;
                }
                Ordering::Equal => {
                    if t2 + 1 < non_unique_query_list.len() {
                        t2 += 1;
                        pos2 = non_unique_query_list[t2].pos;
                        continue;
                    }

                    phrasematch_count += 1;
                    if phrasematch_count >= 1 {
                        break;
                    }

                    t2 = 1;
                    non_unique_query_list[t1].p_pos += 1;
                    if non_unique_query_list[t1].p_pos
                        == non_unique_query_list[t1].positions_count as i32
                    {
                        break;
                    }
                    non_unique_query_list[t2].p_pos += 1;
                    if non_unique_query_list[t2].p_pos
                        == non_unique_query_list[t2].positions_count as i32
                    {
                        break;
                    }

                    pos1 += get_next_position_singlefield(&mut non_unique_query_list[t1]) + 1;
                    pos2 = non_unique_query_list[t2].pos
                        + get_next_position_singlefield(&mut non_unique_query_list[t2])
                        + 1;
                    non_unique_query_list[t2].pos = pos2;
                }
            }
        }

        if phrase_query && (phrasematch_count == 0) {
            return;
        }
    }

    facet_count(index, search_result, docid);

    *result_count += 1;
    if result_type == &ResultType::Count {
        return;
    }

    if result_type != &ResultType::Topk || !phrase_query {
        bm25 = get_bm25f_multiterm_singlefield(index, docid, query_list);
    }

    search_result.topk_candidates.add_topk(
        min_heap::Result {
            doc_id: docid,
            score: bm25,
        },
        top_k,
    );
}
