use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use ahash::{AHashMap, AHashSet};
use smallvec::SmallVec;

use crate::{
    add_result::{B, K, SIGMA, facet_count, is_facet_filter, read_multifield_vec},
    index::{
        AccessType, DOCUMENT_LENGTH_COMPRESSION, DUMMY_VEC_8, NgramType,
        NonUniquePostingListObjectQuery, NonUniqueTermObject, PostingListObjectQuery, STOP_BIT,
        Shard, SimilarityType, TermObject, hash32, hash64,
    },
    min_heap,
    search::{FilterSparse, QueryType, ResultType, SearchResult, decode_posting_list_counts},
    utils::{read_u16, read_u16_ref, read_u32, read_u32_ref},
};

#[inline(always)]
pub(crate) fn get_next_position_uncommitted(
    shard: &Shard,
    plo: &mut NonUniquePostingListObjectQuery,
) -> u32 {
    if plo.is_embedded {
        return plo.embedded_positions[if plo.p_field == 0 {
            plo.p_pos as usize
        } else {
            plo.field_vec[plo.p_field - 1].1 + plo.p_pos as usize
        }];
    }

    if (shard.postings_buffer[plo.positions_pointer] & STOP_BIT) != 0 {
        let position = (shard.postings_buffer[plo.positions_pointer] & 0b0111_1111) as u32;
        plo.positions_pointer += 1;
        position
    } else if (shard.postings_buffer[plo.positions_pointer + 1] & STOP_BIT) != 0 {
        let position = ((shard.postings_buffer[plo.positions_pointer] as u32) << 7)
            | (shard.postings_buffer[plo.positions_pointer + 1] & 0b0111_1111) as u32;
        plo.positions_pointer += 2;
        position
    } else {
        let position = ((shard.postings_buffer[plo.positions_pointer] as u32) << 13)
            | ((shard.postings_buffer[plo.positions_pointer + 1] as u32) << 7)
            | (shard.postings_buffer[plo.positions_pointer + 2] & 0b0111_1111) as u32;
        plo.positions_pointer += 3;
        position
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn add_result_singleterm_uncommitted(
    shard: &Shard,
    docid: usize,
    result_count: &mut i32,
    search_result: &mut SearchResult,
    top_k: usize,
    result_type: &ResultType,
    field_filter_set: &AHashSet<u16>,
    facet_filter: &[FilterSparse],

    plo_single: &mut PostingListObjectQuery,
    not_query_list: &mut [PostingListObjectQuery],
) {
    if !shard.delete_hashset.is_empty() && shard.delete_hashset.contains(&docid) {
        return;
    }

    for plo in not_query_list.iter_mut() {
        if !plo.bm25_flag {
            continue;
        }

        let local_docid = docid & 0b11111111_11111111;

        while plo.p_docid < plo.p_docid_count
            && (plo.p_docid == 0 || (plo.docid as usize) < local_docid)
        {
            let mut read_pointer = plo.posting_pointer;

            plo.posting_pointer = read_u32_ref(&shard.postings_buffer, &mut read_pointer) as usize;
            plo.docid = read_u16_ref(&shard.postings_buffer, &mut read_pointer) as i32;

            plo.p_docid += 1;
        }
        if (plo.docid as usize) == local_docid {
            return;
        }
    }

    if !facet_filter.is_empty() && is_facet_filter(shard, facet_filter, docid) {
        return;
    };

    let filtered = !not_query_list.is_empty()
        || !field_filter_set.is_empty()
        || !shard.delete_hashset.is_empty()
        || !facet_filter.is_empty();

    shard.decode_positions_uncommitted(plo_single, false);

    if !field_filter_set.is_empty()
        && plo_single.field_vec.len() + field_filter_set.len() <= shard.indexed_field_vec.len()
    {
        let mut match_flag = false;
        for field in plo_single.field_vec.iter() {
            if field_filter_set.contains(&field.0) {
                match_flag = true;
            }
        }
        if !match_flag {
            return;
        }
    }

    match *result_type {
        ResultType::Count => {
            if filtered {
                facet_count(shard, search_result, docid);

                *result_count += 1;
            }
            return;
        }
        ResultType::Topk => {}
        ResultType::TopkCount => {
            if filtered {
                facet_count(shard, search_result, docid);

                *result_count += 1;
            }
        }
    }

    let bm25 = get_bm25f_singleterm_multifield_uncommitted(shard, docid, plo_single);

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
pub(crate) fn add_result_multiterm_uncommitted(
    shard: &Shard,
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
) {
    if !shard.delete_hashset.is_empty() && shard.delete_hashset.contains(&docid) {
        return;
    }

    for plo in not_query_list.iter_mut() {
        if !plo.bm25_flag {
            continue;
        }

        let local_docid = docid & 0b11111111_11111111;

        while plo.p_docid < plo.p_docid_count
            && (plo.p_docid == 0 || (plo.docid as usize) < local_docid)
        {
            let mut read_pointer = plo.posting_pointer;

            plo.posting_pointer = read_u32_ref(&shard.postings_buffer, &mut read_pointer) as usize;
            plo.docid = read_u16_ref(&shard.postings_buffer, &mut read_pointer) as i32;

            plo.p_docid += 1;
        }
        if (plo.docid as usize) == local_docid {
            return;
        }
    }

    if !facet_filter.is_empty() && is_facet_filter(shard, facet_filter, docid) {
        return;
    };

    let filtered = phrase_query
        || !field_filter_set.is_empty()
        || !shard.delete_hashset.is_empty()
        || !facet_filter.is_empty();

    if !filtered && result_type == &ResultType::Count {
        facet_count(shard, search_result, docid);

        *result_count += 1;
        return;
    }

    for plo in query_list.iter_mut() {
        shard.decode_positions_uncommitted(plo, phrase_query);

        if !field_filter_set.is_empty()
            && plo.field_vec.len() + field_filter_set.len() <= shard.indexed_field_vec.len()
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

    if phrase_query {
        let len = query_list.len();
        let mut index_transpose = vec![0; len];
        for i in 0..len {
            index_transpose[query_list[i].term_index_unique] = i;
        }

        let mut phrasematch_count = 0;
        if shard.indexed_field_vec.len() == 1 {
            for plo in non_unique_query_list.iter_mut() {
                plo.p_pos = 0;
                let item = &query_list[index_transpose[plo.term_index_unique]];
                plo.positions_pointer = item.positions_pointer as usize;
                plo.positions_count = item.positions_count;

                plo.is_embedded = item.is_embedded;
                plo.embedded_positions = item.embedded_positions;

                plo.pos = get_next_position_uncommitted(shard, plo);
            }

            non_unique_query_list
                .sort_by(|x, y| x.positions_count.partial_cmp(&y.positions_count).unwrap());

            let t1 = 0;
            let mut t2 = 1;
            let mut pos1 = non_unique_query_list[t1].pos;
            let mut pos2 = non_unique_query_list[t2].pos;

            loop {
                match (pos1 + non_unique_query_list[t2].term_index_nonunique as u32)
                    .cmp(&(pos2 + non_unique_query_list[t1].term_index_nonunique as u32))
                {
                    std::cmp::Ordering::Less => {
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
                        pos1 +=
                            get_next_position_uncommitted(shard, &mut non_unique_query_list[t1])
                                + 1;
                    }
                    std::cmp::Ordering::Greater => {
                        non_unique_query_list[t2].p_pos += 1;
                        if non_unique_query_list[t2].p_pos
                            == non_unique_query_list[t2].positions_count as i32
                        {
                            break;
                        }
                        pos2 = non_unique_query_list[t2].pos
                            + get_next_position_uncommitted(shard, &mut non_unique_query_list[t2])
                            + 1;
                        non_unique_query_list[t2].pos = pos2;
                    }
                    std::cmp::Ordering::Equal => {
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

                        pos1 +=
                            get_next_position_uncommitted(shard, &mut non_unique_query_list[t1])
                                + 1;
                        pos2 = non_unique_query_list[t2].pos
                            + get_next_position_uncommitted(shard, &mut non_unique_query_list[t2])
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

            'main: for i in 0..shard.indexed_field_vec.len() as u16 {
                for plo in non_unique_query_list.iter_mut() {
                    while plo.field_vec[plo.p_field].0 < i {
                        if !plo.is_embedded {
                            for _ in plo.p_pos..plo.field_vec[plo.p_field].1 as i32 {
                                get_next_position_uncommitted(shard, plo);
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
                    plo.pos = get_next_position_uncommitted(shard, plo);
                }

                if !field_filter_set.is_empty() && !field_filter_set.contains(&i) {
                    continue;
                }

                non_unique_query_list
                    .sort_by(|x, y| x.positions_count.partial_cmp(&y.positions_count).unwrap());

                let t1 = 0;
                let mut t2 = 1;
                let mut pos1 = non_unique_query_list[t1].pos;
                let mut pos2 = non_unique_query_list[t2].pos;

                loop {
                    match (pos1 + non_unique_query_list[t2].term_index_nonunique as u32)
                        .cmp(&(pos2 + non_unique_query_list[t1].term_index_nonunique as u32))
                    {
                        std::cmp::Ordering::Less => {
                            if t2 > 1 {
                                t2 = 1;
                                pos2 = non_unique_query_list[t2].pos;
                            }

                            non_unique_query_list[t1].p_pos += 1;
                            if non_unique_query_list[t1].p_pos
                                == non_unique_query_list[t1].positions_count as i32
                            {
                                if (i as usize) < shard.indexed_field_vec.len() - 1 {
                                    for item in non_unique_query_list.iter_mut().skip(1) {
                                        item.p_pos += 1
                                    }
                                }
                                break;
                            }
                            pos1 += get_next_position_uncommitted(
                                shard,
                                &mut non_unique_query_list[t1],
                            ) + 1;
                        }
                        std::cmp::Ordering::Greater => {
                            non_unique_query_list[t2].p_pos += 1;
                            if non_unique_query_list[t2].p_pos
                                == non_unique_query_list[t2].positions_count as i32
                            {
                                if (i as usize) < shard.indexed_field_vec.len() - 1 {
                                    for (j, item) in non_unique_query_list.iter_mut().enumerate() {
                                        if j != t2 {
                                            item.p_pos += 1
                                        }
                                    }
                                }
                                break;
                            }
                            pos2 = non_unique_query_list[t2].pos
                                + get_next_position_uncommitted(
                                    shard,
                                    &mut non_unique_query_list[t2],
                                )
                                + 1;
                            non_unique_query_list[t2].pos = pos2;
                        }
                        std::cmp::Ordering::Equal => {
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
                                if (i as usize) < shard.indexed_field_vec.len() - 1 {
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
                                if (i as usize) < shard.indexed_field_vec.len() - 1 {
                                    for item in non_unique_query_list.iter_mut().skip(2) {
                                        item.p_pos += 1
                                    }
                                }
                                break;
                            }

                            pos1 += get_next_position_uncommitted(
                                shard,
                                &mut non_unique_query_list[t1],
                            ) + 1;
                            pos2 = non_unique_query_list[t2].pos
                                + get_next_position_uncommitted(
                                    shard,
                                    &mut non_unique_query_list[t2],
                                )
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

    match *result_type {
        ResultType::Count => {
            facet_count(shard, search_result, docid);

            *result_count += 1;
            return;
        }
        ResultType::Topk => {}
        ResultType::TopkCount => {
            facet_count(shard, search_result, docid);

            *result_count += 1;
        }
    }

    let bm25 = get_bm25f_multiterm_multifield_uncommitted(shard, docid, query_list);

    search_result.topk_candidates.add_topk(
        min_heap::Result {
            doc_id: docid,
            score: bm25,
        },
        top_k,
    );
}

#[inline(always)]
pub(crate) fn get_bm25f_singleterm_multifield_uncommitted(
    shard: &Shard,
    docid: usize,
    plo_single: &PostingListObjectQuery,
) -> f32 {
    let mut bm25f = 0.0;

    let document_length_normalized_average = if shard.document_length_normalized_average == 0.0 {
        shard.positions_sum_normalized as f32 / shard.indexed_doc_count as f32
    } else {
        shard.document_length_normalized_average
    };

    if shard.indexed_field_vec.len() == 1 {
        let document_length_normalized = DOCUMENT_LENGTH_COMPRESSION
            [shard.document_length_compressed_array[0][docid & 0b11111111_11111111] as usize]
            as f32;

        let document_length_quotient =
            document_length_normalized / document_length_normalized_average;

        match plo_single.ngram_type {
            NgramType::SingleTerm => {
                let tf = plo_single.field_vec[0].1 as f32;

                bm25f = plo_single.idf
                    * ((tf * (K + 1.0) / (tf + (K * (1.0 - B + (B * document_length_quotient)))))
                        + SIGMA);
            }
            NgramType::NgramFF | NgramType::NgramFR | NgramType::NgramRF => {
                let tf_ngram1 = plo_single.field_vec_ngram1[0].1 as f32;
                let tf_ngram2 = plo_single.field_vec_ngram2[0].1 as f32;
                bm25f = plo_single.idf_ngram1
                    * ((tf_ngram1 * (K + 1.0)
                        / (tf_ngram1 + (K * (1.0 - B + (B * document_length_quotient)))))
                        + SIGMA)
                    + plo_single.idf_ngram2
                        * ((tf_ngram2 * (K + 1.0)
                            / (tf_ngram2 + (K * (1.0 - B + (B * document_length_quotient)))))
                            + SIGMA);
            }
            _ => {
                let tf_ngram1 = plo_single.field_vec_ngram1[0].1 as f32;
                let tf_ngram2 = plo_single.field_vec_ngram2[0].1 as f32;
                let tf_ngram3 = plo_single.field_vec_ngram3[0].1 as f32;
                bm25f = plo_single.idf_ngram1
                    * ((tf_ngram1 * (K + 1.0)
                        / (tf_ngram1 + (K * (1.0 - B + (B * document_length_quotient)))))
                        + SIGMA)
                    + plo_single.idf_ngram2
                        * ((tf_ngram2 * (K + 1.0)
                            / (tf_ngram2 + (K * (1.0 - B + (B * document_length_quotient)))))
                            + SIGMA)
                    + plo_single.idf_ngram3
                        * ((tf_ngram3 * (K + 1.0)
                            / (tf_ngram3 + (K * (1.0 - B + (B * document_length_quotient)))))
                            + SIGMA);
            }
        }
    } else {
        match plo_single.ngram_type {
            NgramType::SingleTerm => {
                for field in plo_single.field_vec.iter() {
                    let field_id = field.0 as usize;

                    let document_length_normalized = DOCUMENT_LENGTH_COMPRESSION[shard
                        .document_length_compressed_array[field_id][docid & 0b11111111_11111111]
                        as usize] as f32;

                    let document_length_quotient =
                        document_length_normalized / document_length_normalized_average;

                    let tf = field.1 as f32;

                    bm25f += plo_single.idf
                        * ((tf * (K + 1.0)
                            / (tf + (K * (1.0 - B + (B * document_length_quotient)))))
                            + SIGMA);
                }
            }
            NgramType::NgramFF | NgramType::NgramFR | NgramType::NgramRF => {
                for field in plo_single.field_vec_ngram1.iter() {
                    let field_id = field.0 as usize;

                    let document_length_normalized = DOCUMENT_LENGTH_COMPRESSION[shard
                        .document_length_compressed_array[field_id][docid & 0b11111111_11111111]
                        as usize] as f32;

                    let document_length_quotient =
                        document_length_normalized / document_length_normalized_average;

                    let tf_ngram1 = field.1 as f32;

                    bm25f += plo_single.idf_ngram1
                        * ((tf_ngram1 * (K + 1.0)
                            / (tf_ngram1 + (K * (1.0 - B + (B * document_length_quotient)))))
                            + SIGMA);
                }

                for field in plo_single.field_vec_ngram2.iter() {
                    let field_id = field.0 as usize;

                    let document_length_normalized = DOCUMENT_LENGTH_COMPRESSION[shard
                        .document_length_compressed_array[field_id][docid & 0b11111111_11111111]
                        as usize] as f32;

                    let document_length_quotient =
                        document_length_normalized / document_length_normalized_average;

                    let tf_ngram2 = field.1 as f32;

                    bm25f += plo_single.idf_ngram2
                        * ((tf_ngram2 * (K + 1.0)
                            / (tf_ngram2 + (K * (1.0 - B + (B * document_length_quotient)))))
                            + SIGMA);
                }
            }
            _ => {
                for field in plo_single.field_vec_ngram1.iter() {
                    let field_id = field.0 as usize;

                    let document_length_normalized = DOCUMENT_LENGTH_COMPRESSION[shard
                        .document_length_compressed_array[field_id][docid & 0b11111111_11111111]
                        as usize] as f32;

                    let document_length_quotient =
                        document_length_normalized / document_length_normalized_average;

                    let tf_ngram1 = field.1 as f32;

                    bm25f += plo_single.idf_ngram1
                        * ((tf_ngram1 * (K + 1.0)
                            / (tf_ngram1 + (K * (1.0 - B + (B * document_length_quotient)))))
                            + SIGMA);
                }

                for field in plo_single.field_vec_ngram2.iter() {
                    let field_id = field.0 as usize;

                    let document_length_normalized = DOCUMENT_LENGTH_COMPRESSION[shard
                        .document_length_compressed_array[field_id][docid & 0b11111111_11111111]
                        as usize] as f32;

                    let document_length_quotient =
                        document_length_normalized / document_length_normalized_average;

                    let tf_ngram2 = field.1 as f32;

                    bm25f += plo_single.idf_ngram2
                        * ((tf_ngram2 * (K + 1.0)
                            / (tf_ngram2 + (K * (1.0 - B + (B * document_length_quotient)))))
                            + SIGMA);
                }

                for field in plo_single.field_vec_ngram3.iter() {
                    let field_id = field.0 as usize;

                    let document_length_normalized = DOCUMENT_LENGTH_COMPRESSION[shard
                        .document_length_compressed_array[field_id][docid & 0b11111111_11111111]
                        as usize] as f32;

                    let document_length_quotient =
                        document_length_normalized / document_length_normalized_average;

                    let tf_ngram3 = field.1 as f32;

                    bm25f += plo_single.idf_ngram3
                        * ((tf_ngram3 * (K + 1.0)
                            / (tf_ngram3 + (K * (1.0 - B + (B * document_length_quotient)))))
                            + SIGMA);
                }
            }
        }
    }

    bm25f
}

#[inline(always)]
pub(crate) fn get_bm25f_multiterm_multifield_uncommitted(
    shard: &Shard,
    docid: usize,
    query_list: &mut [PostingListObjectQuery],
) -> f32 {
    let mut bm25f = 0.0;

    let document_length_normalized_average = if shard.document_length_normalized_average == 0.0 {
        shard.positions_sum_normalized as f32 / shard.indexed_doc_count as f32
    } else {
        shard.document_length_normalized_average
    };

    if shard.indexed_field_vec.len() == 1 {
        let mut document_length_quotient = 0.0;

        for plo in query_list.iter() {
            if !plo.bm25_flag {
                continue;
            }

            if document_length_quotient == 0.0 {
                let document_length_normalized = DOCUMENT_LENGTH_COMPRESSION[shard
                    .document_length_compressed_array[0][docid & 0b11111111_11111111]
                    as usize] as f32;

                document_length_quotient =
                    document_length_normalized / document_length_normalized_average;
            }

            match plo.ngram_type {
                NgramType::SingleTerm => {
                    let tf = plo.field_vec[0].1 as f32;

                    bm25f += plo.idf
                        * ((tf * (K + 1.0)
                            / (tf + (K * (1.0 - B + (B * document_length_quotient)))))
                            + SIGMA);
                }
                NgramType::NgramFF | NgramType::NgramFR | NgramType::NgramRF => {
                    let tf_ngram1 = plo.field_vec_ngram1[0].1 as f32;
                    let tf_ngram2 = plo.field_vec_ngram2[0].1 as f32;

                    bm25f += plo.idf_ngram1
                        * ((tf_ngram1 * (K + 1.0)
                            / (tf_ngram1 + (K * (1.0 - B + (B * document_length_quotient)))))
                            + SIGMA)
                        + plo.idf_ngram2
                            * ((tf_ngram2 * (K + 1.0)
                                / (tf_ngram2 + (K * (1.0 - B + (B * document_length_quotient)))))
                                + SIGMA);
                }
                _ => {
                    let tf_ngram1 = plo.field_vec_ngram1[0].1 as f32;
                    let tf_ngram2 = plo.field_vec_ngram2[0].1 as f32;
                    let tf_ngram3 = plo.field_vec_ngram3[0].1 as f32;

                    bm25f += plo.idf_ngram1
                        * ((tf_ngram1 * (K + 1.0)
                            / (tf_ngram1 + (K * (1.0 - B + (B * document_length_quotient)))))
                            + SIGMA)
                        + plo.idf_ngram2
                            * ((tf_ngram2 * (K + 1.0)
                                / (tf_ngram2 + (K * (1.0 - B + (B * document_length_quotient)))))
                                + SIGMA)
                        + plo.idf_ngram3
                            * ((tf_ngram3 * (K + 1.0)
                                / (tf_ngram3 + (K * (1.0 - B + (B * document_length_quotient)))))
                                + SIGMA);
                }
            }
        }
    } else {
        for plo in query_list.iter() {
            if !plo.bm25_flag {
                continue;
            }

            match plo.ngram_type {
                NgramType::SingleTerm => {
                    for field in plo.field_vec.iter() {
                        let field_id = field.0 as usize;

                        let document_length_normalized = DOCUMENT_LENGTH_COMPRESSION[shard
                            .document_length_compressed_array[field_id][docid & 0b11111111_11111111]
                            as usize]
                            as f32;

                        let document_length_quotient =
                            document_length_normalized / document_length_normalized_average;

                        let tf = field.1 as f32;

                        let weight = shard.indexed_schema_vec[field.0 as usize].boost;

                        bm25f += weight
                            * plo.idf
                            * ((tf * (K + 1.0)
                                / (tf + (K * (1.0 - B + (B * document_length_quotient)))))
                                + SIGMA);
                    }
                }
                NgramType::NgramFF | NgramType::NgramFR | NgramType::NgramRF => {
                    for field in plo.field_vec_ngram1.iter() {
                        let field_id = field.0 as usize;

                        let document_length_normalized = DOCUMENT_LENGTH_COMPRESSION[shard
                            .document_length_compressed_array[field_id][docid & 0b11111111_11111111]
                            as usize]
                            as f32;

                        let document_length_quotient =
                            document_length_normalized / document_length_normalized_average;

                        let tf_ngram1 = field.1 as f32;

                        let weight = shard.indexed_schema_vec[field.0 as usize].boost;

                        bm25f += weight
                            * plo.idf_ngram1
                            * ((tf_ngram1 * (K + 1.0)
                                / (tf_ngram1 + (K * (1.0 - B + (B * document_length_quotient)))))
                                + SIGMA);
                    }

                    for field in plo.field_vec_ngram2.iter() {
                        let field_id = field.0 as usize;

                        let document_length_normalized = DOCUMENT_LENGTH_COMPRESSION[shard
                            .document_length_compressed_array[field_id][docid & 0b11111111_11111111]
                            as usize]
                            as f32;

                        let document_length_quotient =
                            document_length_normalized / document_length_normalized_average;

                        let tf_ngram2 = field.1 as f32;

                        let weight = shard.indexed_schema_vec[field.0 as usize].boost;

                        bm25f += weight
                            * plo.idf_ngram2
                            * ((tf_ngram2 * (K + 1.0)
                                / (tf_ngram2 + (K * (1.0 - B + (B * document_length_quotient)))))
                                + SIGMA);
                    }
                }
                _ => {
                    for field in plo.field_vec_ngram1.iter() {
                        let field_id = field.0 as usize;

                        let document_length_normalized = DOCUMENT_LENGTH_COMPRESSION[shard
                            .document_length_compressed_array[field_id][docid & 0b11111111_11111111]
                            as usize]
                            as f32;

                        let document_length_quotient =
                            document_length_normalized / document_length_normalized_average;

                        let tf_ngram1 = field.1 as f32;

                        let weight = shard.indexed_schema_vec[field.0 as usize].boost;

                        bm25f += weight
                            * plo.idf_ngram1
                            * ((tf_ngram1 * (K + 1.0)
                                / (tf_ngram1 + (K * (1.0 - B + (B * document_length_quotient)))))
                                + SIGMA);
                    }

                    for field in plo.field_vec_ngram2.iter() {
                        let field_id = field.0 as usize;

                        let document_length_normalized = DOCUMENT_LENGTH_COMPRESSION[shard
                            .document_length_compressed_array[field_id][docid & 0b11111111_11111111]
                            as usize]
                            as f32;

                        let document_length_quotient =
                            document_length_normalized / document_length_normalized_average;

                        let tf_ngram2 = field.1 as f32;

                        let weight = shard.indexed_schema_vec[field.0 as usize].boost;

                        bm25f += weight
                            * plo.idf_ngram2
                            * ((tf_ngram2 * (K + 1.0)
                                / (tf_ngram2 + (K * (1.0 - B + (B * document_length_quotient)))))
                                + SIGMA);
                    }

                    for field in plo.field_vec_ngram3.iter() {
                        let field_id = field.0 as usize;

                        let document_length_normalized = DOCUMENT_LENGTH_COMPRESSION[shard
                            .document_length_compressed_array[field_id][docid & 0b11111111_11111111]
                            as usize]
                            as f32;

                        let document_length_quotient =
                            document_length_normalized / document_length_normalized_average;

                        let tf_ngram3 = field.1 as f32;

                        let weight = shard.indexed_schema_vec[field.0 as usize].boost;

                        bm25f += weight
                            * plo.idf_ngram3
                            * ((tf_ngram3 * (K + 1.0)
                                / (tf_ngram3 + (K * (1.0 - B + (B * document_length_quotient)))))
                                + SIGMA);
                    }
                }
            }
        }
    }

    bm25f
}

impl Shard {
    pub(crate) fn get_posting_count_uncommited(&self, term_string: &str) -> usize {
        let term_bytes = term_string.as_bytes();
        let key0 = hash32(term_bytes) & self.segment_number_mask1;
        let key_hash = hash64(term_bytes);

        match self.segments_level0[key0 as usize].segment.get(&key_hash) {
            Some(value1) => value1.posting_count,

            None => 0,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn search_uncommitted(
        &self,
        unique_terms: &AHashMap<String, TermObject>,
        non_unique_terms: &[NonUniqueTermObject],
        query_type_mut: &mut QueryType,
        result_type: &ResultType,
        field_filter_set: &AHashSet<u16>,
        facet_filter: &[FilterSparse],

        search_result: &mut SearchResult,
        result_count_arc: &Arc<AtomicUsize>,
        top_k: usize,
    ) {
        let mut query_list_map: AHashMap<u64, PostingListObjectQuery> = AHashMap::new();
        let mut query_list: Vec<PostingListObjectQuery>;

        let mut not_query_list_map: AHashMap<u64, PostingListObjectQuery> = AHashMap::new();
        let mut not_query_list: Vec<PostingListObjectQuery>;

        let mut non_unique_query_list: Vec<NonUniquePostingListObjectQuery> = Vec::new();

        let block_id = if self.is_last_level_incomplete {
            self.level_index.len() - 1
        } else {
            self.level_index.len()
        };
        let mut preceding_ngram_count = 0;

        for non_unique_term in non_unique_terms.iter() {
            let term = unique_terms.get(&non_unique_term.term).unwrap();
            let key0: u32 = term.key0;
            let key_hash: u64 = term.key_hash;

            match self.segments_level0[key0 as usize].segment.get(&key_hash) {
                Some(value1) => {
                    let mut idf = 0.0;
                    let mut idf_ngram1 = 0.0;
                    let mut idf_ngram2 = 0.0;
                    let mut idf_ngram3 = 0.0;
                    if result_type != &ResultType::Count {
                        let posting_counts_option = if self.meta.access_type == AccessType::Mmap {
                            decode_posting_list_counts(
                                &self.segments_index[key0 as usize],
                                self,
                                key_hash,
                            )
                        } else {
                            let posting_list_object_index_option =
                                self.segments_index[key0 as usize].segment.get(&key_hash);
                            posting_list_object_index_option.map(|plo| {
                                (
                                    plo.posting_count,
                                    plo.posting_count_ngram_1,
                                    plo.posting_count_ngram_2,
                                    plo.posting_count_ngram_3,
                                )
                            })
                        };

                        if non_unique_term.ngram_type == NgramType::SingleTerm
                            || self.meta.similarity == SimilarityType::Bm25fProximity
                        {
                            let posting_count = if let Some(posting_count) = posting_counts_option {
                                posting_count.0 as usize + value1.posting_count
                            } else {
                                value1.posting_count
                            };

                            idf = (((self.indexed_doc_count as f32 - posting_count as f32 + 0.5)
                                / (posting_count as f32 + 0.5))
                                + 1.0)
                                .ln();
                        } else if term.ngram_type == NgramType::NgramFF
                            || term.ngram_type == NgramType::NgramRF
                            || term.ngram_type == NgramType::NgramFR
                        {
                            let posting_count_ngram_1 =
                                if let Some(posting_count) = posting_counts_option {
                                    posting_count.1
                                } else {
                                    0
                                } + self.get_posting_count_uncommited(&non_unique_term.term_ngram_1)
                                    as u32;

                            let posting_count_ngram_2 =
                                if let Some(posting_count) = posting_counts_option {
                                    posting_count.2
                                } else {
                                    0
                                } + self.get_posting_count_uncommited(&non_unique_term.term_ngram_0)
                                    as u32;

                            idf_ngram1 = (((self.indexed_doc_count as f32
                                - posting_count_ngram_1 as f32
                                + 0.5)
                                / (posting_count_ngram_1 as f32 + 0.5))
                                + 1.0)
                                .ln();

                            idf_ngram2 = (((self.indexed_doc_count as f32
                                - posting_count_ngram_2 as f32
                                + 0.5)
                                / (posting_count_ngram_2 as f32 + 0.5))
                                + 1.0)
                                .ln();
                        } else {
                            let posting_count_ngram_1 =
                                if let Some(posting_count) = posting_counts_option {
                                    posting_count.1
                                } else {
                                    0
                                } + self.get_posting_count_uncommited(&non_unique_term.term_ngram_1)
                                    as u32;

                            let posting_count_ngram_2 =
                                if let Some(posting_count) = posting_counts_option {
                                    posting_count.2
                                } else {
                                    0
                                } + self.get_posting_count_uncommited(&non_unique_term.term_ngram_0)
                                    as u32;

                            let posting_count_ngram_3 =
                                if let Some(posting_count) = posting_counts_option {
                                    posting_count.3
                                } else {
                                    0
                                } + self.get_posting_count_uncommited(&non_unique_term.term_ngram_0)
                                    as u32;

                            idf_ngram1 = (((self.indexed_doc_count as f32
                                - posting_count_ngram_1 as f32
                                + 0.5)
                                / (posting_count_ngram_1 as f32 + 0.5))
                                + 1.0)
                                .ln();

                            idf_ngram2 = (((self.indexed_doc_count as f32
                                - posting_count_ngram_2 as f32
                                + 0.5)
                                / (posting_count_ngram_2 as f32 + 0.5))
                                + 1.0)
                                .ln();

                            idf_ngram3 = (((self.indexed_doc_count as f32
                                - posting_count_ngram_3 as f32
                                + 0.5)
                                / (posting_count_ngram_3 as f32 + 0.5))
                                + 1.0)
                                .ln();
                        }
                    }

                    let term_index_unique = if non_unique_term.op == QueryType::Not {
                        let query_list_map_len = not_query_list_map.len();
                        let value =
                            not_query_list_map
                                .entry(key_hash)
                                .or_insert(PostingListObjectQuery {
                                    posting_count: value1.posting_count as u32,
                                    posting_pointer: value1.pointer_first,
                                    term: non_unique_term.term.clone(),
                                    key0,
                                    term_index_unique: query_list_map_len,

                                    p_docid: 0,
                                    p_docid_count: value1.posting_count,
                                    docid: 0,

                                    idf,
                                    idf_ngram1,
                                    idf_ngram2,
                                    idf_ngram3,
                                    ngram_type: non_unique_term.ngram_type.clone(),
                                    ..Default::default()
                                });
                        value.term_index_unique
                    } else {
                        let query_list_map_len = query_list_map.len();
                        let value =
                            query_list_map
                                .entry(key_hash)
                                .or_insert(PostingListObjectQuery {
                                    posting_count: value1.posting_count as u32,
                                    posting_pointer: value1.pointer_first,
                                    term: non_unique_term.term.clone(),
                                    key0,
                                    term_index_unique: query_list_map_len,

                                    pointer_pivot_p_docid: value1.pointer_pivot_p_docid,
                                    p_docid: 0,
                                    p_docid_count: value1.posting_count,
                                    docid: 0,

                                    idf,
                                    idf_ngram1,
                                    idf_ngram2,
                                    idf_ngram3,
                                    ngram_type: non_unique_term.ngram_type.clone(),
                                    ..Default::default()
                                });
                        value.term_index_unique
                    };

                    if non_unique_term.op == QueryType::Phrase {
                        let nu_plo = NonUniquePostingListObjectQuery {
                            term_index_unique,
                            term_index_nonunique: non_unique_query_list.len()
                                + preceding_ngram_count,
                            pos: 0,
                            p_pos: 0,
                            positions_pointer: 0,
                            positions_count: 0,
                            byte_array: &DUMMY_VEC_8,
                            key0,
                            is_embedded: false,
                            p_field: 0,
                            field_vec: SmallVec::new(),
                            embedded_positions: [0; 4],
                        };

                        non_unique_query_list.push(nu_plo);
                    }

                    match non_unique_term.ngram_type {
                        NgramType::SingleTerm => {}
                        NgramType::NgramFF | NgramType::NgramRF | NgramType::NgramFR => {
                            preceding_ngram_count += 1
                        }
                        _ => preceding_ngram_count += 2,
                    };
                }
                None => {
                    if non_unique_term.op == QueryType::Intersection
                        || non_unique_term.op == QueryType::Phrase
                    {
                        return;
                    }
                }
            }
        }

        not_query_list = not_query_list_map.into_values().collect();
        query_list = query_list_map.into_values().collect();
        let query_list_len = query_list.len();

        let non_unique_query_list_count = non_unique_query_list.len();

        if query_list_len == 0 {
        } else if query_list_len == 1 {
            self.single_docid_uncommitted(
                block_id,
                &mut non_unique_query_list,
                &mut query_list,
                &mut not_query_list,
                0,
                result_type,
                field_filter_set,
                facet_filter,
                search_result,
                result_count_arc,
                top_k,
            );
        } else if query_type_mut == &QueryType::Union {
            self.union_docid_uncommitted(
                &mut non_unique_query_list,
                &mut query_list,
                &mut not_query_list,
                block_id,
                result_count_arc,
                search_result,
                top_k,
                result_type,
                field_filter_set,
                facet_filter,
            );
        } else {
            self.intersection_docid_uncommitted(
                &mut non_unique_query_list,
                &mut query_list,
                &mut not_query_list,
                block_id,
                result_count_arc,
                search_result,
                top_k,
                result_type,
                field_filter_set,
                facet_filter,
                query_type_mut == &mut QueryType::Phrase && non_unique_query_list_count >= 2,
            );
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn single_docid_uncommitted<'a>(
        self: &Shard,
        block_id: usize,
        non_unique_query_list: &mut [NonUniquePostingListObjectQuery<'a>],
        query_list: &mut [PostingListObjectQuery<'a>],
        not_query_list: &mut [PostingListObjectQuery<'a>],
        term_index: usize,
        result_type: &ResultType,
        field_filter_set: &AHashSet<u16>,
        facet_filter: &[FilterSparse],

        search_result: &mut SearchResult,
        result_count_arc: &Arc<AtomicUsize>,
        top_k: usize,
    ) {
        let filtered = !not_query_list.is_empty()
            || !field_filter_set.is_empty()
            || !self.delete_hashset.is_empty()
            || !facet_filter.is_empty();

        if (self.enable_single_term_topk || (result_type == &ResultType::Count))
            && (non_unique_query_list.len() <= 1 && !filtered)
        {
            result_count_arc.fetch_add(
                query_list[term_index].posting_count as usize,
                Ordering::Relaxed,
            );

            return;
        }

        let plo1 = &mut query_list[term_index];

        let mut result_count_local = 0;
        for i in 0..plo1.posting_count {
            plo1.p_docid = i as usize;

            self.get_next_docid_uncommitted(plo1);

            add_result_singleterm_uncommitted(
                self,
                (block_id << 16) | plo1.docid as usize,
                &mut result_count_local,
                search_result,
                top_k,
                result_type,
                field_filter_set,
                facet_filter,
                plo1,
                not_query_list,
            );
        }

        if result_type != &ResultType::Topk {
            let filtered = !not_query_list.is_empty() || !field_filter_set.is_empty();
            result_count_arc.fetch_add(
                if filtered {
                    result_count_local as usize
                } else {
                    plo1.posting_count as usize
                },
                Ordering::Relaxed,
            );
        }
    }

    pub(crate) fn get_next_docid_uncommitted(self: &Shard, plo: &mut PostingListObjectQuery) {
        plo.posting_pointer_previous = plo.posting_pointer;

        let mut read_pointer = plo.posting_pointer;

        plo.posting_pointer = read_u32_ref(&self.postings_buffer, &mut read_pointer) as usize;

        plo.docid = read_u16_ref(&self.postings_buffer, &mut read_pointer) as i32;
    }

    #[inline(always)]
    pub(crate) fn decode_positions_uncommitted(
        self: &Shard,
        plo: &mut PostingListObjectQuery,
        phrase_query: bool,
    ) {
        let mut read_pointer = plo.posting_pointer_previous + 6;

        let position_size_byte_temp: u16 = read_u16_ref(&self.postings_buffer, &mut read_pointer);

        let mut field_vec: SmallVec<[(u16, usize); 2]> = SmallVec::new();
        plo.is_embedded = position_size_byte_temp & 0b10000000_00000000 > 0;

        if !plo.is_embedded {
            match plo.ngram_type {
                NgramType::SingleTerm => {}
                NgramType::NgramFF | NgramType::NgramFR | NgramType::NgramRF => {
                    plo.field_vec_ngram1 = SmallVec::new();
                    plo.field_vec_ngram2 = SmallVec::new();
                    read_multifield_vec(
                        self.indexed_field_vec.len(),
                        self.indexed_field_id_bits,
                        self.indexed_field_id_mask,
                        self.longest_field_id,
                        &mut plo.field_vec_ngram1,
                        &self.postings_buffer,
                        &mut read_pointer,
                    );
                    read_multifield_vec(
                        self.indexed_field_vec.len(),
                        self.indexed_field_id_bits,
                        self.indexed_field_id_mask,
                        self.longest_field_id,
                        &mut plo.field_vec_ngram2,
                        &self.postings_buffer,
                        &mut read_pointer,
                    );
                }
                _ => {
                    plo.field_vec_ngram1 = SmallVec::new();
                    plo.field_vec_ngram2 = SmallVec::new();
                    plo.field_vec_ngram3 = SmallVec::new();
                    read_multifield_vec(
                        self.indexed_field_vec.len(),
                        self.indexed_field_id_bits,
                        self.indexed_field_id_mask,
                        self.longest_field_id,
                        &mut plo.field_vec_ngram1,
                        &self.postings_buffer,
                        &mut read_pointer,
                    );
                    read_multifield_vec(
                        self.indexed_field_vec.len(),
                        self.indexed_field_id_bits,
                        self.indexed_field_id_mask,
                        self.longest_field_id,
                        &mut plo.field_vec_ngram2,
                        &self.postings_buffer,
                        &mut read_pointer,
                    );
                    read_multifield_vec(
                        self.indexed_field_vec.len(),
                        self.indexed_field_id_bits,
                        self.indexed_field_id_mask,
                        self.longest_field_id,
                        &mut plo.field_vec_ngram3,
                        &self.postings_buffer,
                        &mut read_pointer,
                    );
                }
            }

            read_multifield_vec(
                self.indexed_field_vec.len(),
                self.indexed_field_id_bits,
                self.indexed_field_id_mask,
                self.longest_field_id,
                &mut field_vec,
                &self.postings_buffer,
                &mut read_pointer,
            );
        } else {
            let field_id;

            if plo.p_docid < plo.pointer_pivot_p_docid as usize {
                let rank_position_pointer = read_u16(&self.postings_buffer, read_pointer) as u32;

                match (
                    self.indexed_field_vec.len() == 1,
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
                        field_id = self.longest_field_id as u16;
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
                        field_id = self.longest_field_id as u16;
                        field_vec.push((field_id, 2));
                    }

                    (false, 0b1000) => {
                        let position_bits = 12 - self.indexed_field_id_bits;
                        field_id = ((rank_position_pointer >> position_bits)
                            & self.indexed_field_id_mask as u32)
                            as u16;
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
                        let position_bits = 12 - self.indexed_field_id_bits;
                        field_id = ((rank_position_pointer >> position_bits)
                            & self.indexed_field_id_mask as u32)
                            as u16;
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
                        let position_bits = 12 - self.indexed_field_id_bits;
                        field_id = ((rank_position_pointer >> position_bits)
                            & self.indexed_field_id_mask as u32)
                            as u16;
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
                            12 - self.indexed_field_id_bits - self.indexed_field_id_bits;
                        field_id = ((rank_position_pointer
                            >> (position_bits + self.indexed_field_id_bits))
                            & self.indexed_field_id_mask as u32)
                            as u16;
                        let field_id_2 = ((rank_position_pointer >> position_bits)
                            & self.indexed_field_id_mask as u32)
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
                let rank_position_pointer = read_u32(&self.postings_buffer, read_pointer);

                match (
                    self.indexed_field_vec.len() == 1,
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
                        field_id = self.longest_field_id as u16;
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
                        field_id = self.longest_field_id as u16;
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
                        field_id = self.longest_field_id as u16;
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
                        field_id = self.longest_field_id as u16;
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
                        let position_bits = 19 - self.indexed_field_id_bits;
                        field_id = ((rank_position_pointer >> position_bits)
                            & self.indexed_field_id_mask as u32)
                            as u16;
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
                        let position_bits = 19 - self.indexed_field_id_bits;
                        field_id = ((rank_position_pointer >> position_bits)
                            & self.indexed_field_id_mask as u32)
                            as u16;
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
                        let position_bits = 19 - self.indexed_field_id_bits;
                        field_id = ((rank_position_pointer >> position_bits)
                            & self.indexed_field_id_mask as u32)
                            as u16;
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
                        let position_bits = 19 - self.indexed_field_id_bits;
                        field_id = ((rank_position_pointer >> position_bits)
                            & self.indexed_field_id_mask as u32)
                            as u16;
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
                            19 - self.indexed_field_id_bits - self.indexed_field_id_bits;
                        field_id = ((rank_position_pointer
                            >> (position_bits + self.indexed_field_id_bits))
                            & self.indexed_field_id_mask as u32)
                            as u16;
                        let field_id_2 = ((rank_position_pointer >> position_bits)
                            & self.indexed_field_id_mask as u32)
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
                            19 - self.indexed_field_id_bits - self.indexed_field_id_bits;
                        field_id = ((rank_position_pointer
                            >> (position_bits + self.indexed_field_id_bits))
                            & self.indexed_field_id_mask as u32)
                            as u16;
                        let field_id_2 = ((rank_position_pointer >> position_bits)
                            & self.indexed_field_id_mask as u32)
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
                            19 - self.indexed_field_id_bits - self.indexed_field_id_bits;
                        field_id = ((rank_position_pointer
                            >> (position_bits + self.indexed_field_id_bits))
                            & self.indexed_field_id_mask as u32)
                            as u16;
                        let field_id_2 = ((rank_position_pointer >> position_bits)
                            & self.indexed_field_id_mask as u32)
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
                        let position_bits = 19
                            - self.indexed_field_id_bits
                            - self.indexed_field_id_bits
                            - self.indexed_field_id_bits;
                        field_id = ((rank_position_pointer
                            >> (position_bits
                                + self.indexed_field_id_bits
                                + self.indexed_field_id_bits))
                            & self.indexed_field_id_mask as u32)
                            as u16;
                        let field_id_2 = ((rank_position_pointer
                            >> (position_bits + self.indexed_field_id_bits))
                            & self.indexed_field_id_mask as u32)
                            as u16;
                        let field_id_3 = ((rank_position_pointer >> position_bits)
                            & self.indexed_field_id_mask as u32)
                            as u16;
                        field_vec.extend([(field_id, 1), (field_id_2, 1), (field_id_3, 1)]);

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

                    (_, _) => {
                        if phrase_query {
                            println!("unsupported 3 byte pointer embedded");
                            plo.embedded_positions = [0, 0, 0, 0]
                        };
                    }
                }
            };
        }

        plo.positions_count = field_vec[0].1 as u32;
        plo.field_vec = field_vec;
        plo.positions_pointer = read_pointer as u32;
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn intersection_docid_uncommitted(
        self: &Shard,
        non_unique_query_list: &mut [NonUniquePostingListObjectQuery<'_>],
        query_list: &mut [PostingListObjectQuery<'_>],
        not_query_list: &mut [PostingListObjectQuery<'_>],
        block_id: usize,
        result_count_arc: &Arc<AtomicUsize>,
        search_result: &mut SearchResult,
        top_k: usize,
        result_type: &ResultType,
        field_filter_set: &AHashSet<u16>,
        facet_filter: &[FilterSparse],
        phrase_query: bool,
    ) {
        let mut result_count = 0;
        let t1 = 0;
        let mut t2 = 1;

        query_list.sort_by(|x, y| x.posting_count.partial_cmp(&y.posting_count).unwrap());

        for plo in query_list.iter_mut() {
            plo.p_docid = 0;
            self.get_next_docid_uncommitted(plo);
        }

        'outer: loop {
            match query_list[t1].docid.cmp(&query_list[t2].docid) {
                std::cmp::Ordering::Less => {
                    if t2 > 1 {
                        t2 = 1;
                    }

                    query_list[t1].p_docid += 1;
                    if query_list[t1].p_docid == query_list[t1].posting_count as usize {
                        break;
                    }
                    self.get_next_docid_uncommitted(&mut query_list[t1]);
                }
                std::cmp::Ordering::Greater => {
                    query_list[t2].p_docid += 1;
                    if query_list[t2].p_docid == query_list[t2].posting_count as usize {
                        break;
                    }

                    self.get_next_docid_uncommitted(&mut query_list[t2]);
                }
                std::cmp::Ordering::Equal => {
                    if t2 + 1 < query_list.len() {
                        t2 += 1;
                        continue;
                    }

                    add_result_multiterm_uncommitted(
                        self,
                        (block_id << 16) | query_list[t1].docid as usize,
                        &mut result_count,
                        search_result,
                        top_k,
                        result_type,
                        field_filter_set,
                        facet_filter,
                        non_unique_query_list,
                        query_list,
                        not_query_list,
                        phrase_query,
                    );

                    query_list[t1].p_docid += 1;
                    if query_list[t1].p_docid == query_list[t1].posting_count as usize {
                        break;
                    }
                    for item in query_list.iter_mut().skip(1) {
                        item.p_docid += 1;
                        if item.p_docid == item.posting_count as usize {
                            break 'outer;
                        }
                        self.get_next_docid_uncommitted(item);
                    }

                    t2 = 1;
                    self.get_next_docid_uncommitted(&mut query_list[t1]);
                }
            }
        }

        if result_type != &ResultType::Topk {
            result_count_arc.fetch_add(result_count as usize, Ordering::Relaxed);
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn union_docid_uncommitted(
        &self,
        non_unique_query_list: &mut [NonUniquePostingListObjectQuery],
        query_list: &mut [PostingListObjectQuery],
        not_query_list: &mut [PostingListObjectQuery],
        block_id: usize,
        result_count_arc: &Arc<AtomicUsize>,
        search_result: &mut SearchResult,
        top_k: usize,
        result_type: &ResultType,
        field_filter_set: &AHashSet<u16>,
        facet_filter: &[FilterSparse],
    ) {
        let mut result_count: i32 = 0;

        if result_type == &ResultType::Count {
            self.union_count_uncommitted(&mut result_count, query_list);
            result_count_arc.fetch_add(result_count as usize, Ordering::Relaxed);
            return;
        }

        self.union_scan_uncommitted(
            &mut result_count,
            block_id,
            search_result,
            top_k,
            result_type,
            field_filter_set,
            facet_filter,
            non_unique_query_list,
            query_list,
            not_query_list,
        );

        result_count_arc.fetch_add(result_count as usize, Ordering::Relaxed);
    }

    pub(crate) fn union_count_uncommitted(
        &self,
        result_count: &mut i32,
        query_list: &mut [PostingListObjectQuery],
    ) {
        query_list.sort_by(|a, b| b.posting_count.partial_cmp(&a.posting_count).unwrap());

        let mut result_count_local = query_list[0].posting_count;
        let mut bitmap_0: [u8; 8192] = [0u8; 8192];

        for (i, item) in query_list.iter_mut().enumerate() {
            if item.end_flag {
                continue;
            }

            if i == 0 {
                for _p_docid in 0..item.posting_count {
                    self.get_next_docid_uncommitted(item);
                    let docid = item.docid as usize;
                    let byte_index = docid >> 3;
                    let bit_index = docid & 7;

                    bitmap_0[byte_index] |= 1 << bit_index;
                }
            } else {
                for _p_docid in 0..item.posting_count {
                    self.get_next_docid_uncommitted(item);
                    let docid = item.docid as usize;
                    let byte_index = docid >> 3;
                    let bit_index = docid & 7;

                    if bitmap_0[byte_index] & (1 << bit_index) == 0 {
                        bitmap_0[byte_index] |= 1 << bit_index;
                        result_count_local += 1;
                    }
                }
            }
        }

        *result_count += result_count_local as i32;
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn union_scan_uncommitted(
        &self,
        result_count: &mut i32,
        block_id: usize,
        search_result: &mut SearchResult,
        top_k: usize,
        result_type: &ResultType,
        field_filter_set: &AHashSet<u16>,
        facet_filter: &[FilterSparse],
        non_unique_query_list: &mut [NonUniquePostingListObjectQuery],
        query_list: &mut [PostingListObjectQuery],
        not_query_list: &mut [PostingListObjectQuery],
    ) {
        for plo in query_list.iter_mut() {
            if !plo.end_flag {
                self.get_next_docid_uncommitted(plo);
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
                    add_result_singleterm_uncommitted(
                        self,
                        (block_id << 16) | docid_min as usize,
                        result_count,
                        search_result,
                        top_k,
                        result_type,
                        field_filter_set,
                        facet_filter,
                        &mut query_list[term_index],
                        not_query_list,
                    );
                    if not_query_list.is_empty() && result_type != &ResultType::Topk {
                        *result_count += 1;
                    }
                } else {
                    add_result_multiterm_uncommitted(
                        self,
                        (block_id << 16) | docid_min as usize,
                        result_count,
                        search_result,
                        top_k,
                        result_type,
                        field_filter_set,
                        facet_filter,
                        non_unique_query_list,
                        query_list,
                        not_query_list,
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
                        if plo.p_docid < plo.posting_count as usize - 1 {
                            plo.p_docid += 1;
                            self.get_next_docid_uncommitted(plo);
                            break_loop = false;
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
}
