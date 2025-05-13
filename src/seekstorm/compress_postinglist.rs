use std::cmp;

use smallvec::SmallVec;

use crate::{
    add_result::{B, DOCUMENT_LENGTH_COMPRESSION, K, SIGMA, decode_positions_commit},
    compatible::_lzcnt_u32,
    index::{CompressionType, Index, STOP_BIT, SimilarityType},
    utils::{
        block_copy, read_u16_ref, read_u32_ref, write_u8_ref, write_u16, write_u16_ref,
        write_u32_ref, write_u64_ref,
    },
};

/// Compress a single postinglist using roaring bitmaps compression for docid https://roaringbitmap.org/about/
pub(crate) fn compress_postinglist(
    index: &mut Index,
    key_head_pointer_w: &mut usize,
    roaring_offset: &mut usize,
    key_body_offset: u32,
    key0: &usize,
    key_hash: &u64,
) -> usize {
    let mut posting_count_bigram1 = 0;
    let mut posting_count_bigram2 = 0;
    {
        let plo = index.segments_level0[*key0].segment.get(key_hash).unwrap();

        if plo.is_bigram {
            let bigram_term_index1 = index
                .frequent_words
                .binary_search(&plo.term_bigram1)
                .unwrap();
            let bigram_term_index2 = index
                .frequent_words
                .binary_search(&plo.term_bigram2)
                .unwrap();
            posting_count_bigram1 = index.frequentword_posting_counts[bigram_term_index1] as usize;
            posting_count_bigram2 = index.frequentword_posting_counts[bigram_term_index2] as usize;
        }
    }

    let plo = index.segments_level0[*key0]
        .segment
        .get_mut(key_hash)
        .unwrap();
    let plo_posting_count = plo.posting_count;

    if plo.is_bigram {
        plo.posting_count_bigram1 = posting_count_bigram1;
        plo.posting_count_bigram2 = posting_count_bigram2;
    }

    let mut size_compressed_docid_key: usize = 0;

    let enable_rle_compression: bool = true;
    let enable_bitmap_compression: bool = true;
    let enable_delta_compression: bool = false;

    index.docid_count += plo.posting_count;
    index.postinglist_count += 1;
    index.position_count += plo.position_count;
    let mut compression_type_pointer = CompressionType::Error as u32;

    let mut runs_count: u16 = 0;

    let delta_size_bits: u32 = 4;
    let range_bits: u32 = 32 - unsafe { _lzcnt_u32(plo.docid_delta_max.into()) };
    let result_bits: u32 = delta_size_bits + (range_bits * plo.posting_count as u32);
    let delta_compression_size_byte: u32 = result_bits.div_ceil(8);

    if (plo.posting_count < 4096) || !enable_bitmap_compression {
        if enable_rle_compression {
            let runs_count_threshold: u16 = cmp::min(
                (plo.posting_count / 2) as u16,
                if enable_delta_compression {
                    (delta_compression_size_byte >> 2) as u16
                } else {
                    u16::MAX
                },
            );
            compress_postinglist_rle(
                index,
                roaring_offset,
                &mut size_compressed_docid_key,
                *key0,
                *key_hash,
                runs_count_threshold,
                &mut runs_count,
                &key_body_offset,
                &mut compression_type_pointer,
            );
        }

        if runs_count == 0 {
            if enable_delta_compression
                && ((delta_compression_size_byte as usize) < (plo_posting_count << 1))
            {
            } else {
                compress_postinglist_array(
                    index,
                    roaring_offset,
                    &mut size_compressed_docid_key,
                    *key0,
                    *key_hash,
                    &key_body_offset,
                    &mut compression_type_pointer,
                );
            }
        }
    } else {
        if enable_rle_compression {
            let runs_count_threshold: u16 = cmp::min(
                2048,
                if enable_delta_compression {
                    (delta_compression_size_byte >> 2) as u16
                } else {
                    u16::MAX
                },
            );
            compress_postinglist_rle(
                index,
                roaring_offset,
                &mut size_compressed_docid_key,
                *key0,
                *key_hash,
                runs_count_threshold,
                &mut runs_count,
                &key_body_offset,
                &mut compression_type_pointer,
            );
        }

        if runs_count == 0 {
            if enable_delta_compression && (delta_compression_size_byte < 8192) {
            } else {
                compress_postinglist_bitmap(
                    index,
                    roaring_offset,
                    &mut size_compressed_docid_key,
                    *key0,
                    *key_hash,
                    &key_body_offset,
                    &mut compression_type_pointer,
                );
            }
        }
    }

    let plo = index.segments_level0[*key0]
        .segment
        .get_mut(key_hash)
        .unwrap();

    write_u64_ref(
        *key_hash,
        &mut index.compressed_index_segment_block_buffer,
        key_head_pointer_w,
    );

    write_u16_ref(
        (plo.posting_count - 1) as u16,
        &mut index.compressed_index_segment_block_buffer,
        key_head_pointer_w,
    );

    write_u16_ref(
        plo.max_docid,
        &mut index.compressed_index_segment_block_buffer,
        key_head_pointer_w,
    );

    write_u16_ref(
        plo.max_p_docid,
        &mut index.compressed_index_segment_block_buffer,
        key_head_pointer_w,
    );

    let bigram_term_index1 = if plo.is_bigram {
        index
            .frequent_words
            .binary_search(&plo.term_bigram1)
            .unwrap() as u8
    } else {
        255
    };
    write_u8_ref(
        bigram_term_index1,
        &mut index.compressed_index_segment_block_buffer,
        key_head_pointer_w,
    );

    let bigram_term_index2 = if plo.is_bigram {
        index
            .frequent_words
            .binary_search(&plo.term_bigram2)
            .unwrap() as u8
    } else {
        255
    };
    write_u8_ref(
        bigram_term_index2,
        &mut index.compressed_index_segment_block_buffer,
        key_head_pointer_w,
    );

    write_u16_ref(
        plo.pointer_pivot_p_docid,
        &mut index.compressed_index_segment_block_buffer,
        key_head_pointer_w,
    );

    write_u32_ref(
        compression_type_pointer,
        &mut index.compressed_index_segment_block_buffer,
        key_head_pointer_w,
    );

    size_compressed_docid_key
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn docid_iterator(
    index: &mut Index,
    posting_pointer_size: u8,
    next_pointer: &mut usize,
    key_position_pointer_w: &mut usize,
    key_rank_position_pointer_w: &mut usize,
    key0: usize,
    key_hash: u64,
    doc_id: &mut u16,
    size_compressed_positions_key: &mut usize,
    p_docid: usize,
) {
    let mut read_pointer = *next_pointer;
    *next_pointer = read_u32_ref(&index.postings_buffer, &mut read_pointer) as usize;

    *doc_id = read_u16_ref(&index.postings_buffer, &mut read_pointer);

    let position_size_byte_temp: u16 = read_u16_ref(&index.postings_buffer, &mut read_pointer);
    let embed_flag = position_size_byte_temp & 0b10000000_00000000 > 0;
    let position_size_byte = (position_size_byte_temp & 0b01111111_11111111) as usize;

    let plo = index.segments_level0[key0]
        .segment
        .get_mut(&key_hash)
        .unwrap();

    let mut field_vec: SmallVec<[(u16, usize); 2]> = SmallVec::new();
    let mut field_vec_bigram1 = SmallVec::new();
    let mut field_vec_bigram2 = SmallVec::new();

    decode_positions_commit(
        posting_pointer_size,
        embed_flag,
        &index.postings_buffer,
        read_pointer,
        plo.is_bigram,
        index.indexed_field_vec.len(),
        index.indexed_field_id_bits,
        index.indexed_field_id_mask,
        index.longest_field_id as u16,
        &mut field_vec,
        &mut field_vec_bigram1,
        &mut field_vec_bigram2,
    );

    if posting_pointer_size == 2 {
        if embed_flag {
            block_copy(
                &index.postings_buffer,
                read_pointer,
                &mut index.compressed_index_segment_block_buffer,
                *key_rank_position_pointer_w,
                position_size_byte,
            );

            *key_rank_position_pointer_w += 2;
        } else {
            *size_compressed_positions_key += position_size_byte;
            *key_position_pointer_w -= position_size_byte;

            index.compressed_index_segment_block_buffer[*key_rank_position_pointer_w] =
                (*size_compressed_positions_key & 255) as u8;
            *key_rank_position_pointer_w += 1;
            index.compressed_index_segment_block_buffer[*key_rank_position_pointer_w] =
                ((*size_compressed_positions_key >> 8) & 127) as u8;
            *key_rank_position_pointer_w += 1;

            block_copy(
                &index.postings_buffer,
                read_pointer,
                &mut index.compressed_index_segment_block_buffer,
                *key_position_pointer_w,
                position_size_byte,
            );
        }
    } else if posting_pointer_size == 3 {
        if embed_flag {
            block_copy(
                &index.postings_buffer,
                read_pointer,
                &mut index.compressed_index_segment_block_buffer,
                *key_rank_position_pointer_w,
                position_size_byte,
            );

            *key_rank_position_pointer_w += 3;
        } else {
            *size_compressed_positions_key += position_size_byte;
            *key_position_pointer_w -= position_size_byte;

            index.compressed_index_segment_block_buffer[*key_rank_position_pointer_w] =
                (*size_compressed_positions_key & 255) as u8;
            *key_rank_position_pointer_w += 1;
            index.compressed_index_segment_block_buffer[*key_rank_position_pointer_w] =
                ((*size_compressed_positions_key >> 8) & 255) as u8;
            *key_rank_position_pointer_w += 1;
            index.compressed_index_segment_block_buffer[*key_rank_position_pointer_w] =
                ((*size_compressed_positions_key >> 16) & 127) as u8;
            *key_rank_position_pointer_w += 1;

            block_copy(
                &index.postings_buffer,
                read_pointer,
                &mut index.compressed_index_segment_block_buffer,
                *key_position_pointer_w,
                position_size_byte,
            );
        }
    } else {
        println!("postingPointerSize exceeded: {}", posting_pointer_size);
    }

    if !plo.is_bigram || index.meta.similarity == SimilarityType::Bm25fProximity {
        let mut posting_score = 0.0;
        for field in field_vec.iter() {
            let document_length_compressed =
                index.document_length_compressed_array[field.0 as usize][*doc_id as usize];

            let document_length_normalized_doc =
                DOCUMENT_LENGTH_COMPRESSION[document_length_compressed as usize] as f32;
            let document_length_quotient_doc =
                document_length_normalized_doc / index.document_length_normalized_average;

            let tf = field.1 as f32;

            let weight = index.indexed_schema_vec[field.0 as usize].boost;

            posting_score += weight
                * ((tf * (K + 1.0) / (tf + (K * (1.0 - B + (B * document_length_quotient_doc)))))
                    + SIGMA);
        }

        if posting_score > plo.max_block_score {
            plo.max_block_score = posting_score;
            plo.max_docid = *doc_id;
            plo.max_p_docid = p_docid as u16;
        }
    } else {
        let idf_bigram1 = (((index.indexed_doc_count as f32 - plo.posting_count_bigram1 as f32
            + 0.5)
            / (plo.posting_count_bigram1 as f32 + 0.5))
            + 1.0)
            .ln();
        let idf_bigram2 = (((index.indexed_doc_count as f32 - plo.posting_count_bigram2 as f32
            + 0.5)
            / (plo.posting_count_bigram2 as f32 + 0.5))
            + 1.0)
            .ln();

        let mut posting_score = 0.0;
        for field in field_vec_bigram1.iter() {
            let document_length_compressed =
                index.document_length_compressed_array[field.0 as usize][*doc_id as usize];
            let document_length_normalized_doc =
                DOCUMENT_LENGTH_COMPRESSION[document_length_compressed as usize] as f32;
            let document_length_quotient_doc =
                document_length_normalized_doc / index.document_length_normalized_average;

            let tf_bigram1 = field.1 as f32;

            let weight = index.indexed_schema_vec[field.0 as usize].boost;

            posting_score += weight
                * idf_bigram1
                * ((tf_bigram1 * (K + 1.0)
                    / (tf_bigram1 + (K * (1.0 - B + (B * document_length_quotient_doc)))))
                    + SIGMA);
        }

        for field in field_vec_bigram2.iter() {
            let document_length_compressed =
                index.document_length_compressed_array[field.0 as usize][*doc_id as usize];
            let document_length_normalized_doc =
                DOCUMENT_LENGTH_COMPRESSION[document_length_compressed as usize] as f32;
            let document_length_quotient_doc =
                document_length_normalized_doc / index.document_length_normalized_average;

            let tf_bigram2 = field.1 as f32;

            let weight = index.indexed_schema_vec[field.0 as usize].boost;

            posting_score += weight
                * idf_bigram2
                * ((tf_bigram2 * (K + 1.0)
                    / (tf_bigram2 + (K * (1.0 - B + (B * document_length_quotient_doc)))))
                    + SIGMA);
        }

        if posting_score > plo.max_block_score {
            plo.max_block_score = posting_score;
            plo.max_docid = *doc_id;
            plo.max_p_docid = p_docid as u16;
        }
    }
}

/// Compress postinglist to array
pub(crate) fn compress_postinglist_array(
    index: &mut Index,
    roaring_offset: &mut usize,
    size_compressed_docid_key: &mut usize,
    key0: usize,
    key_hash: u64,
    key_body_offset: &u32,
    compression_type_pointer: &mut u32,
) {
    let plo = index.segments_level0[key0]
        .segment
        .get_mut(&key_hash)
        .unwrap();

    let key_rank_position_pointer_range = *roaring_offset + plo.size_compressed_positions_key;
    let mut key_position_pointer_w = key_rank_position_pointer_range;
    let mut key_rank_position_pointer_w = key_rank_position_pointer_range;
    let posting_pointer_size_sum = plo.pointer_pivot_p_docid as usize * 2
        + if (plo.pointer_pivot_p_docid as usize) < plo.posting_count {
            (plo.posting_count - plo.pointer_pivot_p_docid as usize) * 3
        } else {
            0
        };
    let key_docid_pointer_w =
        *roaring_offset + plo.size_compressed_positions_key + posting_pointer_size_sum;
    let mut size_compressed_positions_key = 0;

    let count_byte = plo.posting_count * 2;

    plo.compression_type_pointer = key_body_offset | ((CompressionType::Array as u32) << 30);
    *compression_type_pointer = plo.compression_type_pointer;

    let pointer_pivot_p_docid = plo.pointer_pivot_p_docid;
    let mut next_pointer = plo.pointer_first;
    for p_docid in 0..plo.posting_count {
        let plo_posting_pointer_size = if p_docid < pointer_pivot_p_docid as usize {
            2
        } else {
            3
        };
        let mut doc_id = 0;
        docid_iterator(
            index,
            plo_posting_pointer_size,
            &mut next_pointer,
            &mut key_position_pointer_w,
            &mut key_rank_position_pointer_w,
            key0,
            key_hash,
            &mut doc_id,
            &mut size_compressed_positions_key,
            p_docid,
        );

        write_u16(
            doc_id,
            &mut index.compressed_index_segment_block_buffer,
            key_docid_pointer_w + (p_docid * 2),
        );
    }

    *size_compressed_docid_key = count_byte;
    *roaring_offset = key_docid_pointer_w + count_byte;
}

/// Compress postinglist to bitmap
pub(crate) fn compress_postinglist_bitmap(
    index: &mut Index,
    roaring_offset: &mut usize,
    size_compressed_docid_key: &mut usize,
    key0: usize,
    key_hash: u64,
    key_body_offset: &u32,
    compression_type_pointer: &mut u32,
) {
    let plo = index.segments_level0[key0]
        .segment
        .get_mut(&key_hash)
        .unwrap();

    let key_rank_position_pointer_range = *roaring_offset + plo.size_compressed_positions_key;
    let mut key_position_pointer_w = key_rank_position_pointer_range;
    let mut key_rank_position_pointer_w = key_rank_position_pointer_range;
    let posting_pointer_size_sum = plo.pointer_pivot_p_docid as usize * 2
        + if (plo.pointer_pivot_p_docid as usize) < plo.posting_count {
            (plo.posting_count - plo.pointer_pivot_p_docid as usize) * 3
        } else {
            0
        };
    let key_docid_pointer_w =
        *roaring_offset + plo.size_compressed_positions_key + posting_pointer_size_sum;
    let mut size_compressed_positions_key = 0;

    let count_byte = 8192;

    plo.compression_type_pointer = key_body_offset | ((CompressionType::Bitmap as u32) << 30);
    *compression_type_pointer = plo.compression_type_pointer;

    index.compressed_index_segment_block_buffer
        [key_docid_pointer_w..key_docid_pointer_w + count_byte]
        .fill(0);

    let pointer_pivot_p_docid = plo.pointer_pivot_p_docid;
    let mut next_pointer = plo.pointer_first;
    for p_docid in 0..plo.posting_count {
        let plo_posting_pointer_size = if p_docid < pointer_pivot_p_docid as usize {
            2
        } else {
            3
        };

        let mut doc_id = 0;
        docid_iterator(
            index,
            plo_posting_pointer_size,
            &mut next_pointer,
            &mut key_position_pointer_w,
            &mut key_rank_position_pointer_w,
            key0,
            key_hash,
            &mut doc_id,
            &mut size_compressed_positions_key,
            p_docid,
        );

        let docid_pos = doc_id;
        let byte_pos = docid_pos >> 3;
        let bit_pos = docid_pos & 7;

        index.compressed_index_segment_block_buffer[key_docid_pointer_w + byte_pos as usize] |=
            1u8 << bit_pos;
    }

    *size_compressed_docid_key = count_byte;
    *roaring_offset = key_docid_pointer_w + count_byte;
}

/// Compress postinglist to RLE
#[allow(clippy::too_many_arguments)]
pub(crate) fn compress_postinglist_rle(
    index: &mut Index,
    roaring_offset: &mut usize,
    size_compressed_docid_key: &mut usize,
    key0: usize,
    key_hash: u64,
    runs_count_threshold: u16,
    runs_count: &mut u16,
    key_body_offset: &u32,
    compression_type_pointer: &mut u32,
) {
    let plo = index.segments_level0[key0]
        .segment
        .get_mut(&key_hash)
        .unwrap();

    *runs_count = 0;

    let mut run_start = 0;
    let mut run_length = 0;

    let key_rank_position_pointer_range = *roaring_offset + plo.size_compressed_positions_key;
    let mut key_position_pointer_w = key_rank_position_pointer_range;
    let mut key_rank_position_pointer_w = key_rank_position_pointer_range;
    let posting_pointer_size_sum = plo.pointer_pivot_p_docid as usize * 2
        + if (plo.pointer_pivot_p_docid as usize) < plo.posting_count {
            (plo.posting_count - plo.pointer_pivot_p_docid as usize) * 3
        } else {
            0
        };
    let mut key_docid_pointer_w =
        *roaring_offset + plo.size_compressed_positions_key + posting_pointer_size_sum;
    let key_docid_pointer_w_old = key_docid_pointer_w;
    let mut size_compressed_positions_key = 0;

    plo.compression_type_pointer = key_body_offset | ((CompressionType::Rle as u32) << 30);
    *compression_type_pointer = plo.compression_type_pointer;

    let mut doc_id_old = 0;
    let pointer_pivot_p_docid = plo.pointer_pivot_p_docid;
    let mut next_pointer = plo.pointer_first;
    for p_docid in 0..plo.posting_count {
        let plo_posting_pointer_size = if p_docid < pointer_pivot_p_docid as usize {
            2
        } else {
            3
        };

        let mut doc_id = 0;
        docid_iterator(
            index,
            plo_posting_pointer_size,
            &mut next_pointer,
            &mut key_position_pointer_w,
            &mut key_rank_position_pointer_w,
            key0,
            key_hash,
            &mut doc_id,
            &mut size_compressed_positions_key,
            p_docid,
        );

        if p_docid == 0 {
            run_start = doc_id;
        } else if doc_id_old + 1 == doc_id {
            run_length += 1;
        } else {
            write_u16(
                run_start,
                &mut index.compressed_index_segment_block_buffer,
                key_docid_pointer_w_old + (((*runs_count << 1) as usize + 1) * 2),
            );
            write_u16(
                run_length,
                &mut index.compressed_index_segment_block_buffer,
                key_docid_pointer_w_old + (((*runs_count << 1) as usize + 2) * 2),
            );
            key_docid_pointer_w += 4;

            run_start = doc_id;
            run_length = 0;
            *runs_count += 1;
        }

        if *runs_count >= runs_count_threshold {
            *runs_count = 0;
            return;
        }
        doc_id_old = doc_id;
    }

    write_u16(
        run_start,
        &mut index.compressed_index_segment_block_buffer,
        key_docid_pointer_w_old + (((*runs_count << 1) as usize + 1) * 2),
    );
    write_u16(
        run_length,
        &mut index.compressed_index_segment_block_buffer,
        key_docid_pointer_w_old + (((*runs_count << 1) as usize + 2) * 2),
    );

    *runs_count += 1;
    key_docid_pointer_w += 4;

    write_u16(
        *runs_count,
        &mut index.compressed_index_segment_block_buffer,
        key_docid_pointer_w_old,
    );
    key_docid_pointer_w += 2;

    *size_compressed_docid_key = key_docid_pointer_w - key_docid_pointer_w_old;
    *roaring_offset = key_docid_pointer_w;
}

/// Compress positions: input delta compressed vector, output VINT compressioned byte array
pub(crate) fn compress_positions(
    positions: &[u16],
    positions_compressed: &mut [u8],
    size_compressed_positions_pointer: &mut usize,
) {
    for delta in positions {
        if *delta < 128 {
            positions_compressed[*size_compressed_positions_pointer] = *delta as u8 | STOP_BIT;
            *size_compressed_positions_pointer += 1
        } else if *delta < 16_384 {
            positions_compressed[*size_compressed_positions_pointer] =
                (delta >> 7) as u8 & 0b01111111;
            *size_compressed_positions_pointer += 1;
            positions_compressed[*size_compressed_positions_pointer] =
                (delta & 0b01111111) as u8 | STOP_BIT;
            *size_compressed_positions_pointer += 1;
        } else {
            positions_compressed[*size_compressed_positions_pointer] =
                (delta >> 13) as u8 & 0b01111111;
            *size_compressed_positions_pointer += 1;
            positions_compressed[*size_compressed_positions_pointer] =
                (delta >> 7) as u8 & 0b01111111;
            *size_compressed_positions_pointer += 1;
            positions_compressed[*size_compressed_positions_pointer] =
                (delta & 0b01111111) as u8 | STOP_BIT;
            *size_compressed_positions_pointer += 1;
        }
    }
}
