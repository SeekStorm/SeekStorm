use std::cmp;

use num::FromPrimitive;

use crate::{
    compress_postinglist::compress_positions,
    index::{
        AccessType, CompressionType, FIELD_STOP_BIT_1, FIELD_STOP_BIT_2, NgramType,
        POSTING_BUFFER_SIZE, PostingListObject0, ROARING_BLOCK_SIZE, STOP_BIT, Shard, TermObject,
    },
    search::binary_search,
    utils::{block_copy_mut, read_u16, read_u32, write_u16_ref, write_u32},
};

impl Shard {
    pub(crate) fn index_posting(
        &mut self,
        term: TermObject,
        doc_id: usize,
        restore: bool,
        posting_count_ngram_1_compressed: u8,
        posting_count_ngram_2_compressed: u8,
        posting_count_ngram_3_compressed: u8,
    ) {
        if let Some(spelling_correction) = self.meta.spelling_correction.as_ref()
            && term.key_hash & 7 == 0
            && (spelling_correction.term_length_threshold.as_ref().is_none()
                || spelling_correction
                    .term_length_threshold
                    .as_ref()
                    .unwrap()
                    .is_empty()
                || term.term.len()
                    >= spelling_correction.term_length_threshold.as_ref().unwrap()[0])
        {
            let sum: usize = term
                .field_positions_vec
                .iter()
                .map(|field| field.len())
                .sum();
            if sum > 1 {
                _ = self
                    .level_terms
                    .entry((term.key_hash >> 32) as u32)
                    .or_insert(term.term.clone());
            }
        };

        let mut positions_count_sum = 0;
        let mut field_positions_vec: Vec<Vec<u16>> = Vec::new();
        for positions_uncompressed in term.field_positions_vec.iter() {
            positions_count_sum += positions_uncompressed.len();
            let mut positions: Vec<u16> = Vec::new();
            let mut previous_position: u16 = 0;
            for pos in positions_uncompressed.iter() {
                if positions.is_empty() {
                    positions.push(*pos);
                } else {
                    positions.push(*pos - previous_position - 1);
                }
                previous_position = *pos;
            }
            field_positions_vec.push(positions);
        }

        if positions_count_sum == 0 {
            println!("empty posting {} docid {}", term.term, doc_id);
            return;
        }

        if self.postings_buffer_pointer > self.postings_buffer.len() - (POSTING_BUFFER_SIZE >> 4) {
            self.postings_buffer
                .resize(self.postings_buffer.len() + (POSTING_BUFFER_SIZE >> 2), 0);
        }

        let strip_object0 = self.segments_level0.get_mut(term.key0 as usize).unwrap();

        let value = strip_object0
            .segment
            .entry(term.key_hash)
            .or_insert(PostingListObject0 {
                posting_count_ngram_1_compressed,
                posting_count_ngram_2_compressed,
                posting_count_ngram_3_compressed,
                ..Default::default()
            });
        let exists: bool = value.posting_count > 0;

        if self.is_last_level_incomplete && !exists && !restore {
            if self.meta.access_type == AccessType::Mmap {
                let pointer = self.segments_index[term.key0 as usize]
                    .byte_array_blocks_pointer
                    .last()
                    .unwrap();

                let key_count = pointer.2 as usize;

                let byte_array_keys =
                    &self.index_file_mmap[pointer.0 - (key_count * self.key_head_size)..pointer.0];
                let key_index = binary_search(
                    byte_array_keys,
                    key_count,
                    term.key_hash,
                    self.key_head_size,
                );

                if key_index >= 0 {
                    let key_address = key_index as usize * self.key_head_size;
                    let compression_type_pointer =
                        read_u32(byte_array_keys, key_address + self.key_head_size - 4);
                    let rank_position_pointer_range =
                        compression_type_pointer & 0b0011_1111_1111_1111_1111_1111_1111_1111;

                    let position_range_previous = if key_index == 0 {
                        0
                    } else {
                        let posting_count_previous =
                            read_u16(byte_array_keys, key_address + 8 - self.key_head_size)
                                as usize
                                + 1;
                        let pointer_pivot_p_docid_previous =
                            read_u16(byte_array_keys, key_address - 6);

                        let posting_pointer_size_sum_previous = pointer_pivot_p_docid_previous
                            as usize
                            * 2
                            + if (pointer_pivot_p_docid_previous as usize) < posting_count_previous
                            {
                                (posting_count_previous - pointer_pivot_p_docid_previous as usize)
                                    * 3
                            } else {
                                0
                            };

                        let compression_type_pointer_previous =
                            read_u32(byte_array_keys, key_address + 18 - self.key_head_size);
                        let rank_position_pointer_range_previous = compression_type_pointer_previous
                            & 0b0011_1111_1111_1111_1111_1111_1111_1111;
                        let compression_type_previous: CompressionType = FromPrimitive::from_i32(
                            (compression_type_pointer_previous >> 30) as i32,
                        )
                        .unwrap();

                        let compressed_docid_previous = match compression_type_previous {
                            CompressionType::Array => posting_count_previous * 2,
                            CompressionType::Bitmap => 8192,
                            CompressionType::Rle => {
                                let block_id = doc_id >> 16;
                                let segment: &crate::index::SegmentIndex =
                                    &self.segments_index[term.key0 as usize];
                                let byte_array_docid = &self.index_file_mmap[segment
                                    .byte_array_blocks_pointer[block_id]
                                    .0
                                    ..segment.byte_array_blocks_pointer[block_id].0
                                        + segment.byte_array_blocks_pointer[block_id].1];

                                4 * read_u16(
                                    byte_array_docid,
                                    rank_position_pointer_range_previous as usize
                                        + posting_pointer_size_sum_previous,
                                ) as usize
                                    + 2
                            }
                            _ => 0,
                        };

                        rank_position_pointer_range_previous
                            + (posting_pointer_size_sum_previous + compressed_docid_previous) as u32
                    };

                    value.size_compressed_positions_key =
                        (rank_position_pointer_range - position_range_previous) as usize;
                }
            } else {
                let posting_list_object_index_option = self.segments_index[term.key0 as usize]
                    .segment
                    .get(&term.key_hash);

                if let Some(plo) = posting_list_object_index_option {
                    let block = plo.blocks.last().unwrap();
                    if block.block_id as usize == self.level_index.len() - 1 {
                        let rank_position_pointer_range: u32 = block.compression_type_pointer
                            & 0b0011_1111_1111_1111_1111_1111_1111_1111;

                        value.size_compressed_positions_key =
                            (rank_position_pointer_range - plo.position_range_previous) as usize;
                    }
                };
            }
        }

        let mut posting_pointer_size =
            if value.size_compressed_positions_key < 32_768 && value.posting_count < 65_535 {
                value.pointer_pivot_p_docid = value.posting_count as u16 + 1;
                2u8
            } else {
                3u8
            };

        let mut nonempty_field_count = 0;
        let mut only_longest_field = true;
        for (field_id, item) in field_positions_vec.iter().enumerate() {
            if !item.is_empty() {
                nonempty_field_count += 1;

                if !self.indexed_field_vec[field_id].is_longest_field {
                    only_longest_field = false;
                }
            }
        }

        let mut positions_meta_compressed_nonembedded_size = 0;

        match term.ngram_type {
            NgramType::SingleTerm => {}
            NgramType::NgramFF | NgramType::NgramFR | NgramType::NgramRF => {
                for (i, field) in term.field_vec_ngram1.iter().enumerate() {
                    if field_positions_vec.len() == 1 {
                        positions_meta_compressed_nonembedded_size += if field.1 < 128 {
                            1
                        } else if field.1 < 16_384 {
                            2
                        } else {
                            3
                        };
                    } else if term.field_vec_ngram1.len() == 1
                        && term.field_vec_ngram1[0].0 == self.longest_field_id
                    {
                        positions_meta_compressed_nonembedded_size += if field.1 < 64 {
                            1
                        } else if field.1 < 8_192 {
                            2
                        } else {
                            3
                        };
                    } else {
                        let required_position_count_bits = u32::BITS - field.1.leading_zeros();
                        let only_longest_field_bit = if i == 0 { 1 } else { 0 };
                        let meta_bits = only_longest_field_bit
                            + required_position_count_bits
                            + self.indexed_field_id_bits as u32;

                        if meta_bits <= 6 {
                            positions_meta_compressed_nonembedded_size += 1;
                        } else if meta_bits <= 13 {
                            positions_meta_compressed_nonembedded_size += 2;
                        } else if meta_bits <= 20 {
                            positions_meta_compressed_nonembedded_size += 3;
                        }
                    }
                }
                for (i, field) in term.field_vec_ngram2.iter().enumerate() {
                    if field_positions_vec.len() == 1 {
                        positions_meta_compressed_nonembedded_size += if field.1 < 128 {
                            1
                        } else if field.1 < 16_384 {
                            2
                        } else {
                            3
                        };
                    } else if term.field_vec_ngram2.len() == 1
                        && term.field_vec_ngram2[0].0 == self.longest_field_id
                    {
                        positions_meta_compressed_nonembedded_size += if field.1 < 64 {
                            1
                        } else if field.1 < 8_192 {
                            2
                        } else {
                            3
                        };
                    } else {
                        let required_position_count_bits = u32::BITS - field.1.leading_zeros();
                        let only_longest_field_bit = if i == 0 { 1 } else { 0 };
                        let meta_bits = only_longest_field_bit
                            + required_position_count_bits
                            + self.indexed_field_id_bits as u32;

                        if meta_bits <= 6 {
                            positions_meta_compressed_nonembedded_size += 1;
                        } else if meta_bits <= 13 {
                            positions_meta_compressed_nonembedded_size += 2;
                        } else if meta_bits <= 20 {
                            positions_meta_compressed_nonembedded_size += 3;
                        }
                    }
                }
            }
            _ => {
                for (i, field) in term.field_vec_ngram1.iter().enumerate() {
                    if field_positions_vec.len() == 1 {
                        positions_meta_compressed_nonembedded_size += if field.1 < 128 {
                            1
                        } else if field.1 < 16_384 {
                            2
                        } else {
                            3
                        };
                    } else if term.field_vec_ngram1.len() == 1
                        && term.field_vec_ngram1[0].0 == self.longest_field_id
                    {
                        positions_meta_compressed_nonembedded_size += if field.1 < 64 {
                            1
                        } else if field.1 < 8_192 {
                            2
                        } else {
                            3
                        };
                    } else {
                        let required_position_count_bits = u32::BITS - field.1.leading_zeros();
                        let only_longest_field_bit = if i == 0 { 1 } else { 0 };
                        let meta_bits = only_longest_field_bit
                            + required_position_count_bits
                            + self.indexed_field_id_bits as u32;

                        if meta_bits <= 6 {
                            positions_meta_compressed_nonembedded_size += 1;
                        } else if meta_bits <= 13 {
                            positions_meta_compressed_nonembedded_size += 2;
                        } else if meta_bits <= 20 {
                            positions_meta_compressed_nonembedded_size += 3;
                        }
                    }
                }
                for (i, field) in term.field_vec_ngram2.iter().enumerate() {
                    if field_positions_vec.len() == 1 {
                        positions_meta_compressed_nonembedded_size += if field.1 < 128 {
                            1
                        } else if field.1 < 16_384 {
                            2
                        } else {
                            3
                        };
                    } else if term.field_vec_ngram2.len() == 1
                        && term.field_vec_ngram2[0].0 == self.longest_field_id
                    {
                        positions_meta_compressed_nonembedded_size += if field.1 < 64 {
                            1
                        } else if field.1 < 8_192 {
                            2
                        } else {
                            3
                        };
                    } else {
                        let required_position_count_bits = u32::BITS - field.1.leading_zeros();
                        let only_longest_field_bit = if i == 0 { 1 } else { 0 };
                        let meta_bits = only_longest_field_bit
                            + required_position_count_bits
                            + self.indexed_field_id_bits as u32;

                        if meta_bits <= 6 {
                            positions_meta_compressed_nonembedded_size += 1;
                        } else if meta_bits <= 13 {
                            positions_meta_compressed_nonembedded_size += 2;
                        } else if meta_bits <= 20 {
                            positions_meta_compressed_nonembedded_size += 3;
                        }
                    }
                }
                for (i, field) in term.field_vec_ngram3.iter().enumerate() {
                    if field_positions_vec.len() == 1 {
                        positions_meta_compressed_nonembedded_size += if field.1 < 128 {
                            1
                        } else if field.1 < 16_384 {
                            2
                        } else {
                            3
                        };
                    } else if term.field_vec_ngram3.len() == 1
                        && term.field_vec_ngram3[0].0 == self.longest_field_id
                    {
                        positions_meta_compressed_nonembedded_size += if field.1 < 64 {
                            1
                        } else if field.1 < 8_192 {
                            2
                        } else {
                            3
                        };
                    } else {
                        let required_position_count_bits = u32::BITS - field.1.leading_zeros();
                        let only_longest_field_bit = if i == 0 { 1 } else { 0 };
                        let meta_bits = only_longest_field_bit
                            + required_position_count_bits
                            + self.indexed_field_id_bits as u32;

                        if meta_bits <= 6 {
                            positions_meta_compressed_nonembedded_size += 1;
                        } else if meta_bits <= 13 {
                            positions_meta_compressed_nonembedded_size += 2;
                        } else if meta_bits <= 20 {
                            positions_meta_compressed_nonembedded_size += 3;
                        }
                    }
                }
            }
        }

        let mut positions_sum = 0;
        let mut positions_vec: Vec<u16> = Vec::new();
        let mut field_vec: Vec<(usize, u32)> = Vec::new();
        for (field_id, field) in field_positions_vec.iter().enumerate() {
            if !field.is_empty() {
                if field_positions_vec.len() == 1 {
                    positions_meta_compressed_nonembedded_size += if field.len() < 128 {
                        1
                    } else if field.len() < 16_384 {
                        2
                    } else {
                        3
                    };
                } else if only_longest_field {
                    positions_meta_compressed_nonembedded_size += if field.len() < 64 {
                        1
                    } else if field.len() < 8_192 {
                        2
                    } else {
                        3
                    };
                } else {
                    let required_position_count_bits = usize::BITS - field.len().leading_zeros();
                    let only_longest_field_bit = if field_vec.is_empty() { 1 } else { 0 };

                    let meta_bits = only_longest_field_bit
                        + required_position_count_bits
                        + self.indexed_field_id_bits as u32;

                    if meta_bits <= 6 {
                        positions_meta_compressed_nonembedded_size += 1;
                    } else if meta_bits <= 13 {
                        positions_meta_compressed_nonembedded_size += 2;
                    } else if meta_bits <= 20 {
                        positions_meta_compressed_nonembedded_size += 3;
                    }
                }

                positions_sum += field.len();
                if self.indexed_field_vec.len() > 1 && field.len() <= 4 {
                    positions_vec.append(&mut field.clone())
                };

                field_vec.push((field_id, field.len() as u32));
            }
        }

        let mut embed_flag = term.ngram_type == NgramType::SingleTerm;

        if self.indexed_field_vec.len() == 1 {
            if posting_pointer_size == 2 {
                embed_flag &= positions_sum <= 2
                    && ((positions_sum == 1
                        && u16::BITS - field_positions_vec[0][0].leading_zeros() <= 14)
                        || (positions_sum == 2
                            && u16::BITS - field_positions_vec[0][0].leading_zeros() <= 7
                            && u16::BITS - field_positions_vec[0][1].leading_zeros() <= 7));
            } else {
                embed_flag &= positions_sum <= 4
                    && ((positions_sum == 1
                        && u16::BITS - field_positions_vec[0][0].leading_zeros() <= 21)
                        || (positions_sum == 2
                            && u16::BITS - field_positions_vec[0][0].leading_zeros() <= 10
                            && u16::BITS - field_positions_vec[0][1].leading_zeros() <= 11)
                        || (positions_sum == 3
                            && u16::BITS - field_positions_vec[0][0].leading_zeros() <= 7
                            && u16::BITS - field_positions_vec[0][1].leading_zeros() <= 7
                            && u16::BITS - field_positions_vec[0][2].leading_zeros() <= 7)
                        || (positions_sum == 4
                            && u16::BITS - field_positions_vec[0][0].leading_zeros() <= 5
                            && u16::BITS - field_positions_vec[0][1].leading_zeros() <= 5
                            && u16::BITS - field_positions_vec[0][2].leading_zeros() <= 5
                            && u16::BITS - field_positions_vec[0][3].leading_zeros() <= 6));
            }
        } else if only_longest_field {
            if posting_pointer_size == 2 {
                embed_flag &= positions_sum <= 2
                    && ((positions_sum == 1 && u16::BITS - positions_vec[0].leading_zeros() <= 13)
                        || (positions_sum == 2
                            && u16::BITS - positions_vec[0].leading_zeros() <= 6
                            && u16::BITS - positions_vec[1].leading_zeros() <= 7));
            } else {
                embed_flag &= positions_sum <= 4
                    && ((positions_sum == 1 && u16::BITS - positions_vec[0].leading_zeros() <= 20)
                        || (positions_sum == 2
                            && u16::BITS - positions_vec[0].leading_zeros() <= 10
                            && u16::BITS - positions_vec[1].leading_zeros() <= 10)
                        || (positions_sum == 3
                            && u16::BITS - positions_vec[0].leading_zeros() <= 6
                            && u16::BITS - positions_vec[1].leading_zeros() <= 7
                            && u16::BITS - positions_vec[2].leading_zeros() <= 7)
                        || (positions_sum == 4
                            && u16::BITS - positions_vec[0].leading_zeros() <= 5
                            && u16::BITS - positions_vec[1].leading_zeros() <= 5
                            && u16::BITS - positions_vec[2].leading_zeros() <= 5
                            && u16::BITS - positions_vec[3].leading_zeros() <= 5));
            }
        } else {
            let used_bits = nonempty_field_count * self.indexed_field_id_bits as u32;
            let bits = if posting_pointer_size == 2 { 12 } else { 19 };
            let remaining_bits_new = if used_bits < bits {
                bits - used_bits
            } else {
                embed_flag = false;
                0
            };

            if posting_pointer_size == 2 {
                embed_flag &= positions_sum <= 3
                    && ((positions_sum == 1
                        && u16::BITS - positions_vec[0].leading_zeros() <= remaining_bits_new)
                        || (positions_sum == 2
                            && u16::BITS - positions_vec[0].leading_zeros()
                                <= remaining_bits_new / 2
                            && u16::BITS - positions_vec[1].leading_zeros()
                                <= remaining_bits_new - remaining_bits_new / 2)
                        || (positions_sum == 3
                            && nonempty_field_count == 1
                            && u16::BITS - positions_vec[0].leading_zeros()
                                <= remaining_bits_new / 3
                            && u16::BITS - positions_vec[1].leading_zeros()
                                <= (remaining_bits_new - remaining_bits_new / 3) / 2
                            && u16::BITS - positions_vec[2].leading_zeros()
                                <= remaining_bits_new
                                    - (remaining_bits_new - remaining_bits_new / 3) / 2
                                    - (remaining_bits_new / 3)));
            } else {
                embed_flag &= positions_sum <= 4
                    && ((positions_sum == 1
                        && u16::BITS - positions_vec[0].leading_zeros() <= remaining_bits_new)
                        || (positions_sum == 2
                            && u16::BITS - positions_vec[0].leading_zeros()
                                <= remaining_bits_new / 2
                            && u16::BITS - positions_vec[1].leading_zeros()
                                <= remaining_bits_new - remaining_bits_new / 2)
                        || (positions_sum == 3
                            && u16::BITS - positions_vec[0].leading_zeros()
                                <= remaining_bits_new / 3
                            && u16::BITS - positions_vec[1].leading_zeros()
                                <= (remaining_bits_new - remaining_bits_new / 3) / 2
                            && u16::BITS - positions_vec[2].leading_zeros()
                                <= remaining_bits_new
                                    - (remaining_bits_new - remaining_bits_new / 3) / 2
                                    - (remaining_bits_new / 3))
                        || (positions_sum == 4
                            && nonempty_field_count == 1
                            && u16::BITS - positions_vec[0].leading_zeros()
                                <= remaining_bits_new / 4
                            && u16::BITS - positions_vec[1].leading_zeros()
                                <= (remaining_bits_new - remaining_bits_new / 4) / 3
                            && u16::BITS - positions_vec[2].leading_zeros()
                                <= (remaining_bits_new
                                    - (remaining_bits_new - remaining_bits_new / 4) / 3
                                    - (remaining_bits_new / 4))
                                    / 2
                            && u16::BITS - positions_vec[3].leading_zeros()
                                <= remaining_bits_new
                                    - remaining_bits_new / 4
                                    - (remaining_bits_new - remaining_bits_new / 4) / 3
                                    - (remaining_bits_new
                                        - (remaining_bits_new - remaining_bits_new / 4) / 3
                                        - (remaining_bits_new / 4))
                                        / 2));
            }
        };

        let mut write_pointer_base = self.postings_buffer_pointer;
        let mut write_pointer = self.postings_buffer_pointer + 8;

        let mut positions_compressed_pointer = 0usize;
        let positions_stack = if embed_flag {
            0
        } else {
            for field_positions in field_positions_vec.iter() {
                compress_positions(
                    field_positions,
                    &mut strip_object0.positions_compressed,
                    &mut positions_compressed_pointer,
                );
            }

            let exceeded = posting_pointer_size == 2
                && (value.size_compressed_positions_key
                    + positions_meta_compressed_nonembedded_size
                    + positions_compressed_pointer
                    >= 32_768);
            if exceeded {
                posting_pointer_size = 3;
                value.pointer_pivot_p_docid = value.posting_count as u16;
            }

            positions_meta_compressed_nonembedded_size + positions_compressed_pointer
        };

        let compressed_position_size = if embed_flag {
            let mut positions_vec: Vec<u16> = Vec::new();
            let mut data: u32 = 0;
            for field in field_vec.iter() {
                for pos in field_positions_vec[field.0].iter() {
                    positions_vec.push(*pos);
                }
                if self.indexed_field_vec.len() > 1 && !only_longest_field {
                    data <<= self.indexed_field_id_bits;
                    data |= field.0 as u32;
                }
            }

            let mut remaining_bits = posting_pointer_size as usize * 8
                - if posting_pointer_size == 2 { 0 } else { 1 }
                - if self.indexed_field_vec.len() == 1 {
                    2
                } else if only_longest_field {
                    3
                } else {
                    4 + nonempty_field_count as usize * self.indexed_field_id_bits
                };
            for (i, position) in positions_vec.iter().enumerate() {
                let position_bits = remaining_bits / (positions_vec.len() - i);
                remaining_bits -= position_bits;
                data <<= position_bits;
                data |= *position as u32;
            }

            if posting_pointer_size == 2 {
                self.postings_buffer[write_pointer] = (data & 0b11111111) as u8;
                if self.indexed_field_vec.len() == 1 {
                    self.postings_buffer[write_pointer + 1] =
                        (data >> 8) as u8 | 0b10000000 | ((positions_vec.len() - 1) << 6) as u8;
                } else if only_longest_field {
                    self.postings_buffer[write_pointer + 1] =
                        (data >> 8) as u8 | 0b11000000 | ((positions_vec.len() - 1) << 5) as u8;
                } else if nonempty_field_count == 1 {
                    self.postings_buffer[write_pointer + 1] =
                        (data >> 8) as u8 | 0b10000000 | ((positions_vec.len() - 1) << 4) as u8;
                } else {
                    self.postings_buffer[write_pointer + 1] = (data >> 8) as u8 | 0b10110000;
                };
            } else {
                self.postings_buffer[write_pointer] = (data & 0b11111111) as u8;
                self.postings_buffer[write_pointer + 1] = ((data >> 8) & 0b11111111) as u8;
                if self.indexed_field_vec.len() == 1 {
                    self.postings_buffer[write_pointer + 2] =
                        (data >> 16) as u8 | 0b10000000 | ((positions_vec.len() - 1) << 5) as u8;
                } else if only_longest_field {
                    self.postings_buffer[write_pointer + 2] =
                        (data >> 16) as u8 | 0b11000000 | ((positions_vec.len() - 1) << 4) as u8;
                } else {
                    self.postings_buffer[write_pointer + 2] = (data >> 16) as u8
                        | 0b10000000
                        | if nonempty_field_count == 1 {
                            ((positions_vec.len() - 1) << 3) as u8
                        } else if nonempty_field_count == 3 {
                            0b00111000
                        } else if field_vec[0].1 == 1 && field_vec[1].1 == 1 {
                            0b00100000
                        } else if field_vec[0].1 == 1 && field_vec[1].1 == 2 {
                            0b00101000
                        } else {
                            0b00110000
                        };
                }
            }

            write_pointer += posting_pointer_size as usize;
            posting_pointer_size as usize
        } else {
            let write_pointer_start = write_pointer;

            match term.ngram_type {
                NgramType::SingleTerm => {}
                NgramType::NgramFF | NgramType::NgramFR | NgramType::NgramRF => {
                    write_field_vec(
                        &mut self.postings_buffer,
                        &mut write_pointer,
                        &term.field_vec_ngram1,
                        self.indexed_field_vec.len(),
                        term.field_vec_ngram1.len() == 1
                            && term.field_vec_ngram1[0].0 == self.longest_field_id,
                        term.field_vec_ngram1.len() as u32,
                        self.indexed_field_id_bits as u32,
                    );
                    write_field_vec(
                        &mut self.postings_buffer,
                        &mut write_pointer,
                        &term.field_vec_ngram2,
                        self.indexed_field_vec.len(),
                        term.field_vec_ngram2.len() == 1
                            && term.field_vec_ngram2[0].0 == self.longest_field_id,
                        term.field_vec_ngram2.len() as u32,
                        self.indexed_field_id_bits as u32,
                    );
                }
                _ => {
                    write_field_vec(
                        &mut self.postings_buffer,
                        &mut write_pointer,
                        &term.field_vec_ngram1,
                        self.indexed_field_vec.len(),
                        term.field_vec_ngram1.len() == 1
                            && term.field_vec_ngram1[0].0 == self.longest_field_id,
                        term.field_vec_ngram1.len() as u32,
                        self.indexed_field_id_bits as u32,
                    );
                    write_field_vec(
                        &mut self.postings_buffer,
                        &mut write_pointer,
                        &term.field_vec_ngram2,
                        self.indexed_field_vec.len(),
                        term.field_vec_ngram2.len() == 1
                            && term.field_vec_ngram2[0].0 == self.longest_field_id,
                        term.field_vec_ngram2.len() as u32,
                        self.indexed_field_id_bits as u32,
                    );
                    write_field_vec(
                        &mut self.postings_buffer,
                        &mut write_pointer,
                        &term.field_vec_ngram3,
                        self.indexed_field_vec.len(),
                        term.field_vec_ngram3.len() == 1
                            && term.field_vec_ngram3[0].0 == self.longest_field_id,
                        term.field_vec_ngram3.len() as u32,
                        self.indexed_field_id_bits as u32,
                    );
                }
            }

            write_field_vec(
                &mut self.postings_buffer,
                &mut write_pointer,
                &field_vec,
                self.indexed_field_vec.len(),
                only_longest_field,
                nonempty_field_count,
                self.indexed_field_id_bits as u32,
            );

            block_copy_mut(
                &mut strip_object0.positions_compressed,
                0,
                &mut self.postings_buffer,
                write_pointer,
                positions_compressed_pointer,
            );

            write_pointer += positions_compressed_pointer;
            write_pointer - write_pointer_start
        };

        let docid_lsb = (doc_id & 0xFFFF) as u16;
        if exists {
            value.posting_count += 1;
            value.position_count += positions_count_sum;
            value.size_compressed_positions_key += positions_stack;
            if docid_lsb > value.docid_old {
                value.docid_delta_max =
                    cmp::max(value.docid_delta_max, docid_lsb - value.docid_old - 1);
            }
            value.docid_old = docid_lsb;

            write_u32(
                write_pointer_base as u32,
                &mut self.postings_buffer,
                value.pointer_last,
            );

            value.pointer_last = write_pointer_base;
        } else if term.ngram_type == NgramType::NgramFF
            || term.ngram_type == NgramType::NgramRF
            || term.ngram_type == NgramType::NgramFR
        {
            *value = PostingListObject0 {
                pointer_first: write_pointer_base,
                pointer_last: write_pointer_base,
                posting_count: 1,
                position_count: positions_count_sum,
                ngram_type: term.ngram_type.clone(),
                term_ngram1: term.term_ngram_1,
                term_ngram2: term.term_ngram_0,
                term_ngram3: term.term_ngram_2,
                size_compressed_positions_key: value.size_compressed_positions_key
                    + positions_stack,
                docid_delta_max: docid_lsb,
                docid_old: docid_lsb,
                ..*value
            };
        } else {
            *value = PostingListObject0 {
                pointer_first: write_pointer_base,
                pointer_last: write_pointer_base,
                posting_count: 1,
                position_count: positions_count_sum,
                ngram_type: term.ngram_type.clone(),
                term_ngram1: term.term_ngram_2,
                term_ngram2: term.term_ngram_1,
                term_ngram3: term.term_ngram_0,
                size_compressed_positions_key: value.size_compressed_positions_key
                    + positions_stack,
                docid_delta_max: docid_lsb,
                docid_old: docid_lsb,
                ..*value
            };
        }

        write_pointer_base += 4;

        write_u16_ref(
            docid_lsb,
            &mut self.postings_buffer,
            &mut write_pointer_base,
        );

        if positions_compressed_pointer + 2 > ROARING_BLOCK_SIZE {
            println!(
                "compressed positions size exceeded: {}",
                positions_compressed_pointer + 2
            )
        };

        if !embed_flag && positions_stack != compressed_position_size {
            println!(
                "size conflict: embed {} term {} ngram_type {:?} frequent {} pos_count {} : positions_stack {} compressed_position_size {} : positions_compressed_pointer {} posting_pointer_size {} docid {}",
                embed_flag,
                term.term,
                term.ngram_type,
                only_longest_field,
                positions_count_sum,
                positions_stack,
                compressed_position_size,
                positions_compressed_pointer,
                posting_pointer_size,
                doc_id
            );
        }

        write_u16_ref(
            if embed_flag {
                compressed_position_size | 0b10000000_00000000
            } else {
                compressed_position_size & 0b01111111_11111111
            } as u16,
            &mut self.postings_buffer,
            &mut write_pointer_base,
        );

        self.postings_buffer_pointer = write_pointer;
    }
}

pub(crate) fn write_field_vec(
    postings_buffer: &mut [u8],
    write_pointer: &mut usize,
    field_vec: &[(usize, u32)],
    indexed_field_vec_len: usize,
    only_longest_field: bool,
    nonempty_field_count: u32,
    indexed_field_id_bits: u32,
) {
    for (i, field) in field_vec.iter().enumerate() {
        if indexed_field_vec_len == 1 {
            if field.1 < 128 {
                postings_buffer[*write_pointer] = field.1 as u8 | STOP_BIT;
                *write_pointer += 1;
            } else if field.1 < 16_384 {
                postings_buffer[*write_pointer] = (field.1 >> 7) as u8;
                *write_pointer += 1;
                postings_buffer[*write_pointer] = (field.1 & 0b01111111) as u8 | STOP_BIT;
                *write_pointer += 1;
            } else if field.1 < 2_097_152 {
                postings_buffer[*write_pointer] = (field.1 >> 14) as u8;
                *write_pointer += 1;
                postings_buffer[*write_pointer] = ((field.1 >> 7) & 0b01111111) as u8;
                *write_pointer += 1;

                postings_buffer[*write_pointer] = (field.1 & 0b01111111) as u8 | STOP_BIT;
                *write_pointer += 1;
            } else {
                println!("positionCount exceeded1: {}", field.1);
            }
        } else if only_longest_field {
            if field.1 < 64 {
                postings_buffer[*write_pointer] = field.1 as u8 | 0b11000000;
                *write_pointer += 1;
            } else if field.1 < 8_192 {
                postings_buffer[*write_pointer] = (field.1 >> 7) as u8 | 0b01000000;
                *write_pointer += 1;
                postings_buffer[*write_pointer] = (field.1 & 0b01111111) as u8 | STOP_BIT;
                *write_pointer += 1;
            } else if field.1 < 1_048_576 {
                postings_buffer[*write_pointer] = (field.1 >> 14) as u8 | 0b01000000;
                *write_pointer += 1;
                postings_buffer[*write_pointer] = ((field.1 >> 7) & 0b01111111) as u8;
                *write_pointer += 1;

                postings_buffer[*write_pointer] = (field.1 & 0b01111111) as u8 | STOP_BIT;
                *write_pointer += 1;
            } else {
                println!("positionCount exceeded2: {}", field.1);
            }
        } else {
            let field_stop_bit = if i == nonempty_field_count as usize - 1 {
                if i == 0 {
                    FIELD_STOP_BIT_1
                } else {
                    FIELD_STOP_BIT_2
                }
            } else {
                0b00000000
            };

            let required_position_count_bits = u32::BITS - field.1.leading_zeros();

            let field_id_position_count = ((field.1 as usize) << indexed_field_id_bits) | field.0;
            let only_longest_field_bit = if i == 0 { 1 } else { 0 };
            let meta_bits =
                only_longest_field_bit + required_position_count_bits + indexed_field_id_bits;

            if meta_bits <= 6 {
                postings_buffer[*write_pointer] =
                    field_stop_bit | field_id_position_count as u8 | STOP_BIT;
                *write_pointer += 1;
            } else if meta_bits <= 13 {
                postings_buffer[*write_pointer] =
                    field_stop_bit | (field_id_position_count >> 7) as u8;
                *write_pointer += 1;
                postings_buffer[*write_pointer] =
                    (field_id_position_count & 0b01111111) as u8 | STOP_BIT;
                *write_pointer += 1;
            } else if meta_bits <= 20 {
                postings_buffer[*write_pointer] =
                    field_stop_bit | (field_id_position_count >> 14) as u8;
                *write_pointer += 1;
                postings_buffer[*write_pointer] =
                    ((field_id_position_count >> 7) & 0b01111111) as u8;
                *write_pointer += 1;
                postings_buffer[*write_pointer] =
                    (field_id_position_count & 0b01111111) as u8 | STOP_BIT;
                *write_pointer += 1;
            } else {
                println!("positionCount exceeded3: {} ", field_id_position_count);
            }
        }
    }
}
