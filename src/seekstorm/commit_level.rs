use ahash::AHashMap;
use memmap2::Mmap;
use std::io::{Seek, SeekFrom, Write};

use crate::{
    compress_postinglist::compress_postinglist,
    index::{
        update_list_max_impact_score, AccessType, BlockObjectIndex, Index, LevelIndex,
        PostingListObjectIndex, ROARING_BLOCK_SIZE, STOPWORDS,
    },
    utils::{block_copy_mut, write_u16, write_u32, write_u64},
};

pub(crate) const KEY_HEAD_SIZE: usize = 22;

impl Index {
    /// Flush level (64K documents) from HashMap in RAM to roaring bitmap compressed structure on Mmap or disk.
    /// commit_level is invoked automatically each time 64K documents are indexed as well as on server exit.
    /// It can also be invoked manually at any time.
    pub fn commit_level(&mut self, indexed_doc_count: usize) {
        if !self.level0_uncommitted {
            return;
        }

        if self.level_index.is_empty() {
            write_u16(
                self.longest_field_id as u16,
                &mut self.compressed_index_segment_block_buffer,
                0,
            );

            let _ = self
                .index_file
                .write(&self.compressed_index_segment_block_buffer[0..2]);
        }

        let document_length_compressed_array_pointer =
            self.index_file.stream_position().unwrap() as usize;

        for document_length_compressed_array in self.document_length_compressed_array.iter_mut() {
            let _ = self.index_file.write(document_length_compressed_array);
        }

        write_u64(
            indexed_doc_count as u64,
            &mut self.compressed_index_segment_block_buffer,
            0,
        );
        write_u64(
            self.positions_sum_normalized,
            &mut self.compressed_index_segment_block_buffer,
            8,
        );

        let _ = self
            .index_file
            .write(&self.compressed_index_segment_block_buffer[0..16]);

        let segment_head_position = self.index_file.stream_position().unwrap() as usize;
        self.index_file
            .seek(SeekFrom::Current((self.segment_number1 * 8) as i64))
            .unwrap();

        self.document_length_normalized_average =
            self.positions_sum_normalized as f32 / indexed_doc_count as f32;

        for k0 in 0..self.segment_number1 {
            let strip_compressed = self.commit_segment(k0);
            self.strip_compressed_sum += strip_compressed as u64;
            self.key_count_sum += self.segments_level0[k0].segment.len() as u64;
        }

        let mut document_length_compressed_array: Vec<[u8; ROARING_BLOCK_SIZE]> = Vec::new();
        if self.meta.access_type != AccessType::Mmap {
            for document_length_compressed_array_item in
                self.document_length_compressed_array.iter_mut()
            {
                document_length_compressed_array.push(*document_length_compressed_array_item);
            }
        }

        self.level_index.push(LevelIndex {
            document_length_compressed_array,
            docstore_pointer_docs: Vec::new(),
            docstore_pointer_docs_pointer: 0,
            document_length_compressed_array_pointer,
        });

        for document_length_compressed_array in self.document_length_compressed_array.iter_mut() {
            *document_length_compressed_array = [0; ROARING_BLOCK_SIZE];
        }

        let segment_head_position2 = self.index_file.stream_position().unwrap() as usize;
        self.index_file
            .seek(SeekFrom::Start(segment_head_position as u64))
            .unwrap();
        let segment_head_position3 =
            self.compressed_index_segment_block_buffer.len() - (self.segment_number1 * 8);
        let _ = self
            .index_file
            .write(&self.compressed_index_segment_block_buffer[segment_head_position3..]);
        self.index_file
            .seek(SeekFrom::Start(segment_head_position2 as u64))
            .unwrap();

        if !self.stored_field_names.is_empty() {
            self.commit_level_docstore();
        }

        self.index_file_mmap =
            unsafe { Mmap::map(&self.index_file).expect("Unable to create Mmap") };

        update_list_max_impact_score(self);

        self.compressed_index_segment_block_buffer = vec![0; 10_000_000];
        self.postings_buffer = vec![0; 400_000_000];

        self.postings_buffer_pointer = 0;
        self.strip_compressed_sum = 0;

        for segment in self.segments_level0.iter_mut() {
            segment.positions_compressed = Vec::new();
            segment.segment = AHashMap::new();
        }

        self.level0_uncommitted = false;
    }

    /// Flush a single segment from the key hash range partitioned level to RAM and disk
    pub(crate) fn commit_segment(&mut self, key0: usize) -> usize {
        let block_id = self.block_id as u32;

        let mut key_head_pointer_w: usize = 0;
        let segment_head_position = self.compressed_index_segment_block_buffer.len()
            - (self.segment_number1 * 8)
            + (key0 * 8)
            + 4;
        write_u32(
            self.segments_level0[key0].segment.len() as u32,
            &mut self.compressed_index_segment_block_buffer,
            segment_head_position,
        );

        let mut key_body_pointer_w: usize =
            key_head_pointer_w + (self.segments_level0[key0].segment.len() * KEY_HEAD_SIZE);
        let key_body_pointer_wstart: usize = key_body_pointer_w;

        let mut key_list: Vec<u64> = self.segments_level0[key0].segment.keys().cloned().collect();
        key_list.sort_unstable();
        for key in key_list.iter() {
            let plo = self.segments_level0[key0].segment.get_mut(key).unwrap();

            let mut key_position_pointer_w: usize = key_body_pointer_w;
            let key_rank_position_pointer_w: usize =
                key_body_pointer_w + plo.size_compressed_positions_key;

            let posting_pointer_size_sum = plo.pointer_pivot_p_docid as usize * 2
                + if (plo.pointer_pivot_p_docid as usize) < plo.posting_count {
                    (plo.posting_count - plo.pointer_pivot_p_docid as usize) * 3
                } else {
                    0
                };

            let size_compressed_positions_key =
                plo.size_compressed_positions_key + posting_pointer_size_sum;

            let key_docid_pointer_w: usize = key_body_pointer_w + size_compressed_positions_key;

            let mut size_compressed_docid_key;

            let key_body_offset =
                key_rank_position_pointer_w as u32 - key_body_pointer_wstart as u32;

            size_compressed_docid_key = compress_postinglist(
                self,
                &mut key_head_pointer_w,
                &mut key_position_pointer_w,
                key_body_offset,
                &key0,
                key,
            );

            key_body_pointer_w = key_docid_pointer_w + size_compressed_docid_key;
            size_compressed_docid_key += KEY_HEAD_SIZE;

            self.size_compressed_docid_index += size_compressed_docid_key as u64;
            self.size_compressed_positions_index += size_compressed_positions_key as u64;
        }

        let compressed_segment_block_size = key_body_pointer_w;

        let segment_head_position = self.compressed_index_segment_block_buffer.len()
            - (self.segment_number1 * 8)
            + (key0 * 8);
        write_u32(
            compressed_segment_block_size as u32,
            &mut self.compressed_index_segment_block_buffer,
            segment_head_position,
        );

        let file_position = self.index_file.stream_position().unwrap() as usize;

        let _ = self
            .index_file
            .write(&self.compressed_index_segment_block_buffer[0..compressed_segment_block_size]);

        if self.meta.access_type == AccessType::Mmap {
            self.segments_index[key0].byte_array_blocks_pointer.push((
                file_position + key_body_pointer_wstart,
                (compressed_segment_block_size - key_body_pointer_wstart),
                key_list.len() as u32,
            ));
        } else {
            let mut byte_array: Vec<u8> =
                vec![0; compressed_segment_block_size - key_body_pointer_wstart];
            block_copy_mut(
                &mut self.compressed_index_segment_block_buffer,
                key_body_pointer_wstart,
                &mut byte_array,
                0,
                compressed_segment_block_size - key_body_pointer_wstart,
            );

            self.segments_index[key0].byte_array_blocks.push(byte_array);

            for kv in self.segments_level0[key0].segment.iter_mut() {
                let plo = kv.1;

                let value = self.segments_index[key0].segment.entry(*kv.0).or_insert(
                    PostingListObjectIndex {
                        ..Default::default()
                    },
                );
                let exists: bool = value.posting_count > 0;

                if exists {
                    value.posting_count += plo.posting_count as u32;
                    if self.meta.access_type != AccessType::Mmap {
                        value.blocks.push(BlockObjectIndex {
                            block_id,
                            posting_count: (plo.posting_count - 1) as u16,
                            max_block_score: plo.max_block_score,
                            max_docid: plo.max_docid,
                            max_p_docid: plo.max_p_docid,
                            pointer_pivot_p_docid: plo.pointer_pivot_p_docid,
                            compression_type_pointer: plo.compression_type_pointer,
                        });
                    }
                } else {
                    value.posting_count = plo.posting_count as u32;
                    value.max_list_score = 0.0;
                    value.bigram_term_index1 = if plo.is_bigram {
                        STOPWORDS.binary_search(&plo.term_bigram1.as_str()).unwrap() as u8
                    } else {
                        255
                    };
                    value.bigram_term_index2 = if plo.is_bigram {
                        STOPWORDS.binary_search(&plo.term_bigram2.as_str()).unwrap() as u8
                    } else {
                        255
                    };

                    value.blocks = vec![BlockObjectIndex {
                        block_id,
                        posting_count: (plo.posting_count - 1) as u16,
                        max_block_score: plo.max_block_score,
                        max_docid: plo.max_docid,
                        max_p_docid: plo.max_p_docid,
                        pointer_pivot_p_docid: plo.pointer_pivot_p_docid,
                        compression_type_pointer: plo.compression_type_pointer,
                    }]
                }
            }
        }

        compressed_segment_block_size
    }
}
