use ahash::AHashMap;
use memmap2::{Mmap, MmapMut};
use num::FromPrimitive;
use num_format::{Locale, ToFormattedString};
use std::{
    fs::File,
    io::{Seek, SeekFrom, Write},
    path::Path,
    thread::available_parallelism,
};

use crate::{
    add_result::{
        B, DOCUMENT_LENGTH_COMPRESSION, K, decode_positions_multiterm_multifield,
        decode_positions_multiterm_singlefield, get_next_position_multifield,
        get_next_position_singlefield,
    },
    compatible::{_blsr_u64, _mm_tzcnt_64},
    compress_postinglist::compress_postinglist,
    index::{
        AccessType, BlockObjectIndex, CompressionType, FACET_VALUES_FILENAME, Index, IndexArc,
        LevelIndex, MAX_POSITIONS_PER_TERM, NonUniquePostingListObjectQuery, POSTING_BUFFER_SIZE,
        PostingListObjectIndex, PostingListObjectQuery, ROARING_BLOCK_SIZE, STOPWORDS, TermObject,
        update_list_max_impact_score, update_stopwords_posting_counts, warmup,
    },
    utils::{
        block_copy, block_copy_mut, read_u8, read_u16, read_u32, read_u64, write_u16, write_u32,
        write_u64,
    },
};

pub(crate) const KEY_HEAD_SIZE: usize = 22;

/// Commit moves indexed documents from the intermediate uncompressed data structure (array lists/HashMap, queryable by realtime search) in RAM
/// to the final compressed data structure (roaring bitmap) on Mmap or disk -
/// which is persistent, more compact, with lower query latency and allows search with realtime=false.
/// Commit is invoked automatically each time 64K documents are newly indexed as well as on close_index (e.g. server quit).
/// There is no way to prevent this automatic commit by not manually invoking it.
/// But commit can also be invoked manually at any time at any number of newly indexed documents.
/// commit is a **hard commit** for persistence on disk. A **soft commit** for searchability
/// is invoked implicitly with every index_doc,
/// i.e. the document can immediately searched and included in the search results
/// if it matches the query AND the query paramter realtime=true is enabled.
/// **Use commit with caution, as it is an expensive operation**.
/// **Usually, there is no need to invoke it manually**, as it is invoked automatically every 64k documents and when the index is closed with close_index.
/// Before terminating the program, always call close_index (commit), otherwise all documents indexed since last (manual or automatic) commit are lost.
/// There are only 2 reasons that justify a manual commit:
/// 1. if you want to search newly indexed documents without using realtime=true for search performance reasons or
/// 2. if after indexing new documents there won't be more documents indexed (for some time),
///    so there won't be (soon) a commit invoked automatically at the next 64k threshold or close_index,
///    but you still need immediate persistence guarantees on disk to protect against data loss in the event of a crash.
#[allow(async_fn_in_trait)]
pub trait Commit {
    /// Commit moves indexed documents from the intermediate uncompressed data structure (array lists/HashMap, queryable by realtime search) in RAM
    /// to the final compressed data structure (roaring bitmap) on Mmap or disk -
    /// which is persistent, more compact, with lower query latency and allows search with realtime=false.
    /// Commit is invoked automatically each time 64K documents are newly indexed as well as on close_index (e.g. server quit).
    /// There is no way to prevent this automatic commit by not manually invoking it.
    /// But commit can also be invoked manually at any time at any number of newly indexed documents.
    /// commit is a **hard commit** for persistence on disk. A **soft commit** for searchability
    /// is invoked implicitly with every index_doc,
    /// i.e. the document can immediately searched and included in the search results
    /// if it matches the query AND the query paramter realtime=true is enabled.
    /// **Use commit with caution, as it is an expensive operation**.
    /// **Usually, there is no need to invoke it manually**, as it is invoked automatically every 64k documents and when the index is closed with close_index.
    /// Before terminating the program, always call close_index (commit), otherwise all documents indexed since last (manual or automatic) commit are lost.
    /// There are only 2 reasons that justify a manual commit:
    /// 1. if you want to search newly indexed documents without using realtime=true for search performance reasons or
    /// 2. if after indexing new documents there won't be more documents indexed (for some time),
    ///    so there won't be (soon) a commit invoked automatically at the next 64k threshold or close_index,
    ///    but you still need immediate persistence guarantees on disk to protect against data loss in the event of a crash.
    async fn commit(&self);
}

/// Commit moves indexed documents from the intermediate uncompressed data structure (array lists/HashMap, queryable by realtime search) in RAM
/// to the final compressed data structure (roaring bitmap) on Mmap or disk -
/// which is persistent, more compact, with lower query latency and allows search with realtime=false.
/// Commit is invoked automatically each time 64K documents are newly indexed as well as on close_index (e.g. server quit).
/// There is no way to prevent this automatic commit by not manually invoking it.
/// But commit can also be invoked manually at any time at any number of newly indexed documents.
/// commit is a **hard commit** for persistence on disk. A **soft commit** for searchability
/// is invoked implicitly with every index_doc,
/// i.e. the document can immediately searched and included in the search results
/// if it matches the query AND the query paramter realtime=true is enabled.
/// **Use commit with caution, as it is an expensive operation**.
/// **Usually, there is no need to invoke it manually**, as it is invoked automatically every 64k documents and when the index is closed with close_index.
/// Before terminating the program, always call close_index (commit), otherwise all documents indexed since last (manual or automatic) commit are lost.
/// There are only 2 reasons that justify a manual commit:
/// 1. if you want to search newly indexed documents without using realtime=true for search performance reasons or
/// 2. if after indexing new documents there won't be more documents indexed (for some time),
///    so there won't be (soon) a commit invoked automatically at the next 64k threshold or close_index,
///    but you still need immediate persistence guarantees on disk to protect against data loss in the event of a crash.
impl Commit for IndexArc {
    /// Commit moves indexed documents from the intermediate uncompressed data structure (array lists/HashMap, queryable by realtime search) in RAM
    /// to the final compressed data structure (roaring bitmap) on Mmap or disk -
    /// which is persistent, more compact, with lower query latency and allows search with realtime=false.
    /// Commit is invoked automatically each time 64K documents are newly indexed as well as on close_index (e.g. server quit).
    /// There is no way to prevent this automatic commit by not manually invoking it.
    /// But commit can also be invoked manually at any time at any number of newly indexed documents.
    /// commit is a **hard commit** for persistence on disk. A **soft commit** for searchability
    /// is invoked implicitly with every index_doc,
    /// i.e. the document can immediately searched and included in the search results
    /// if it matches the query AND the query paramter realtime=true is enabled.
    /// **Use commit with caution, as it is an expensive operation**.
    /// **Usually, there is no need to invoke it manually**, as it is invoked automatically every 64k documents and when the index is closed with close_index.
    /// Before terminating the program, always call close_index (commit), otherwise all documents indexed since last (manual or automatic) commit are lost.
    /// There are only 2 reasons that justify a manual commit:
    /// 1. if you want to search newly indexed documents without using realtime=true for search performance reasons or
    /// 2. if after indexing new documents there won't be more documents indexed (for some time),
    ///    so there won't be (soon) a commit invoked automatically at the next 64k threshold or close_index,
    ///    but you still need immediate persistence guarantees on disk to protect against data loss in the event of a crash.
    async fn commit(&self) {
        let index_ref = self.read().await;
        let index_permits = index_ref.permits.clone();
        drop(index_ref);
        let thread_number = available_parallelism().unwrap().get();
        let mut permit_vec = Vec::new();
        for _i in 0..thread_number {
            permit_vec.push(index_permits.acquire().await.unwrap());
        }

        let mut index_mut = self.write().await;
        let indexed_doc_count = index_mut.indexed_doc_count;
        index_mut.commit(indexed_doc_count);
        drop(index_mut);
        warmup(self).await;
    }
}

impl Index {
    pub(crate) fn commit(&mut self, indexed_doc_count: usize) {
        if !self.uncommitted {
            return;
        }
        let new_document_count = indexed_doc_count - self.committed_doc_count;

        let is_last_level_incomplete = self.is_last_level_incomplete;
        if self.is_last_level_incomplete {
            self.merge_incomplete_index_level_to_level0();

            if let Err(e) = self
                .index_file
                .set_len(self.last_level_index_file_start_pos)
            {
                println!("Unable to index_file.set_len in commit {:?}", e)
            };
            let _ = self
                .index_file
                .seek(SeekFrom::Start(self.last_level_index_file_start_pos));

            let idx = self.level_index.len() - 1;
            if self.meta.access_type == AccessType::Mmap {
                self.index_file_mmap =
                    unsafe { Mmap::map(&self.index_file).expect("Unable to create Mmap") };

                for segment in self.segments_index.iter_mut() {
                    if idx == segment.byte_array_blocks_pointer.len() - 1 {
                        segment.byte_array_blocks_pointer.remove(idx);
                    }
                }
            } else {
                for segment in self.segments_index.iter_mut() {
                    if idx == segment.byte_array_blocks.len() - 1 {
                        segment.byte_array_blocks.remove(idx);
                    }
                }

                for key0 in 0..self.segment_number1 {
                    for item in self.segments_index[key0].segment.iter_mut() {
                        if let Some(block) = item.1.blocks.last() {
                            if block.block_id as usize == idx {
                                item.1.posting_count -= block.posting_count as u32 + 1;
                                item.1.blocks.remove(idx);
                            }
                        }
                    }
                    self.segments_index[key0]
                        .segment
                        .retain(|_key, value| !value.blocks.is_empty())
                }
            }
        } else {
            self.last_level_index_file_start_pos = self.index_file.stream_position().unwrap();
            self.last_level_docstore_file_start_pos = self.docstore_file.stream_position().unwrap();
        };

        update_stopwords_posting_counts(self, true);

        if self.committed_doc_count / ROARING_BLOCK_SIZE == 0 {
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

        for (i, component) in self.bm25_component_cache.iter_mut().enumerate() {
            let document_length_quotient =
                DOCUMENT_LENGTH_COMPRESSION[i] as f32 / self.document_length_normalized_average;
            *component = K * (1.0 - B + B * document_length_quotient);
        }

        for k0 in 0..self.segment_number1 {
            let strip_compressed = self.commit_segment(k0);
            self.strip_compressed_sum += strip_compressed as u64;
            self.key_count_sum += self.segments_level0[k0].segment.len() as u64;
        }

        if !is_last_level_incomplete {
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
                document_length_compressed_array_pointer,
                docstore_pointer_docs: Vec::new(),
                docstore_pointer_docs_pointer: 0,
            });
        }

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

        let _ = self.index_file.flush();

        self.index_file
            .seek(SeekFrom::Start(segment_head_position2 as u64))
            .unwrap();

        if !self.stored_field_names.is_empty() {
            self.commit_docstore(indexed_doc_count, is_last_level_incomplete);
        }

        if self.meta.access_type == AccessType::Mmap {
            self.index_file_mmap =
                unsafe { Mmap::map(&self.index_file).expect("Unable to create Mmap") };
        }

        if !self.facets.is_empty() {
            self.facets_file_mmap.flush().expect("Unable to flush Mmap");
            if self.facets_file.metadata().unwrap().len()
                != (self.facets_size_sum * (self.level_index.len() + 1) * ROARING_BLOCK_SIZE) as u64
            {
                if let Err(e) = self.facets_file.set_len(
                    (self.facets_size_sum * (self.level_index.len() + 1) * ROARING_BLOCK_SIZE)
                        as u64,
                ) {
                    println!("Unable to facets_file.set_len in commit {:?}", e)
                };

                self.facets_file_mmap =
                    unsafe { MmapMut::map_mut(&self.facets_file).expect("Unable to create Mmap") };
            }

            let index_path = Path::new(&self.index_path_string);
            serde_json::to_writer(
                &File::create(Path::new(index_path).join(FACET_VALUES_FILENAME)).unwrap(),
                &self.facets,
            )
            .unwrap();
        }

        self.string_set_to_single_term_id();

        update_list_max_impact_score(self);

        self.committed_doc_count = indexed_doc_count;
        self.is_last_level_incomplete = (self.committed_doc_count) % ROARING_BLOCK_SIZE > 0;
        if !self.mute {
            println!(
                "commit level {} committed documents {} {} mode {}",
                self.level_index.len(),
                new_document_count.to_formatted_string(&Locale::en),
                self.committed_doc_count.to_formatted_string(&Locale::en),
                if is_last_level_incomplete {
                    "merge"
                } else {
                    "append"
                }
            );
        }

        self.compressed_index_segment_block_buffer = vec![0; 10_000_000];
        self.postings_buffer = vec![0; POSTING_BUFFER_SIZE];

        self.postings_buffer_pointer = 0;
        self.strip_compressed_sum = 0;

        for segment in self.segments_level0.iter_mut() {
            segment.positions_compressed = Vec::new();
            segment.segment = AHashMap::new();
        }

        self.uncommitted = false;
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
            let mut byte_array_docid: Vec<u8> =
                vec![0; compressed_segment_block_size - key_body_pointer_wstart];
            block_copy_mut(
                &mut self.compressed_index_segment_block_buffer,
                key_body_pointer_wstart,
                &mut byte_array_docid,
                0,
                compressed_segment_block_size - key_body_pointer_wstart,
            );

            let mut posting_count_previous = 0;
            let mut pointer_pivot_p_docid_previous = 0;
            let mut compression_type_pointer_previous = 0;

            for (key_index, key) in key_list.iter().enumerate() {
                let plo = self.segments_level0[key0].segment.get_mut(key).unwrap();

                let value = self.segments_index[key0].segment.entry(*key).or_insert(
                    PostingListObjectIndex {
                        ..Default::default()
                    },
                );
                let exists: bool = value.posting_count > 0;

                if self.indexed_doc_count % ROARING_BLOCK_SIZE > 0
                    && self.meta.access_type == AccessType::Ram
                {
                    let position_range_previous = if key_index == 0 {
                        0
                    } else {
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
                                4 * read_u16(
                                    &byte_array_docid,
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

                    value.position_range_previous = position_range_previous;

                    posting_count_previous = plo.posting_count;
                    pointer_pivot_p_docid_previous = plo.pointer_pivot_p_docid;
                    compression_type_pointer_previous = plo.compression_type_pointer;
                };

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

                    if self.meta.access_type != AccessType::Mmap {
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

            self.segments_index[key0]
                .byte_array_blocks
                .push(byte_array_docid);
        }

        compressed_segment_block_size
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn add_docid(
        self: &mut Index,
        plo: &mut PostingListObjectQuery,
        docid: usize,
        key_hash: u64,
        key0: usize,
        is_bigram: bool,
        bigram_term_index1: u8,
        bigram_term_index2: u8,
    ) {
        let mut field_positions_vec: Vec<Vec<u16>> = vec![Vec::new(); self.indexed_field_vec.len()];

        if self.indexed_field_vec.len() == 1 {
            decode_positions_multiterm_singlefield(plo, true, true, false);

            let mut plo2 = NonUniquePostingListObjectQuery {
                positions_pointer: plo.positions_pointer as usize,
                is_embedded: plo.is_embedded,
                embedded_positions: plo.embedded_positions,
                field_vec: plo.field_vec.clone(),
                p_pos: 0,
                p_field: 0,
                positions_count: plo.positions_count,
                key0: key0 as u32,
                byte_array: plo.byte_array,
                term_index_unique: 0,
                term_index_nonunique: 0,
                pos: 0,
            };

            let mut prev_pos = 0;
            let mut one = 0;
            for _ in 0..plo.positions_count {
                plo2.pos = get_next_position_singlefield(&mut plo2);
                let pos = prev_pos + plo2.pos as u16 + one;
                field_positions_vec[0].push(pos);
                prev_pos = pos;
                one = 1;
                plo2.p_pos += 1;
            }
        } else {
            decode_positions_multiterm_multifield(self, plo, true, true, false);

            let mut plo2 = NonUniquePostingListObjectQuery {
                positions_pointer: plo.positions_pointer as usize,
                is_embedded: plo.is_embedded,
                embedded_positions: plo.embedded_positions,
                field_vec: plo.field_vec.clone(),
                p_pos: 0,
                p_field: 0,
                positions_count: plo.positions_count,
                key0: key0 as u32,
                byte_array: plo.byte_array,
                term_index_unique: 0,
                term_index_nonunique: 0,
                pos: 0,
            };

            for field in plo.field_vec.iter() {
                let mut prev_pos = 0;
                let mut one = 0;
                for _ in 0..field.1 {
                    plo2.pos = get_next_position_multifield(&mut plo2);
                    let pos = prev_pos + plo2.pos as u16 + one;
                    field_positions_vec[field.0 as usize].push(pos);
                    prev_pos = pos;
                    one = 1;
                    plo2.p_pos += 1;
                }
            }
        }

        let term = TermObject {
            key_hash,
            key0: key0 as u32,
            is_bigram,
            term_bigram1: if bigram_term_index1 == 255 {
                String::new()
            } else {
                STOPWORDS[bigram_term_index1 as usize].to_string()
            },
            term_bigram2: if bigram_term_index2 == 255 {
                String::new()
            } else {
                STOPWORDS[bigram_term_index2 as usize].to_string()
            },
            field_positions_vec,
            field_vec_bigram1: if is_bigram {
                if self.indexed_field_vec.len() == 1 {
                    vec![(0, plo.tf_bigram1)]
                } else {
                    plo.field_vec_bigram1
                        .iter()
                        .map(|field| (field.0 as usize, field.1 as u32))
                        .collect()
                }
            } else {
                Vec::new()
            },
            field_vec_bigram2: if is_bigram {
                if self.indexed_field_vec.len() == 1 {
                    vec![(0, plo.tf_bigram2)]
                } else {
                    plo.field_vec_bigram2
                        .iter()
                        .map(|field| (field.0 as usize, field.1 as u32))
                        .collect()
                }
            } else {
                Vec::new()
            },

            ..Default::default()
        };

        self.index_posting(term, docid, true);
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn iterate_docid(
        self: &mut Index,
        compression_type_pointer: u32,
        pointer_pivot_p_docid: u16,
        posting_count: u16,
        block_id: usize,
        key0: usize,
        key_hash: u64,
        is_bigram: bool,
        bigram_term_index1: u8,
        bigram_term_index2: u8,
    ) {
        let compression_type: CompressionType =
            FromPrimitive::from_i32((compression_type_pointer >> 30) as i32).unwrap();

        let rank_position_pointer_range: u32 =
            compression_type_pointer & 0b0011_1111_1111_1111_1111_1111_1111_1111;

        let posting_pointer_size_sum = pointer_pivot_p_docid as u32 * 2
            + if (pointer_pivot_p_docid as usize) <= posting_count as usize {
                ((posting_count as u32 + 1) - pointer_pivot_p_docid as u32) * 3
            } else {
                0
            };
        let compressed_doc_id_range: u32 = rank_position_pointer_range + posting_pointer_size_sum;

        let byte_array = if self.meta.access_type == AccessType::Mmap {
            let segment = &self.segments_index[key0];
            let byte_array = &self.index_file_mmap[segment.byte_array_blocks_pointer[block_id].0
                ..segment.byte_array_blocks_pointer[block_id].0
                    + segment.byte_array_blocks_pointer[block_id].1];
            byte_array.to_owned()
        } else {
            self.segments_index[key0].byte_array_blocks[block_id].to_owned()
        };

        let mut plo = PostingListObjectQuery {
            rank_position_pointer_range,
            pointer_pivot_p_docid,
            byte_array: &byte_array,
            p_docid: 0,
            is_bigram,
            ..Default::default()
        };

        match compression_type {
            CompressionType::Array => {
                for i in 0..=posting_count {
                    plo.p_docid = i as usize;

                    let docid = (block_id << 16)
                        | read_u16(
                            &byte_array[compressed_doc_id_range as usize..],
                            i as usize * 2,
                        ) as usize;

                    self.add_docid(
                        &mut plo,
                        docid,
                        key_hash,
                        key0,
                        is_bigram,
                        bigram_term_index1,
                        bigram_term_index2,
                    );
                }
            }

            CompressionType::Rle => {
                let runs_count =
                    read_u16(&byte_array[compressed_doc_id_range as usize..], 0) as i32;

                plo.p_docid = 0;
                for i in (1..(runs_count << 1) + 1).step_by(2) {
                    let startdocid = read_u16(
                        &byte_array[compressed_doc_id_range as usize..],
                        i as usize * 2,
                    );
                    let runlength = read_u16(
                        &byte_array[compressed_doc_id_range as usize..],
                        (i + 1) as usize * 2,
                    );

                    for j in 0..=runlength {
                        let docid = (block_id << 16) | (startdocid + j) as usize;
                        self.add_docid(
                            &mut plo,
                            docid,
                            key_hash,
                            key0,
                            is_bigram,
                            bigram_term_index1,
                            bigram_term_index2,
                        );

                        plo.p_docid += 1;
                    }
                }
            }

            CompressionType::Bitmap => {
                plo.p_docid = 0;

                for ulong_pos in 0u64..1024 {
                    let mut intersect: u64 = read_u64(
                        &byte_array[compressed_doc_id_range as usize..],
                        ulong_pos as usize * 8,
                    );

                    while intersect != 0 {
                        let bit_pos = unsafe { _mm_tzcnt_64(intersect) } as u64;

                        intersect = unsafe { _blsr_u64(intersect) };

                        let docid = (block_id << 16) | ((ulong_pos << 6) + bit_pos) as usize;

                        self.add_docid(
                            &mut plo,
                            docid,
                            key_hash,
                            key0,
                            is_bigram,
                            bigram_term_index1,
                            bigram_term_index2,
                        );

                        plo.p_docid += 1;
                    }
                }
            }

            _ => {}
        }
    }

    pub(crate) fn merge_incomplete_index_level_to_level0(self: &mut Index) {
        for strip0 in self.segments_level0.iter_mut() {
            if strip0.positions_compressed.is_empty() {
                strip0.positions_compressed = vec![0; MAX_POSITIONS_PER_TERM * 2];
            }
        }

        let block_id = self.level_index.len() - 1;
        let committed_doc_count = (self.committed_doc_count - 1 % ROARING_BLOCK_SIZE) + 1;

        for i in 0..self.indexed_field_vec.len() {
            if self.meta.access_type == AccessType::Mmap {
                block_copy(
                    &self.index_file_mmap[self.level_index[block_id]
                        .document_length_compressed_array_pointer
                        + i * ROARING_BLOCK_SIZE..],
                    0,
                    &mut self.document_length_compressed_array[i],
                    0,
                    committed_doc_count,
                );
            } else {
                block_copy(
                    &self.level_index[block_id].document_length_compressed_array[i],
                    0,
                    &mut self.document_length_compressed_array[i],
                    0,
                    committed_doc_count,
                );
            }
        }

        for key0 in 0..self.segment_number1 {
            if self.meta.access_type == AccessType::Mmap {
                let pointer = self.segments_index[key0].byte_array_blocks_pointer[block_id];

                let key_count = pointer.2 as usize;

                for key_index in 0..key_count {
                    let key_address;
                    let key_hash;
                    let posting_count;
                    let bigram_term_index1;
                    let bigram_term_index2;
                    let pointer_pivot_p_docid_old;
                    let compression_type_pointer;
                    {
                        let byte_array = &self.index_file_mmap
                            [pointer.0 - (key_count * KEY_HEAD_SIZE)..pointer.0];
                        key_address = key_index * KEY_HEAD_SIZE;
                        key_hash = read_u64(byte_array, key_address);
                        posting_count = read_u16(byte_array, key_address + 8);
                        bigram_term_index1 = read_u8(byte_array, key_address + 14);
                        bigram_term_index2 = read_u8(byte_array, key_address + 15);

                        pointer_pivot_p_docid_old = read_u16(byte_array, key_address + 16);
                        compression_type_pointer = read_u32(byte_array, key_address + 18);
                    }
                    let is_bigram = bigram_term_index1 < 255;

                    let mut pointer_pivot_p_docid_new = 0;
                    let mut size_compressed_positions_key_new = 0;
                    let mut pointer_first_new = 0;
                    let mut pointer_last_new = 0;
                    let mut pointer_first_old = 0;
                    let merge = match self.segments_level0[key0].segment.get_mut(&key_hash) {
                        Some(plo0) => {
                            pointer_pivot_p_docid_new = plo0.pointer_pivot_p_docid;
                            size_compressed_positions_key_new = plo0.size_compressed_positions_key;
                            plo0.pointer_pivot_p_docid = 0;
                            plo0.size_compressed_positions_key = 0;

                            pointer_first_new = plo0.pointer_first;
                            pointer_last_new = plo0.pointer_last;
                            pointer_first_old = self.postings_buffer_pointer;
                            true
                        }
                        None => false,
                    };

                    self.iterate_docid(
                        compression_type_pointer,
                        pointer_pivot_p_docid_old,
                        posting_count,
                        block_id,
                        key0,
                        key_hash,
                        is_bigram,
                        bigram_term_index1,
                        bigram_term_index2,
                    );

                    if merge {
                        let plo0 = self.segments_level0[key0]
                            .segment
                            .get_mut(&key_hash)
                            .unwrap();

                        plo0.pointer_pivot_p_docid = if pointer_pivot_p_docid_new == 0 {
                            pointer_pivot_p_docid_old
                        } else {
                            pointer_pivot_p_docid_old + pointer_pivot_p_docid_new
                        };

                        plo0.size_compressed_positions_key = size_compressed_positions_key_new;

                        let pointer_last_old = plo0.pointer_last;
                        plo0.pointer_first = pointer_first_old;
                        plo0.pointer_last = pointer_last_new;

                        write_u32(
                            pointer_first_new as u32,
                            &mut self.postings_buffer,
                            pointer_last_old,
                        );
                    }
                }
            } else {
                let keys: Vec<u64> = self.segments_index[key0].segment.keys().cloned().collect();

                for key_hash in keys {
                    let plo = &self.segments_index[key0].segment[&key_hash];
                    let last_block = plo.blocks.last().unwrap();
                    if last_block.block_id as usize != self.level_index.len() - 1 {
                        continue;
                    }

                    let posting_count = last_block.posting_count;

                    let bigram_term_index1 = plo.bigram_term_index1;
                    let bigram_term_index2 = plo.bigram_term_index2;

                    let pointer_pivot_p_docid = last_block.pointer_pivot_p_docid;
                    let compression_type_pointer = last_block.compression_type_pointer;

                    let is_bigram = bigram_term_index1 < 255;

                    let mut pointer_pivot_p_docid_new = 0;
                    let mut size_compressed_positions_key_new = 0;
                    let mut pointer_first_new = 0;
                    let mut pointer_last_new = 0;
                    let mut pointer_first_old = 0;
                    let merge = match self.segments_level0[key0].segment.get_mut(&key_hash) {
                        Some(plo0) => {
                            pointer_pivot_p_docid_new = plo0.pointer_pivot_p_docid;
                            size_compressed_positions_key_new = plo0.size_compressed_positions_key;
                            plo0.pointer_pivot_p_docid = 0;
                            plo0.size_compressed_positions_key = 0;

                            pointer_first_new = plo0.pointer_first;
                            pointer_last_new = plo0.pointer_last;
                            pointer_first_old = self.postings_buffer_pointer;
                            true
                        }
                        None => false,
                    };

                    self.iterate_docid(
                        compression_type_pointer,
                        pointer_pivot_p_docid,
                        posting_count,
                        block_id,
                        key0,
                        key_hash,
                        is_bigram,
                        bigram_term_index1,
                        bigram_term_index2,
                    );

                    if merge {
                        let plo0 = self.segments_level0[key0]
                            .segment
                            .get_mut(&key_hash)
                            .unwrap();

                        plo0.pointer_pivot_p_docid = if pointer_pivot_p_docid_new == 0 {
                            pointer_pivot_p_docid
                        } else {
                            pointer_pivot_p_docid + pointer_pivot_p_docid_new
                        };
                        plo0.size_compressed_positions_key = size_compressed_positions_key_new;

                        let pointer_last_old = plo0.pointer_last;
                        plo0.pointer_first = pointer_first_old;
                        plo0.pointer_last = pointer_last_new;

                        write_u32(
                            pointer_first_new as u32,
                            &mut self.postings_buffer,
                            pointer_last_old,
                        );
                    }
                }
            }
        }
    }
}
