use std::cmp::Ordering;

use itertools::Itertools;
use num::integer::Roots;
#[cfg(target_arch = "aarch64")]
use std::arch::aarch64::*;
#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;

use crate::{
    index::{Clustering, Shard},
    vector::{Embedding, EmbeddingView, Precision, Quantization, VectorHeader, read_record},
    vector_similarity::{
        QuerySimd, VectorSimilarity, quantize_f32_to_i8, quantize_f32_to_i8_simd,
        similarity_embedding, similarity_embedding_simd,
    },
};

#[derive(Clone, Debug)]
pub(crate) struct Centroid {
    pub medoid_index: usize,
    pub child_count: usize,
    pub sum_vector: Vec<f32>,
    pub centroid: Embedding,
    pub medoid_index_new: usize,
    pub query_simd: QuerySimd,
    pub best_similarity: f32,
    pub has_changed: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct Medoid {
    pub medoid_index: usize,
    pub child_count: usize,
}

#[derive(Clone, Copy)]
pub(crate) struct ClusterHeader {
    pub start_index: u32,
    pub child_count: u32,
}

#[derive(Clone)]
pub(crate) struct ParentMedoid {
    pub medoid_index: usize,
    pub is_medoid: bool,
    pub similarity: f32,

    pub doc_id: u16,
    pub field_id: u32,
    pub chunk_id: u32,
    pub embedding: Embedding,
    pub scale: f32,
    pub norm: f32,
    pub zero_point: i16,
    pub sum_q: i32,
}

#[cfg(target_arch = "x86_64")]
unsafe fn accumulate_f32_avx2(sum: &mut [f32], emb: &[f32]) {
    unsafe {
        let len = emb.len();

        let mut i = 0;
        while i + 32 <= len {
            let v0 = _mm256_loadu_ps(emb.as_ptr().add(i));
            let v1 = _mm256_loadu_ps(emb.as_ptr().add(i + 8));
            let v2 = _mm256_loadu_ps(emb.as_ptr().add(i + 16));
            let v3 = _mm256_loadu_ps(emb.as_ptr().add(i + 24));

            let s0 = _mm256_loadu_ps(sum.as_ptr().add(i));
            let s1 = _mm256_loadu_ps(sum.as_ptr().add(i + 8));
            let s2 = _mm256_loadu_ps(sum.as_ptr().add(i + 16));
            let s3 = _mm256_loadu_ps(sum.as_ptr().add(i + 24));

            _mm256_storeu_ps(sum.as_mut_ptr().add(i), _mm256_add_ps(s0, v0));
            _mm256_storeu_ps(sum.as_mut_ptr().add(i + 8), _mm256_add_ps(s1, v1));
            _mm256_storeu_ps(sum.as_mut_ptr().add(i + 16), _mm256_add_ps(s2, v2));
            _mm256_storeu_ps(sum.as_mut_ptr().add(i + 24), _mm256_add_ps(s3, v3));

            i += 32;
        }

        for j in i..len {
            *sum.get_unchecked_mut(j) += *emb.get_unchecked(j);
        }
    }
}

#[cfg(target_arch = "x86_64")]
unsafe fn accumulate_i8_avx2(sum: &mut [f32], emb: &[i8]) {
    unsafe {
        let len = emb.len();

        let mut i = 0;
        while i + 32 <= len {
            let bytes = _mm256_loadu_si256(emb.as_ptr().add(i) as *const __m256i);

            let low128 = _mm256_castsi256_si128(bytes);
            let high128 = _mm256_extracti128_si256(bytes, 1);

            let low_lo = _mm256_cvtepi8_epi32(low128);
            let low_hi = _mm256_cvtepi8_epi32(_mm_srli_si128(low128, 8));
            let high_lo = _mm256_cvtepi8_epi32(high128);
            let high_hi = _mm256_cvtepi8_epi32(_mm_srli_si128(high128, 8));

            let f0 = _mm256_cvtepi32_ps(low_lo);
            let f1 = _mm256_cvtepi32_ps(low_hi);
            let f2 = _mm256_cvtepi32_ps(high_lo);
            let f3 = _mm256_cvtepi32_ps(high_hi);

            let s0 = _mm256_loadu_ps(sum.as_ptr().add(i));
            let s1 = _mm256_loadu_ps(sum.as_ptr().add(i + 8));
            let s2 = _mm256_loadu_ps(sum.as_ptr().add(i + 16));
            let s3 = _mm256_loadu_ps(sum.as_ptr().add(i + 24));

            _mm256_storeu_ps(sum.as_mut_ptr().add(i), _mm256_add_ps(s0, f0));
            _mm256_storeu_ps(sum.as_mut_ptr().add(i + 8), _mm256_add_ps(s1, f1));
            _mm256_storeu_ps(sum.as_mut_ptr().add(i + 16), _mm256_add_ps(s2, f2));
            _mm256_storeu_ps(sum.as_mut_ptr().add(i + 24), _mm256_add_ps(s3, f3));

            i += 32;
        }

        for j in i..len {
            *sum.get_unchecked_mut(j) += emb.get_unchecked(j).to_owned() as f32;
        }
    }
}

#[cfg(target_arch = "x86_64")]
pub(crate) fn accumulate_avx2(sum: &mut [f32], emb: &Embedding) {
    match emb {
        Embedding::I8(emb) => unsafe { accumulate_i8_avx2(sum, emb) },
        Embedding::F32(emb) => unsafe { accumulate_f32_avx2(sum, emb) },
    }
}

#[cfg(target_arch = "aarch64")]
unsafe fn accumulate_f32_neon(sum: &mut [f32], emb: &[f32]) {
    unsafe {
        let len = emb.len();
        let mut i = 0;
        while i + 16 <= len {
            let v0 = vld1q_f32(emb.as_ptr().add(i));
            let v1 = vld1q_f32(emb.as_ptr().add(i + 4));
            let v2 = vld1q_f32(emb.as_ptr().add(i + 8));
            let v3 = vld1q_f32(emb.as_ptr().add(i + 12));
            let s0 = vld1q_f32(sum.as_ptr().add(i));
            let s1 = vld1q_f32(sum.as_ptr().add(i + 4));
            let s2 = vld1q_f32(sum.as_ptr().add(i + 8));
            let s3 = vld1q_f32(sum.as_ptr().add(i + 12));
            vst1q_f32(sum.as_mut_ptr().add(i), vaddq_f32(s0, v0));
            vst1q_f32(sum.as_mut_ptr().add(i + 4), vaddq_f32(s1, v1));
            vst1q_f32(sum.as_mut_ptr().add(i + 8), vaddq_f32(s2, v2));
            vst1q_f32(sum.as_mut_ptr().add(i + 12), vaddq_f32(s3, v3));
            i += 16;
        }
        for j in i..len {
            *sum.get_unchecked_mut(j) += *emb.get_unchecked(j);
        }
    }
}

#[cfg(target_arch = "aarch64")]
unsafe fn accumulate_i8_neon(sum: &mut [f32], emb: &[i8]) {
    unsafe {
        let len = emb.len();
        let mut i = 0;
        while i + 16 <= len {
            let bytes = vld1q_s8(emb.as_ptr().add(i));
            let lo_i16 = vmovl_s8(vget_low_s8(bytes));
            let hi_i16 = vmovl_s8(vget_high_s8(bytes));
            let lo_lo = vcvtq_f32_s32(vmovl_s16(vget_low_s16(lo_i16)));
            let lo_hi = vcvtq_f32_s32(vmovl_s16(vget_high_s16(lo_i16)));
            let hi_lo = vcvtq_f32_s32(vmovl_s16(vget_low_s16(hi_i16)));
            let hi_hi = vcvtq_f32_s32(vmovl_s16(vget_high_s16(hi_i16)));
            let s0 = vld1q_f32(sum.as_ptr().add(i));
            let s1 = vld1q_f32(sum.as_ptr().add(i + 4));
            let s2 = vld1q_f32(sum.as_ptr().add(i + 8));
            let s3 = vld1q_f32(sum.as_ptr().add(i + 12));
            vst1q_f32(sum.as_mut_ptr().add(i), vaddq_f32(s0, lo_lo));
            vst1q_f32(sum.as_mut_ptr().add(i + 4), vaddq_f32(s1, lo_hi));
            vst1q_f32(sum.as_mut_ptr().add(i + 8), vaddq_f32(s2, hi_lo));
            vst1q_f32(sum.as_mut_ptr().add(i + 12), vaddq_f32(s3, hi_hi));
            i += 16;
        }
        for j in i..len {
            *sum.get_unchecked_mut(j) += *emb.get_unchecked(j) as f32;
        }
    }
}

#[cfg(target_arch = "aarch64")]
pub fn accumulate_neon(sum: &mut [f32], emb: &Embedding) {
    match emb {
        Embedding::I8(emb) => unsafe { accumulate_i8_neon(sum, emb) },
        Embedding::F32(emb) => unsafe { accumulate_f32_neon(sum, emb) },
    }
}

#[inline(always)]
pub(crate) fn accumulate_simd(sum: &mut [f32], emb: &Embedding) {
    #[cfg(target_arch = "x86_64")]
    {
        accumulate_avx2(sum, emb)
    }
    #[cfg(target_arch = "aarch64")]
    {
        accumulate_neon(sum, emb)
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        accumulate(sum, emb)
    }
}

pub(crate) fn accumulate(sum: &mut [f32], emb: &Embedding) {
    match emb {
        Embedding::I8(emb) => sum
            .iter_mut()
            .zip(emb.iter())
            .for_each(|(a, b)| *a += *b as f32),
        Embedding::F32(emb) => sum.iter_mut().zip(emb.iter()).for_each(|(a, b)| *a += *b),
    }
}

impl Shard {
    pub(crate) async fn _fill_vector_shard(&mut self, level: usize) {
        let query_string = "rosy panther";
        let index = self.index_option.as_ref().unwrap().read().await;

        let vector_type = index.vector_precision;
        let vector_dimensions = index.vector_dimensions;
        let vector_size = size_of::<VectorHeader>()
            + (vector_dimensions
                * match vector_type {
                    Precision::F32 => 4,
                    Precision::I8 => 1,
                    Precision::None => 0,
                });

        let query_embedding = index
            .embedding_model_option
            .as_ref()
            .unwrap()
            .encode(&[query_string.to_string()])
            .remove(0);
        let query_embedding = if self.is_simd {
            unsafe { quantize_f32_to_i8_simd(&query_embedding) }
        } else {
            quantize_f32_to_i8(&query_embedding)
        };
        let _query_simd = unsafe { QuerySimd::new(&query_embedding) };

        self.block_vector_buffer.clear();

        let _sum_vector = vec![0i32; vector_dimensions];

        let mut offset = 0;
        for level_id in 0..self.level_index.len() {
            let cluster_number_bytes = &self.vector_file_mmap[offset..offset + 4];
            let cluster_number =
                u32::from_le_bytes(cluster_number_bytes.try_into().unwrap()) as usize;
            offset += 4;

            let mut clusters = Vec::with_capacity(cluster_number);
            let mut level_vectors_count = 0;
            let mut start_index = 0;
            for _i in 0..cluster_number {
                let cluster_header_bytes = &self.vector_file_mmap[offset..offset + 4];
                let cluster_header = ClusterHeader {
                    start_index,
                    child_count: u32::from_le_bytes(cluster_header_bytes.try_into().unwrap()),
                };
                offset += 4;
                start_index += cluster_header.child_count;
                clusters.push(cluster_header);
                level_vectors_count += cluster_header.child_count;
            }

            if level_id == level {
                for cluster in clusters.iter() {
                    let cluster_vectors_count = cluster.child_count as usize;
                    let cluster_offset = cluster.start_index as usize * vector_size;

                    for vector_id in 0..cluster_vectors_count {
                        let record = read_record(
                            &self.vector_file_mmap
                                [offset + cluster_offset + (vector_id * vector_size)..],
                            vector_dimensions,
                            vector_type,
                        );

                        self.block_vector_buffer.push(ParentMedoid {
                            medoid_index: 0,
                            similarity: 0.0,
                            is_medoid: false,
                            scale: 0.0,
                            norm: 0.0,
                            zero_point: 0,
                            sum_q: 0,

                            doc_id: record.header.doc_id,
                            field_id: record.header.field_id,
                            chunk_id: record.header.chunk_id,
                            embedding: match record.embedding {
                                EmbeddingView::I8(e) => Embedding::I8(e.to_vec()),
                                EmbeddingView::F32(e) => Embedding::F32(e.to_vec()),
                            },
                        });
                    }
                }
            }

            offset += level_vectors_count as usize * vector_size;
        }

        self.block_vector_buffer
            .sort_unstable_by_key(|p| (p.doc_id, p.field_id, p.chunk_id));
    }

    /// cluster the vectors in the block_vector_buffer, and return the medoids
    pub(crate) async fn cluster_vector_shard(&mut self, sort: bool) -> Vec<Medoid> {
        let non_affine = self.max_vector_value == f32::MIN;

        let vector_count_block = self.block_vector_buffer.len();

        let cluster_number = match self.meta.clustering {
            Clustering::Auto => (vector_count_block.sqrt() * 2).max(1),
            Clustering::None => 1,
            Clustering::Fixed(n) => n.min(vector_count_block).max(1),
        };
        let vector_similarity = self.vector_similarity;

        let sample_size =
            (vector_count_block as f32 / (1.0 + (vector_count_block as f32 * 0.0025))) as usize;
        let m_step = (vector_count_block / sample_size).max(1);
        let v_step = (vector_count_block / sample_size / 16).max(1);

        let medoid_step = m_step;
        let vector_step = v_step;

        use ahash::AHashMap;

        let mut medoid = Medoid {
            medoid_index: 0,
            child_count: 0,
        };
        let mut medoids: AHashMap<usize, Medoid> = AHashMap::new();

        let enable_scale = self.quantization != Quantization::None
            && self.vector_similarity != VectorSimilarity::Cosine;
        unsafe {
            let mut sum_vector = vec![0f32; self.vector_dimensions];
            for i in (0..vector_count_block).step_by(vector_step) {
                let embedding = &self.block_vector_buffer[i].embedding;
                if self.is_simd {
                    accumulate_simd(&mut sum_vector, embedding);
                } else {
                    accumulate(&mut sum_vector, embedding);
                }
            }
            let vector_count_block_step = vector_count_block / vector_step;

            let sum_vector = match &self.block_vector_buffer[0].embedding {
                Embedding::I8(_) => Embedding::I8(
                    sum_vector
                        .iter()
                        .map(|x| (x / vector_count_block_step as f32) as i8)
                        .collect::<Vec<_>>(),
                ),
                Embedding::F32(_) => Embedding::F32(
                    sum_vector
                        .iter()
                        .map(|x| x / vector_count_block_step as f32)
                        .collect::<Vec<_>>(),
                ),
            };

            let query_simd = QuerySimd::new(&sum_vector);
            let mut best_similarity = f32::MIN;
            for (i, medoid_candidate) in self.block_vector_buffer.iter().enumerate() {
                let scale_norm = None;
                let similarity = if self.is_simd {
                    similarity_embedding_simd(
                        &query_simd,
                        &medoid_candidate.embedding,
                        scale_norm,
                        vector_similarity,
                        self.quantization,
                        non_affine,
                    )
                } else {
                    similarity_embedding(
                        &sum_vector,
                        &medoid_candidate.embedding,
                        scale_norm,
                        vector_similarity,
                        self.quantization,
                        non_affine,
                    )
                };
                if similarity > best_similarity {
                    medoid.medoid_index = i;
                    best_similarity = similarity;
                }
            }

            let query_simd = QuerySimd::new(
                &self.block_vector_buffer[medoid.medoid_index]
                    .embedding
                    .clone(),
            );
            for i in 0..self.block_vector_buffer.len() {
                if i != medoid.medoid_index {
                    let scale_norm = if enable_scale {
                        Some((
                            self.block_vector_buffer[medoid.medoid_index].scale,
                            self.block_vector_buffer[medoid.medoid_index].norm,
                            self.block_vector_buffer[medoid.medoid_index].zero_point,
                            self.block_vector_buffer[medoid.medoid_index].sum_q,
                            self.block_vector_buffer[i].scale,
                            self.block_vector_buffer[i].norm,
                            self.block_vector_buffer[i].zero_point,
                            self.block_vector_buffer[i].sum_q,
                        ))
                    } else {
                        None
                    };
                    let similarity = if self.is_simd {
                        similarity_embedding_simd(
                            &query_simd,
                            &self.block_vector_buffer[i].embedding,
                            scale_norm,
                            vector_similarity,
                            self.quantization,
                            non_affine,
                        )
                    } else {
                        similarity_embedding(
                            &self.block_vector_buffer[medoid.medoid_index].embedding,
                            &self.block_vector_buffer[i].embedding,
                            scale_norm,
                            vector_similarity,
                            self.quantization,
                            non_affine,
                        )
                    };
                    self.block_vector_buffer[i].similarity = similarity;
                    self.block_vector_buffer[i].medoid_index = medoid.medoid_index;
                } else {
                    self.block_vector_buffer[i].similarity = 0.0;
                    self.block_vector_buffer[i].medoid_index = medoid.medoid_index;
                }
            }
            medoid.child_count += self.block_vector_buffer.len();
        }

        self.block_vector_buffer[medoid.medoid_index].medoid_index = medoid.medoid_index;
        self.block_vector_buffer[medoid.medoid_index].is_medoid = true;
        medoids.insert(medoid.medoid_index, medoid);

        for cluster_id in 1..cluster_number {
            let mut medoid = Medoid {
                medoid_index: 0,
                child_count: 0,
            };
            let mut best_medoid_similarity_sum = f32::MIN;

            unsafe {
                for i in (0..vector_count_block)
                    .skip(cluster_id)
                    .step_by(medoid_step)
                {
                    if self.block_vector_buffer[i].is_medoid {
                        continue;
                    }

                    let record_outer_simd = QuerySimd::new(&self.block_vector_buffer[i].embedding);
                    let mut similarity_sum = 0.0;

                    for j in (0..vector_count_block)
                        .skip(cluster_id)
                        .step_by(vector_step)
                    {
                        if i != j && !self.block_vector_buffer[j].is_medoid {
                            let scale_norm = if enable_scale {
                                Some((
                                    self.block_vector_buffer[i].scale,
                                    self.block_vector_buffer[i].norm,
                                    self.block_vector_buffer[i].zero_point,
                                    self.block_vector_buffer[i].sum_q,
                                    self.block_vector_buffer[j].scale,
                                    self.block_vector_buffer[j].norm,
                                    self.block_vector_buffer[j].zero_point,
                                    self.block_vector_buffer[j].sum_q,
                                ))
                            } else {
                                None
                            };
                            let similarity = if self.is_simd {
                                similarity_embedding_simd(
                                    &record_outer_simd,
                                    &self.block_vector_buffer[j].embedding,
                                    scale_norm,
                                    vector_similarity,
                                    self.quantization,
                                    non_affine,
                                )
                            } else {
                                similarity_embedding(
                                    &self.block_vector_buffer[i].embedding,
                                    &self.block_vector_buffer[j].embedding,
                                    scale_norm,
                                    vector_similarity,
                                    self.quantization,
                                    non_affine,
                                )
                            };

                            if similarity > self.block_vector_buffer[j].similarity {
                                let similarity_gain =
                                    similarity - self.block_vector_buffer[j].similarity;
                                similarity_sum += similarity_gain;
                            }
                        }
                    }

                    if similarity_sum > best_medoid_similarity_sum {
                        medoid.medoid_index = i;
                        best_medoid_similarity_sum = similarity_sum;
                    }
                }

                for i in 0..self.block_vector_buffer.len() {
                    if self.block_vector_buffer[i].is_medoid {
                        continue;
                    }
                    if i != medoid.medoid_index {
                        let scale_norm = if enable_scale {
                            Some((
                                self.block_vector_buffer[medoid.medoid_index].scale,
                                self.block_vector_buffer[medoid.medoid_index].norm,
                                self.block_vector_buffer[medoid.medoid_index].zero_point,
                                self.block_vector_buffer[medoid.medoid_index].sum_q,
                                self.block_vector_buffer[i].scale,
                                self.block_vector_buffer[i].norm,
                                self.block_vector_buffer[i].zero_point,
                                self.block_vector_buffer[i].sum_q,
                            ))
                        } else {
                            None
                        };
                        let similarity = if self.is_simd {
                            similarity_embedding_simd(
                                &QuerySimd::new(
                                    &self.block_vector_buffer[medoid.medoid_index]
                                        .embedding
                                        .clone(),
                                ),
                                &self.block_vector_buffer[i].embedding,
                                scale_norm,
                                vector_similarity,
                                self.quantization,
                                non_affine,
                            )
                        } else {
                            similarity_embedding(
                                &self.block_vector_buffer[medoid.medoid_index].embedding,
                                &self.block_vector_buffer[i].embedding,
                                scale_norm,
                                vector_similarity,
                                self.quantization,
                                non_affine,
                            )
                        };
                        if similarity > self.block_vector_buffer[i].similarity {
                            medoids
                                .get_mut(&self.block_vector_buffer[i].medoid_index)
                                .unwrap()
                                .child_count -= 1;
                            self.block_vector_buffer[i].similarity = similarity;
                            self.block_vector_buffer[i].medoid_index = medoid.medoid_index;
                            medoid.child_count += 1;
                        }
                    } else {
                        medoids
                            .get_mut(&self.block_vector_buffer[i].medoid_index)
                            .unwrap()
                            .child_count -= 1;
                        self.block_vector_buffer[i].similarity = 0.0;
                        self.block_vector_buffer[i].medoid_index = medoid.medoid_index;
                        medoid.child_count += 1;
                    }
                }
            }

            self.block_vector_buffer[medoid.medoid_index].medoid_index = medoid.medoid_index;
            self.block_vector_buffer[medoid.medoid_index].is_medoid = true;
            medoids.insert(medoid.medoid_index, medoid);
        }

        let mut best_similarity_sum = self
            .block_vector_buffer
            .iter()
            .filter(|v| !v.is_medoid)
            .map(|v| v.similarity as isize)
            .sum::<isize>();
        let vector_count_block = self.block_vector_buffer.len();
        let vector_step = 1;
        let mut centroid_map = medoids
            .into_iter()
            .map(|(m, cluster)| {
                (
                    m,
                    Centroid {
                        medoid_index: m,
                        child_count: cluster.child_count,
                        sum_vector: vec![0f32; self.vector_dimensions],
                        centroid: Embedding::I8(Vec::new()),
                        query_simd: unsafe { QuerySimd::new(&Embedding::I8(Vec::new())) },
                        medoid_index_new: 0,
                        best_similarity: f32::MIN,
                        has_changed: false,
                    },
                )
            })
            .collect::<AHashMap<usize, Centroid>>();

        unsafe {
            loop {
                for i in (0..vector_count_block).step_by(vector_step) {
                    let embedding = &self.block_vector_buffer[i].embedding;
                    let medoid_index = self.block_vector_buffer[i].medoid_index;
                    let centroid = centroid_map.get_mut(&medoid_index).unwrap();
                    if self.is_simd {
                        accumulate_simd(&mut centroid.sum_vector, embedding);
                    } else {
                        accumulate(&mut centroid.sum_vector, embedding);
                    }
                }

                for (_medoid_index, centroid) in centroid_map.iter_mut() {
                    let sum_vector = match &self.block_vector_buffer[0].embedding {
                        Embedding::I8(_) => Embedding::I8(
                            centroid
                                .sum_vector
                                .iter()
                                .map(|x| (x / centroid.child_count as f32) as i8)
                                .collect::<Vec<_>>(),
                        ),
                        Embedding::F32(_) => Embedding::F32(
                            centroid
                                .sum_vector
                                .iter()
                                .map(|x| x / centroid.child_count as f32)
                                .collect::<Vec<_>>(),
                        ),
                    };
                    centroid.centroid = sum_vector;
                    centroid.query_simd = QuerySimd::new(&centroid.centroid);
                }

                for (i, medoid_candidate) in self.block_vector_buffer.iter().enumerate() {
                    let medoid_index = self.block_vector_buffer[i].medoid_index;
                    let scale_norm = None;
                    let similarity = if self.is_simd {
                        similarity_embedding_simd(
                            &centroid_map[&medoid_index].query_simd,
                            &medoid_candidate.embedding,
                            scale_norm,
                            vector_similarity,
                            self.quantization,
                            non_affine,
                        )
                    } else {
                        similarity_embedding(
                            &centroid_map[&medoid_index].centroid,
                            &medoid_candidate.embedding,
                            scale_norm,
                            vector_similarity,
                            self.quantization,
                            non_affine,
                        )
                    };

                    if similarity > centroid_map[&medoid_index].best_similarity {
                        let centroid = centroid_map.get_mut(&medoid_index).unwrap();
                        centroid.medoid_index_new = i;
                        centroid.best_similarity = similarity;
                    }
                }

                for i in 0..vector_count_block {
                    let old_medoid_index = self.block_vector_buffer[i].medoid_index;
                    let new_medoid_index =
                        centroid_map[&self.block_vector_buffer[i].medoid_index].medoid_index_new;
                    if i != new_medoid_index {
                        if old_medoid_index != new_medoid_index {
                            let scale_norm = if enable_scale {
                                Some((
                                    self.block_vector_buffer[new_medoid_index].scale,
                                    self.block_vector_buffer[new_medoid_index].norm,
                                    self.block_vector_buffer[new_medoid_index].zero_point,
                                    self.block_vector_buffer[new_medoid_index].sum_q,
                                    self.block_vector_buffer[i].scale,
                                    self.block_vector_buffer[i].norm,
                                    self.block_vector_buffer[i].zero_point,
                                    self.block_vector_buffer[i].sum_q,
                                ))
                            } else {
                                None
                            };
                            let similarity = if self.is_simd {
                                similarity_embedding_simd(
                                    &QuerySimd::new(
                                        &self.block_vector_buffer[new_medoid_index]
                                            .embedding
                                            .clone(),
                                    ),
                                    &self.block_vector_buffer[i].embedding,
                                    scale_norm,
                                    vector_similarity,
                                    self.quantization,
                                    non_affine,
                                )
                            } else {
                                similarity_embedding(
                                    &self.block_vector_buffer[new_medoid_index].embedding,
                                    &self.block_vector_buffer[i].embedding,
                                    scale_norm,
                                    vector_similarity,
                                    self.quantization,
                                    non_affine,
                                )
                            };
                            let vector = &mut self.block_vector_buffer[i];
                            vector.similarity = similarity;
                            vector.medoid_index = new_medoid_index;
                            vector.is_medoid = false;
                        }
                    } else {
                        let vector = &mut self.block_vector_buffer[i];
                        vector.similarity = 0.0;
                        vector.medoid_index = new_medoid_index;
                        vector.is_medoid = true;
                    }
                }

                centroid_map = centroid_map
                    .into_iter()
                    .map(|(_m, cluster)| {
                        (
                            cluster.medoid_index_new,
                            Centroid {
                                medoid_index: cluster.medoid_index_new,
                                child_count: cluster.child_count,
                                sum_vector: vec![0f32; self.vector_dimensions],
                                centroid: Embedding::I8(Vec::new()),
                                query_simd: QuerySimd::new(
                                    &self.block_vector_buffer[cluster.medoid_index_new].embedding,
                                ),
                                medoid_index_new: 0,
                                best_similarity: f32::MIN,
                                has_changed: cluster.medoid_index != cluster.medoid_index_new,
                            },
                        )
                    })
                    .collect::<AHashMap<usize, Centroid>>();

                let similarity_sum = self
                    .block_vector_buffer
                    .iter()
                    .filter(|v| !v.is_medoid)
                    .map(|v| v.similarity as isize)
                    .sum::<isize>();
                if similarity_sum > best_similarity_sum {
                    best_similarity_sum = similarity_sum;
                }

                let medoid_keys = centroid_map
                    .iter()
                    .filter(|c| c.1.has_changed)
                    .map(|c| *c.0)
                    .collect_vec();
                for i in 0..vector_count_block {
                    if self.block_vector_buffer[i].is_medoid {
                        continue;
                    }
                    for medoid_index in medoid_keys.iter() {
                        let scale_norm = if enable_scale {
                            Some((
                                self.block_vector_buffer[*medoid_index].scale,
                                self.block_vector_buffer[*medoid_index].norm,
                                self.block_vector_buffer[*medoid_index].zero_point,
                                self.block_vector_buffer[*medoid_index].sum_q,
                                self.block_vector_buffer[i].scale,
                                self.block_vector_buffer[i].norm,
                                self.block_vector_buffer[i].zero_point,
                                self.block_vector_buffer[i].sum_q,
                            ))
                        } else {
                            None
                        };
                        let similarity = if self.is_simd {
                            similarity_embedding_simd(
                                &centroid_map[medoid_index].query_simd,
                                &self.block_vector_buffer[i].embedding,
                                scale_norm,
                                vector_similarity,
                                self.quantization,
                                non_affine,
                            )
                        } else {
                            similarity_embedding(
                                &self.block_vector_buffer[*medoid_index].embedding,
                                &self.block_vector_buffer[i].embedding,
                                scale_norm,
                                vector_similarity,
                                self.quantization,
                                non_affine,
                            )
                        };
                        if similarity > self.block_vector_buffer[i].similarity {
                            centroid_map
                                .get_mut(&self.block_vector_buffer[i].medoid_index)
                                .unwrap()
                                .child_count -= 1;
                            centroid_map.get_mut(medoid_index).unwrap().child_count += 1;
                            let vector = &mut self.block_vector_buffer[i];
                            vector.similarity = similarity;
                            vector.medoid_index = *medoid_index;
                            vector.is_medoid = false;
                        }
                    }
                }

                let similarity_sum = self
                    .block_vector_buffer
                    .iter()
                    .filter(|v| !v.is_medoid)
                    .map(|v| v.similarity as isize)
                    .sum::<isize>();
                if similarity_sum > best_similarity_sum {
                    best_similarity_sum = similarity_sum;
                } else {
                    break;
                }
            }
        }

        if sort {
            self.block_vector_buffer.sort_unstable_by(|a, b| {
                let result = a.medoid_index.cmp(&b.medoid_index);
                if result != Ordering::Equal {
                    result
                } else {
                    b.is_medoid.cmp(&a.is_medoid)
                }
            });
        }

        let mut medoids_vec = centroid_map
            .into_iter()
            .map(|(_m, cluster)| Medoid {
                medoid_index: cluster.medoid_index,
                child_count: cluster.child_count,
            })
            .collect::<Vec<_>>();
        medoids_vec.sort_unstable_by_key(|a| a.medoid_index);
        medoids_vec
    }
}
