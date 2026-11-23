use ahash::AHashMap;
use itertools::Itertools;
use num::integer::Roots;
#[cfg(target_arch = "aarch64")]
use std::arch::aarch64::*;
#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;
use std::cmp::Ordering;

use crate::{
    index::{Clustering, Shard},
    vector::{Embedding, Quantization},
    vector_similarity::{
        QuerySimd, VectorSimilarity, similarity_embedding, similarity_embedding_simd,
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
pub(crate) fn accumulate_neon(sum: &mut [f32], emb: &Embedding) {
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
    /// Similarity between the vector `a` (wrapped in `q`) and the vector `b`.
    #[inline(always)]
    fn pair_similarity(
        &self,
        q: &QuerySimd,
        a: usize,
        b: usize,
        vs: VectorSimilarity,
        enable_scale: bool,
        non_affine: bool,
    ) -> f32 {
        let (va, vb) = (&self.block_vector_buffer[a], &self.block_vector_buffer[b]);
        let scale_norm = if enable_scale {
            Some((
                va.scale,
                va.norm,
                va.zero_point,
                va.sum_q,
                vb.scale,
                vb.norm,
                vb.zero_point,
                vb.sum_q,
            ))
        } else {
            None
        };
        if self.is_simd {
            unsafe {
                similarity_embedding_simd(
                    q,
                    &vb.embedding,
                    scale_norm,
                    vs,
                    self.quantization,
                    non_affine,
                )
            }
        } else {
            similarity_embedding(
                &va.embedding,
                &vb.embedding,
                scale_norm,
                vs,
                self.quantization,
                non_affine,
            )
        }
    }

    /// D^1.5 seeding on a coreset. The first medoid already exists and every
    /// vector's `.similarity` holds its similarity to it. Returns k-1 further seed indices.
    fn seed_medoids_d15(
        &self,
        first: usize,
        k: usize,
        vs: VectorSimilarity,
        enable_scale: bool,
        non_affine: bool,
    ) -> Vec<usize> {
        let n = self.block_vector_buffer.len();
        let m = n.min((4 * k).max(512));
        let coreset: Vec<usize> = (0..m).map(|j| j * n / m).collect();
        let m = coreset.len();

        let q_first = unsafe { QuerySimd::new(&self.block_vector_buffer[first].embedding) };
        let s_max = self.pair_similarity(&q_first, first, first, vs, enable_scale, non_affine);

        let mut chosen: Vec<bool> = coreset.iter().map(|&i| i == first).collect();
        let mut best: Vec<f32> = coreset
            .iter()
            .map(|&i| self.block_vector_buffer[i].similarity)
            .collect();
        let mut weights = vec![0.0f64; m];
        let mut seeds = Vec::with_capacity(k.saturating_sub(1));

        let mut rng: u64 = 0xD1B5_4A32_D192_ED03;

        for _ in 1..k {
            let mut total = 0.0f64;
            for j in 0..m {
                let w = if chosen[j] {
                    0.0
                } else {
                    let d = (s_max - best[j]).max(0.0);
                    let r = d.sqrt();
                    let s = r.sqrt();
                    (r * s * s.sqrt()) as f64
                };
                weights[j] = w;
                total += w;
            }

            let pick = if total > 0.0 {
                rng ^= rng >> 12;
                rng ^= rng << 25;
                rng ^= rng >> 27;
                let u =
                    (rng.wrapping_mul(0x2545_F491_4F6C_DD1D) >> 11) as f64 / (1u64 << 53) as f64;
                let target = u * total;
                let (mut acc, mut hit, mut last) = (0.0f64, usize::MAX, 0usize);
                for (j, weight) in weights.iter().enumerate().take(m) {
                    acc += *weight;
                    if *weight > 0.0 {
                        last = j;
                        if hit == usize::MAX && acc >= target {
                            hit = j;
                        }
                    }
                }
                if hit == usize::MAX { last } else { hit }
            } else {
                match chosen.iter().position(|&c| !c) {
                    Some(j) => j,
                    None => break,
                }
            };

            chosen[pick] = true;
            let s = coreset[pick];
            seeds.push(s);

            let q = unsafe { QuerySimd::new(&self.block_vector_buffer[s].embedding) };
            for j in 0..m {
                if chosen[j] {
                    continue;
                }
                let sim = self.pair_similarity(&q, s, coreset[j], vs, enable_scale, non_affine);
                if sim > best[j] {
                    best[j] = sim;
                }
            }
        }
        seeds
    }

    /// cluster the vectors in the block_vector_buffer, and return the medoids
    pub(crate) async fn cluster_vector_shard(&mut self, sort: bool) -> Vec<Medoid> {
        let non_affine = self.max_vector_value == f32::MIN;

        let vector_count_block = self.block_vector_buffer.len();

        let cluster_number = match self.index_config.clustering {
            Clustering::Auto => (vector_count_block.sqrt() * 2).max(1),
            Clustering::None => 1,
            Clustering::Fixed(n) => n.min(vector_count_block).max(1),
        };
        let vector_similarity = self.vector_similarity;

        let sample_size = ((vector_count_block as f32
            / (1.0 + (vector_count_block as f32 * 0.0025))) as usize)
            .max(vector_count_block.min(10));
        let vector_step = (vector_count_block / sample_size).max(1);

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

        let first_medoid_index = medoid.medoid_index;
        self.block_vector_buffer[first_medoid_index].medoid_index = first_medoid_index;
        self.block_vector_buffer[first_medoid_index].is_medoid = true;
        medoids.insert(first_medoid_index, medoid);

        let first = first_medoid_index;
        let seeds = self.seed_medoids_d15(
            first,
            cluster_number,
            vector_similarity,
            enable_scale,
            non_affine,
        );

        for &s in &seeds {
            self.block_vector_buffer[s].is_medoid = true;
            self.block_vector_buffer[s].medoid_index = s;
            self.block_vector_buffer[s].similarity = 0.0;
            medoids.insert(
                s,
                Medoid {
                    medoid_index: s,
                    child_count: 0,
                },
            );
        }

        for &s in &seeds {
            let q = unsafe { QuerySimd::new(&self.block_vector_buffer[s].embedding) };
            for i in 0..self.block_vector_buffer.len() {
                if self.block_vector_buffer[i].is_medoid {
                    continue;
                }
                let sim =
                    self.pair_similarity(&q, s, i, vector_similarity, enable_scale, non_affine);
                if sim > self.block_vector_buffer[i].similarity {
                    self.block_vector_buffer[i].similarity = sim;
                    self.block_vector_buffer[i].medoid_index = s;
                }
            }
        }

        for m in medoids.values_mut() {
            m.child_count = 0;
        }
        for v in self.block_vector_buffer.iter() {
            medoids.get_mut(&v.medoid_index).unwrap().child_count += 1;
        }

        const MAX_REFINE_ITERATIONS: usize = 8;
        const MIN_REL_IMPROVEMENT: f64 = 1e-3;
        let mut iteration = 0usize;

        let mut best_similarity_sum = self
            .block_vector_buffer
            .iter()
            .filter(|v| !v.is_medoid)
            .map(|v| v.similarity as isize)
            .sum::<isize>();
        let vector_count_block = self.block_vector_buffer.len();
        let vector_step = 1;
        let mut medoid_map = medoids
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
                    let centroid = medoid_map.get_mut(&medoid_index).unwrap();
                    if self.is_simd {
                        accumulate_simd(&mut centroid.sum_vector, embedding);
                    } else {
                        accumulate(&mut centroid.sum_vector, embedding);
                    }
                }

                for centroid in medoid_map.values_mut() {
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
                            &medoid_map[&medoid_index].query_simd,
                            &medoid_candidate.embedding,
                            scale_norm,
                            vector_similarity,
                            self.quantization,
                            non_affine,
                        )
                    } else {
                        similarity_embedding(
                            &medoid_map[&medoid_index].centroid,
                            &medoid_candidate.embedding,
                            scale_norm,
                            vector_similarity,
                            self.quantization,
                            non_affine,
                        )
                    };

                    if similarity > medoid_map[&medoid_index].best_similarity {
                        let centroid = medoid_map.get_mut(&medoid_index).unwrap();
                        centroid.medoid_index_new = i;
                        centroid.best_similarity = similarity;
                    }
                }

                for i in 0..vector_count_block {
                    let old_medoid_index = self.block_vector_buffer[i].medoid_index;
                    let new_medoid_index =
                        medoid_map[&self.block_vector_buffer[i].medoid_index].medoid_index_new;
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

                medoid_map = medoid_map
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

                let medoid_keys = medoid_map
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
                                &medoid_map[medoid_index].query_simd,
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
                            medoid_map
                                .get_mut(&self.block_vector_buffer[i].medoid_index)
                                .unwrap()
                                .child_count -= 1;
                            medoid_map.get_mut(medoid_index).unwrap().child_count += 1;
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
                iteration += 1;
                if similarity_sum > best_similarity_sum {
                    let rel = (similarity_sum - best_similarity_sum) as f64
                        / (best_similarity_sum as f64).abs().max(f64::MIN_POSITIVE);

                    best_similarity_sum = similarity_sum;
                    if rel < MIN_REL_IMPROVEMENT || iteration >= MAX_REFINE_ITERATIONS {
                        break;
                    }
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

        let mut medoids_vec = medoid_map
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
