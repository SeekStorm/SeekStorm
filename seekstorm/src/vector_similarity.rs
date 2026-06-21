#[cfg(target_arch = "aarch64")]
use std::arch::aarch64::*;
#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;
use std::fmt;

use crate::vector::Quantization;
use crate::vector::{Embedding, EmbeddingView};
use rand::rngs::ChaCha8Rng;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Similarity measure for comparing vector embeddings.
#[derive(Clone, Copy, Default, Debug, PartialEq, Deserialize, Serialize, ToSchema)]
pub enum VectorSimilarity {
    /// Cosine Similarity
    /// Cosine similarity measures only the direction (angle) between vectors.
    /// At index time the vectors are normalized to a norm of 1.0, so that the cosine similarity can be computed efficiently as a dot product without needing to compute the norms at query time.
    /// The query vector is normalized to a norm of 1.0, so that the cosine similarity can be computed efficiently as a dot product without needing to compute the norms at query time.
    Cosine,
    /// Dot product (Inner Product),  
    /// Dot product measures both the direction and the magnitude (length) of vectors.
    /// Use dot product, when using pre-normalized external embeddings, to prevent unnecessary normalization with cosine similarity.
    /// Dot product and cosine similarity produce the same ranking for L2-normalized vectors.
    #[default]
    Dot,
    /// Euclidean distance (L2)
    Euclidean,
}

impl fmt::Display for VectorSimilarity {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            VectorSimilarity::Cosine => write!(f, "Cosine"),
            VectorSimilarity::Dot => write!(f, "Dot"),
            VectorSimilarity::Euclidean => write!(f, "Euclidean"),
        }
    }
}

///Specifies in which cluster to search for ANN results.
#[derive(Default, PartialEq, Clone, Debug, Serialize, Deserialize, ToSchema)]
pub enum AnnMode {
    /// Search in all clusters (default)
    #[default]
    All,
    /// Search only in the clusters with the highest similarity scores to the query vector.
    /// The number of clusters to search is specified by the n-probe parameter.
    /// You cannot directly set a specific, guaranteed recall number (e.g., "always give me 95% recall@10"). There is no one-fits-all, there is no automatism.
    /// Instead, you manually tune parameters that control the tradeoff between query latency and accuracy.
    /// Because recall depends heavily on the structure of your specific data (distribution, dimensionality, and clustering) and queries,
    /// there is always a trial-and-error (benchmarking) phase required to determine the right settings for your data.
    /// Examples:
    /// wikipedia, VectorSimilarity::Dot, dimensions: 64, Precision::F32, Clustering::Auto, Clustering::I8, recall@10=95% -> Nprobe(55)
    /// wikipedia, VectorSimilarity::Dot, dimensions: 64, Precision::F32, Clustering::Auto, Clustering::I8, recall@10=99% -> Nprobe(140)
    /// sift1m, VectorSimilarity::Euclidean, dimensions: 128, Precision::F32, Clustering::Auto, Quantization::None, recall@10=95% -> Nprobe(11)
    /// sift1m, VectorSimilarity::Euclidean, dimensions: 128, Precision::F32, Clustering::Auto, Quantization::None, recall@10=99% -> Nprobe(22)
    Nprobe(usize),
    /// Search only in clusters with similarity scores to the query vector above the specified threshold.
    /// For dot product similarity, the similarity threshold should be between 0.0 and 1.0, where higher values indicate higher similarity (identical=1.0).
    /// For Euclidean distance similarity, the similarity threshold should be between 0.0 and infinity, where lower values indicate higher similarity (identical=0.0).
    Similaritythreshold(f32),
    /// Search only in the clusters with the highest similarity scores to the query vector, but only if their similarity scores are above the specified threshold, and up to the number of clusters specified by the n-probe parameter.
    /// For dot product similarity, the similarity threshold should be between 0.0 and 1.0, where higher values indicate higher similarity (identical=1.0).
    /// For Euclidean distance similarity, the similarity threshold should be between 0.0 and infinity, where lower values indicate higher similarity (identical=0.0).
    NprobeSimilaritythreshold(usize, f32),
}

#[inline(always)]
pub(crate) fn normalize_f32(embeddings: &mut [f32]) {
    let norm = embeddings.iter().map(|x| x * x).sum::<f32>().sqrt();
    let norm_factor = 1.0 / norm;
    embeddings.iter_mut().for_each(|b| *b *= norm_factor);
}

#[cfg(target_arch = "x86_64")]
#[inline(always)]
pub(crate) unsafe fn normalize_f32_avx2(data: &mut [f32]) {
    unsafe {
        let len = data.len();
        if len == 0 {
            return;
        }

        let mut sum_sq;
        let chunks = data.chunks_exact(8);
        let rem = chunks.remainder();

        let mut sum_vec = _mm256_setzero_ps();
        for chunk in chunks {
            let v = _mm256_loadu_ps(chunk.as_ptr());
            sum_vec = _mm256_add_ps(sum_vec, _mm256_mul_ps(v, v));
        }

        sum_sq = horizontal_sum_avx2(sum_vec);
        for &x in rem {
            sum_sq += x * x;
        }

        let inv_norm = 1.0 / sum_sq.sqrt();
        let inv_norm_vec = _mm256_set1_ps(inv_norm);

        let chunks_mut = data.chunks_exact_mut(8);
        for chunk in chunks_mut {
            let v = _mm256_loadu_ps(chunk.as_ptr());
            let res = _mm256_mul_ps(v, inv_norm_vec);
            _mm256_storeu_ps(chunk.as_mut_ptr(), res);
        }

        let rem_mut = data.chunks_exact_mut(8).into_remainder();
        for x in rem_mut {
            *x *= inv_norm;
        }
    }
}

#[cfg(target_arch = "x86_64")]
#[inline(always)]
unsafe fn horizontal_sum_avx2(v: __m256) -> f32 {
    unsafe {
        let x128 = _mm_add_ps(_mm256_extractf128_ps(v, 1), _mm256_castps256_ps128(v));
        let x64 = _mm_add_ps(x128, _mm_movehl_ps(x128, x128));
        let x32 = _mm_add_ss(x64, _mm_shuffle_ps(x64, x64, 0x55));
        _mm_cvtss_f32(x32)
    }
}

#[allow(clippy::type_complexity)]
#[inline(always)]
pub(crate) fn similarity_embedding_view(
    a: &Embedding,
    b: &EmbeddingView,
    scale_norm: Option<(f32, f32, i16, i32, f32, f32, i16, i32)>,
    vector_similarity: VectorSimilarity,
    quantization: Quantization,
    non_affine: bool,
) -> f32 {
    match (a, vector_similarity, quantization) {
        (Embedding::I8(a), VectorSimilarity::Dot, Quantization::ScalarQuantizationI8) => {
            if let EmbeddingView::I8(b) = b {
                if let Some((
                    query_scale,
                    _query_norm,
                    _query_zero_point,
                    _query_sum_q,
                    embedding_scale,
                    _embedding_norm,
                    _embedding_zero_point,
                    _embedding_sum_q,
                )) = scale_norm
                {
                    dot_i8_quantized(a, query_scale, b, embedding_scale)
                } else {
                    dot_i8(a, b) as f32
                }
            } else {
                panic!("dot_i8 only supports i8 embeddings")
            }
        }

        (Embedding::I8(a), VectorSimilarity::Dot, Quantization::TurboQuantI8) => {
            if let EmbeddingView::I8(b) = b {
                if let Some((
                    query_scale,
                    _query_norm,
                    _query_zero_point,
                    _query_sum_q,
                    embedding_scale,
                    _embedding_norm,
                    _embedding_zero_point,
                    _embedding_sum_q,
                )) = scale_norm
                {
                    -TurboQuant::dot_i8_turboquant(a, query_scale, b, embedding_scale)
                } else {
                    dot_i8(a, b) as f32
                }
            } else {
                panic!("dot_i8 only supports i8 embeddings")
            }
        }

        (Embedding::I8(a), VectorSimilarity::Dot, Quantization::None) => {
            if let EmbeddingView::I8(b) = b {
                dot_i8(a, b) as f32
            } else {
                panic!("dot_i8 only supports i8 embeddings")
            }
        }

        (Embedding::F32(a), VectorSimilarity::Dot, _) => {
            if let EmbeddingView::F32(b) = b {
                dot_f32(a, b)
            } else {
                panic!("dot_f32 only supports f32 embeddings")
            }
        }
        (Embedding::I8(a), VectorSimilarity::Cosine, Quantization::ScalarQuantizationI8) => {
            if let EmbeddingView::I8(b) = b {
                if let Some((
                    query_scale,
                    _query_norm,
                    _query_zero_point,
                    _query_sum_q,
                    embedding_scale,
                    _embedding_norm,
                    _embedding_zero_point,
                    _embedding_sum_q,
                )) = scale_norm
                {
                    dot_i8_quantized(a, query_scale, b, embedding_scale)
                } else {
                    dot_i8(a, b) as f32
                }
            } else {
                panic!("dot_i8 only supports i8 embeddings")
            }
        }

        (Embedding::I8(a), VectorSimilarity::Cosine, Quantization::TurboQuantI8) => {
            if let EmbeddingView::I8(b) = b {
                if let Some((
                    query_scale,
                    _query_norm,
                    _query_zero_point,
                    _query_sum_q,
                    embedding_scale,
                    _embedding_norm,
                    _embedding_zero_point,
                    _embedding_sum_q,
                )) = scale_norm
                {
                    -TurboQuant::dot_i8_turboquant(a, query_scale, b, embedding_scale)
                } else {
                    dot_i8(a, b) as f32
                }
            } else {
                panic!("dot_i8 only supports i8 embeddings")
            }
        }

        (Embedding::I8(a), VectorSimilarity::Cosine, Quantization::None) => {
            if let EmbeddingView::I8(b) = b {
                dot_i8(a, b) as f32
            } else {
                panic!("dot_i8 only supports i8 embeddings")
            }
        }

        (Embedding::F32(a), VectorSimilarity::Cosine, _) => {
            if let EmbeddingView::F32(b) = b {
                dot_f32(a, b)
            } else {
                panic!("dot_f32 only supports f32 embeddings")
            }
        }
        (Embedding::I8(a), VectorSimilarity::Euclidean, Quantization::ScalarQuantizationI8) => {
            if let EmbeddingView::I8(b) = b {
                if let Some((
                    query_scale,
                    query_norm,
                    query_zero_point,
                    query_sum_q,
                    embedding_scale,
                    embedding_norm,
                    embedding_zero_point,
                    embedding_sum_q,
                )) = scale_norm
                {
                    if non_affine {
                        -euclidean_i8_quantized(
                            a,
                            query_scale,
                            query_norm,
                            b,
                            embedding_scale,
                            embedding_norm,
                        )
                    } else {
                        -euclidean_i8_quantized_affine(
                            a,
                            query_scale,
                            query_norm,
                            query_zero_point,
                            query_sum_q,
                            b,
                            embedding_scale,
                            embedding_norm,
                            embedding_zero_point,
                            embedding_sum_q,
                        )
                    }
                } else {
                    -euclidean_i8(a, b)
                }
            } else {
                panic!("euclidean_i8 only supports i8 embeddings")
            }
        }
        (Embedding::I8(a), VectorSimilarity::Euclidean, Quantization::TurboQuantI8) => {
            if let EmbeddingView::I8(b) = b {
                if let Some((
                    query_scale,
                    query_norm,
                    _query_zero_point,
                    _query_sum_q,
                    embedding_scale,
                    embedding_norm,
                    _embedding_zero_point,
                    _embedding_sum_q,
                )) = scale_norm
                {
                    -TurboQuant::euclidean_i8_turboquant(
                        a,
                        query_scale,
                        query_norm,
                        b,
                        embedding_scale,
                        embedding_norm,
                    )
                } else {
                    -euclidean_i8(a, b)
                }
            } else {
                panic!("euclidean_i8 only supports i8 embeddings")
            }
        }

        (Embedding::I8(a), VectorSimilarity::Euclidean, Quantization::None) => {
            if let EmbeddingView::I8(b) = b {
                -euclidean_i8(a, b)
            } else {
                panic!("euclidean_i8 only supports i8 embeddings")
            }
        }

        (Embedding::F32(a), VectorSimilarity::Euclidean, _) => {
            if let EmbeddingView::F32(b) = b {
                -euclidean_f32(a, b)
            } else {
                panic!("euclidean_f32 only supports f32 embeddings")
            }
        }
    }
}

#[allow(clippy::type_complexity)]
#[inline(always)]
pub(crate) fn similarity_embedding(
    a: &Embedding,
    b: &Embedding,
    scale_norm: Option<(f32, f32, i16, i32, f32, f32, i16, i32)>,
    vector_similarity: VectorSimilarity,
    quantization: Quantization,
    non_affine: bool,
) -> f32 {
    match (a, vector_similarity, quantization) {
        (Embedding::I8(a), VectorSimilarity::Dot, Quantization::ScalarQuantizationI8) => {
            if let Embedding::I8(b) = b {
                if let Some((
                    query_scale,
                    _query_norm,
                    _query_zero_point,
                    _query_sum_q,
                    embedding_scale,
                    _embedding_norm,
                    _embedding_zero_point,
                    _embedding_sum_q,
                )) = scale_norm
                {
                    dot_i8_quantized(a, query_scale, b, embedding_scale)
                } else {
                    dot_i8(a, b) as f32
                }
            } else {
                panic!("dot_i8 only supports i8 embeddings")
            }
        }

        (Embedding::I8(a), VectorSimilarity::Dot, Quantization::TurboQuantI8) => {
            if let Embedding::I8(b) = b {
                if let Some((
                    query_scale,
                    _query_norm,
                    _query_zero_point,
                    _query_sum_q,
                    embedding_scale,
                    _embedding_norm,
                    _embedding_zero_point,
                    _embedding_sum_q,
                )) = scale_norm
                {
                    -TurboQuant::dot_i8_turboquant(a, query_scale, b, embedding_scale)
                } else {
                    dot_i8(a, b) as f32
                }
            } else {
                panic!("dot_i8 only supports i8 embeddings")
            }
        }

        (Embedding::I8(a), VectorSimilarity::Dot, Quantization::None) => {
            if let Embedding::I8(b) = b {
                dot_i8(a, b) as f32
            } else {
                panic!("dot_i8 only supports i8 embeddings")
            }
        }

        (Embedding::F32(a), VectorSimilarity::Dot, _) => {
            if let Embedding::F32(b) = b {
                dot_f32(a, b)
            } else {
                panic!("dot_f32 only supports f32 embeddings")
            }
        }

        (Embedding::I8(a), VectorSimilarity::Cosine, Quantization::ScalarQuantizationI8) => {
            if let Embedding::I8(b) = b {
                if let Some((
                    query_scale,
                    _query_norm,
                    _query_zero_point,
                    _query_sum_q,
                    embedding_scale,
                    _embedding_norm,
                    _embedding_zero_point,
                    _embedding_sum_q,
                )) = scale_norm
                {
                    dot_i8_quantized(a, query_scale, b, embedding_scale)
                } else {
                    dot_i8(a, b) as f32
                }
            } else {
                panic!("dot_i8 only supports i8 embeddings")
            }
        }

        (Embedding::I8(a), VectorSimilarity::Cosine, Quantization::TurboQuantI8) => {
            if let Embedding::I8(b) = b {
                if let Some((
                    query_scale,
                    _query_norm,
                    _query_zero_point,
                    _query_sum_q,
                    embedding_scale,
                    _embedding_norm,
                    _embedding_zero_point,
                    _embedding_sum_q,
                )) = scale_norm
                {
                    -TurboQuant::dot_i8_turboquant(a, query_scale, b, embedding_scale)
                } else {
                    dot_i8(a, b) as f32
                }
            } else {
                panic!("dot_i8 only supports i8 embeddings")
            }
        }

        (Embedding::I8(a), VectorSimilarity::Cosine, _) => {
            if let Embedding::I8(b) = b {
                dot_i8(a, b) as f32
            } else {
                panic!("dot_i8 only supports i8 embeddings2")
            }
        }

        (Embedding::F32(a), VectorSimilarity::Cosine, _) => {
            if let Embedding::F32(b) = b {
                dot_f32(a, b)
            } else {
                panic!("dot_f32 only supports f32 embeddings")
            }
        }
        (Embedding::I8(a), VectorSimilarity::Euclidean, Quantization::ScalarQuantizationI8) => {
            if let Embedding::I8(b) = b {
                if let Some((
                    query_scale,
                    query_norm,
                    query_zero_point,
                    query_sum_q,
                    embedding_scale,
                    embedding_norm,
                    embedding_zero_point,
                    embedding_sum_q,
                )) = scale_norm
                {
                    if non_affine {
                        -euclidean_i8_quantized(
                            a,
                            query_scale,
                            query_norm,
                            b,
                            embedding_scale,
                            embedding_norm,
                        )
                    } else {
                        -euclidean_i8_quantized_affine(
                            a,
                            query_scale,
                            query_norm,
                            query_zero_point,
                            query_sum_q,
                            b,
                            embedding_scale,
                            embedding_norm,
                            embedding_zero_point,
                            embedding_sum_q,
                        )
                    }
                } else {
                    -euclidean_i8(a, b)
                }
            } else {
                panic!("euclidean_i8 only supports i8 embeddings")
            }
        }
        (Embedding::I8(a), VectorSimilarity::Euclidean, Quantization::TurboQuantI8) => {
            if let Embedding::I8(b) = b {
                if let Some((
                    query_scale,
                    query_norm,
                    _query_zero_point,
                    _query_sum_q,
                    embedding_scale,
                    embedding_norm,
                    _embedding_zero_point,
                    _embedding_sum_q,
                )) = scale_norm
                {
                    -TurboQuant::euclidean_i8_turboquant(
                        a,
                        query_scale,
                        query_norm,
                        b,
                        embedding_scale,
                        embedding_norm,
                    )
                } else {
                    -euclidean_i8(a, b)
                }
            } else {
                panic!("euclidean_i8 only supports i8 embeddings")
            }
        }
        (Embedding::I8(a), VectorSimilarity::Euclidean, Quantization::None) => {
            if let Embedding::I8(b) = b {
                -euclidean_i8(a, b)
            } else {
                panic!("euclidean_i8 only supports i8 embeddings")
            }
        }
        (Embedding::F32(a), VectorSimilarity::Euclidean, _) => {
            if let Embedding::F32(b) = b {
                -euclidean_f32(a, b)
            } else {
                panic!("euclidean_f32 only supports f32 embeddings")
            }
        }
    }
}

#[allow(clippy::type_complexity)]
#[cfg(target_arch = "x86_64")]
#[inline(always)]
pub(crate) unsafe fn similarity_embedding_view_avx2(
    query: &QuerySimd,
    emb: &EmbeddingView,
    scale_norm: Option<(f32, f32, i16, i32, f32, f32, i16, i32)>,
    vector_similarity: VectorSimilarity,
    quantization: Quantization,
    non_affine: bool,
) -> f32 {
    unsafe {
        match (emb, vector_similarity, quantization) {
            (EmbeddingView::I8(e), VectorSimilarity::Dot, Quantization::ScalarQuantizationI8) => {
                if let Some((
                    query_scale,
                    _query_norm,
                    _query_zero_point,
                    _query_sum_q,
                    embedding_scale,
                    _embedding_norm,
                    _embedding_zero_point,
                    _embedding_sum_q,
                )) = scale_norm
                {
                    dot_i8_quantized_avx2(query, query_scale, e, embedding_scale)
                } else {
                    dot_i8_avx2(query, e) as f32
                }
            }

            (EmbeddingView::I8(e), VectorSimilarity::Dot, Quantization::TurboQuantI8) => {
                if let Some((
                    query_scale,
                    _query_norm,
                    _query_zero_point,
                    _query_sum_q,
                    embedding_scale,
                    _embedding_norm,
                    _embedding_zero_point,
                    _embedding_sum_q,
                )) = scale_norm
                {
                    -TurboQuant::dot_i8_turboquant_avx2(query, query_scale, e, embedding_scale)
                } else {
                    dot_i8_avx2(query, e) as f32
                }
            }

            (EmbeddingView::I8(e), VectorSimilarity::Dot, Quantization::None) => {
                dot_i8_avx2(query, e) as f32
            }

            (EmbeddingView::F32(e), VectorSimilarity::Dot, _) => dot_f32_avx2(query, e),

            (
                EmbeddingView::I8(e),
                VectorSimilarity::Cosine,
                Quantization::ScalarQuantizationI8,
            ) => {
                if let Some((
                    query_scale,
                    _query_norm,
                    _query_zero_point,
                    _query_sum_q,
                    embedding_scale,
                    _embedding_norm,
                    _embedding_zero_point,
                    _embedding_sum_q,
                )) = scale_norm
                {
                    dot_i8_quantized_avx2(query, query_scale, e, embedding_scale)
                } else {
                    dot_i8_avx2(query, e) as f32
                }
            }

            (EmbeddingView::I8(e), VectorSimilarity::Cosine, Quantization::TurboQuantI8) => {
                if let Some((
                    query_scale,
                    _query_norm,
                    _query_zero_point,
                    _query_sum_q,
                    embedding_scale,
                    _embedding_norm,
                    _embedding_zero_point,
                    _embedding_sum_q,
                )) = scale_norm
                {
                    -TurboQuant::dot_i8_turboquant_avx2(query, query_scale, e, embedding_scale)
                } else {
                    dot_i8_avx2(query, e) as f32
                }
            }

            (EmbeddingView::I8(e), VectorSimilarity::Cosine, Quantization::None) => {
                dot_i8_avx2(query, e) as f32
            }

            (EmbeddingView::F32(e), VectorSimilarity::Cosine, _) => dot_f32_avx2(query, e),
            (
                EmbeddingView::I8(e),
                VectorSimilarity::Euclidean,
                Quantization::ScalarQuantizationI8,
            ) => {
                if let Some((
                    query_scale,
                    query_norm,
                    query_zero_point,
                    query_sum_q,
                    embedding_scale,
                    embedding_norm,
                    embedding_zero_point,
                    embedding_sum_q,
                )) = scale_norm
                {
                    if non_affine {
                        -euclidean_i8_quantized_avx2(
                            query,
                            query_scale,
                            query_norm,
                            e,
                            embedding_scale,
                            embedding_norm,
                        )
                    } else {
                        -euclidean_i8_quantized_affine_avx2(
                            query,
                            query_scale,
                            query_norm,
                            query_zero_point,
                            query_sum_q,
                            e,
                            embedding_scale,
                            embedding_norm,
                            embedding_zero_point,
                            embedding_sum_q,
                        )
                    }
                } else {
                    -euclidean_i8_avx2(query, e) as f32
                }
            }
            (EmbeddingView::I8(e), VectorSimilarity::Euclidean, Quantization::TurboQuantI8) => {
                if let Some((
                    query_scale,
                    query_norm,
                    _query_zero_point,
                    _query_sum_q,
                    embedding_scale,
                    embedding_norm,
                    _embedding_zero_point,
                    _embedding_sum_q,
                )) = scale_norm
                {
                    -TurboQuant::euclidean_i8_turboquant_avx2(
                        query,
                        query_scale,
                        query_norm,
                        e,
                        embedding_scale,
                        embedding_norm,
                    )
                } else {
                    -euclidean_i8_avx2(query, e) as f32
                }
            }
            (EmbeddingView::I8(e), VectorSimilarity::Euclidean, Quantization::None) => {
                -euclidean_i8_avx2(query, e) as f32
            }
            (EmbeddingView::F32(e), VectorSimilarity::Euclidean, _) => {
                -euclidean_f32_avx2(query, e)
            }
        }
    }
}

#[allow(clippy::type_complexity)]
#[cfg(target_arch = "x86_64")]
#[inline(always)]
pub(crate) unsafe fn similarity_embedding_avx2(
    query: &QuerySimd,
    emb: &Embedding,
    scale_norm: Option<(f32, f32, i16, i32, f32, f32, i16, i32)>,
    vector_similarity: VectorSimilarity,
    quantization: Quantization,
    non_affine: bool,
) -> f32 {
    unsafe {
        match (emb, vector_similarity, quantization) {
            (Embedding::I8(e), VectorSimilarity::Dot, Quantization::ScalarQuantizationI8) => {
                if let Some((
                    query_scale,
                    _query_norm,
                    _query_zero_point,
                    _query_sum_q,
                    embedding_scale,
                    _embedding_norm,
                    _embedding_zero_point,
                    _embedding_sum_q,
                )) = scale_norm
                {
                    dot_i8_quantized_avx2(query, query_scale, e, embedding_scale)
                } else {
                    dot_i8_avx2(query, e) as f32
                }
            }

            (Embedding::I8(e), VectorSimilarity::Dot, Quantization::TurboQuantI8) => {
                if let Some((
                    query_scale,
                    _query_norm,
                    _query_zero_point,
                    _query_sum_q,
                    embedding_scale,
                    _embedding_norm,
                    _embedding_zero_point,
                    _embedding_sum_q,
                )) = scale_norm
                {
                    -TurboQuant::dot_i8_turboquant_avx2(query, query_scale, e, embedding_scale)
                } else {
                    dot_i8_avx2(query, e) as f32
                }
            }

            (Embedding::I8(e), VectorSimilarity::Dot, Quantization::None) => {
                dot_i8_avx2(query, e) as f32
            }

            (Embedding::F32(e), VectorSimilarity::Dot, _) => dot_f32_avx2(query, e),

            (Embedding::I8(e), VectorSimilarity::Cosine, Quantization::ScalarQuantizationI8) => {
                if let Some((
                    query_scale,
                    _query_norm,
                    _query_zero_point,
                    _query_sum_q,
                    embedding_scale,
                    _embedding_norm,
                    _embedding_zero_point,
                    _embedding_sum_q,
                )) = scale_norm
                {
                    dot_i8_quantized_avx2(query, query_scale, e, embedding_scale)
                } else {
                    dot_i8_avx2(query, e) as f32
                }
            }

            (Embedding::I8(e), VectorSimilarity::Cosine, Quantization::TurboQuantI8) => {
                if let Some((
                    query_scale,
                    _query_norm,
                    _query_zero_point,
                    _query_sum_q,
                    embedding_scale,
                    _embedding_norm,
                    _embedding_zero_point,
                    _embedding_sum_q,
                )) = scale_norm
                {
                    -TurboQuant::dot_i8_turboquant_avx2(query, query_scale, e, embedding_scale)
                } else {
                    dot_i8_avx2(query, e) as f32
                }
            }

            (Embedding::I8(e), VectorSimilarity::Cosine, Quantization::None) => {
                dot_i8_avx2(query, e) as f32
            }

            (Embedding::F32(e), VectorSimilarity::Cosine, _) => dot_f32_avx2(query, e),

            (Embedding::I8(e), VectorSimilarity::Euclidean, Quantization::ScalarQuantizationI8) => {
                if let Some((
                    query_scale,
                    query_norm,
                    query_zero_point,
                    query_sum_q,
                    embedding_scale,
                    embedding_norm,
                    embedding_zero_point,
                    embedding_sum_q,
                )) = scale_norm
                {
                    if non_affine {
                        -euclidean_i8_quantized_avx2(
                            query,
                            query_scale,
                            query_norm,
                            e,
                            embedding_scale,
                            embedding_norm,
                        )
                    } else {
                        -euclidean_i8_quantized_affine_avx2(
                            query,
                            query_scale,
                            query_norm,
                            query_zero_point,
                            query_sum_q,
                            e,
                            embedding_scale,
                            embedding_norm,
                            embedding_zero_point,
                            embedding_sum_q,
                        )
                    }
                } else {
                    -euclidean_i8_avx2(query, e) as f32
                }
            }

            (Embedding::I8(e), VectorSimilarity::Euclidean, Quantization::TurboQuantI8) => {
                if let Some((
                    query_scale,
                    query_norm,
                    _query_zero_point,
                    _query_sum_q,
                    embedding_scale,
                    embedding_norm,
                    _embedding_zero_point,
                    _embedding_sum_q,
                )) = scale_norm
                {
                    -TurboQuant::euclidean_i8_turboquant_avx2(
                        query,
                        query_scale,
                        query_norm,
                        e,
                        embedding_scale,
                        embedding_norm,
                    )
                } else {
                    -euclidean_i8_avx2(query, e) as f32
                }
            }
            (Embedding::I8(e), VectorSimilarity::Euclidean, Quantization::None) => {
                -euclidean_i8_avx2(query, e) as f32
            }
            (Embedding::F32(e), VectorSimilarity::Euclidean, _) => -euclidean_f32_avx2(query, e),
        }
    }
}

#[inline(always)]
pub(crate) fn euclidean_f32(a: &[f32], b: &[f32]) -> f32 {
    assert_eq!(a.len(), b.len(), "Vektoren müssen die gleiche Länge haben");

    let sum_squared_diffs: f32 = a.iter().zip(b).map(|(x, y)| (x - y).powi(2)).sum();

    sum_squared_diffs
}

#[inline(always)]
pub(crate) fn euclidean_i8(a: &[i8], b: &[i8]) -> f32 {
    assert_eq!(a.len(), b.len(), "Vektoren müssen die gleiche Länge haben");

    let sum_squared_diffs: i32 = a
        .iter()
        .zip(b)
        .map(|(x, y)| (*x as i32 - *y as i32).pow(2))
        .sum();

    sum_squared_diffs as f32
}

#[cfg(target_arch = "x86_64")]
#[inline(always)]
pub(crate) unsafe fn euclidean_f32_avx2(query: &QuerySimd, b: &[f32]) -> f32 {
    unsafe {
        let query = match query {
            QuerySimd::F(e) => e,
            _ => {
                println!("{:?}", query);
                panic!("euclidean_avx2_f32 only supports f32 embeddings")
            }
        };

        let mut sum_v = _mm256_setzero_ps();

        for (i, q) in query.iter().enumerate() {
            let va = *q;
            let vb = _mm256_loadu_ps(b.as_ptr().add(i * 8));

            let diff = _mm256_sub_ps(va, vb);
            let squared = _mm256_mul_ps(diff, diff);
            sum_v = _mm256_add_ps(sum_v, squared);
        }

        let mut chunks = [0.0f32; 8];
        _mm256_storeu_ps(chunks.as_mut_ptr(), sum_v);
        let total_sum: f32 = chunks.iter().sum();

        total_sum
    }
}

#[cfg(target_arch = "x86_64")]
#[inline(always)]
pub(crate) unsafe fn euclidean_i8_avx2(query: &QuerySimd, b: &[i8]) -> i32 {
    unsafe {
        let query = match query {
            QuerySimd::I(e) => e,
            _ => {
                println!("{:?}", query);
                panic!("euclidean_avx2_i8 only supports i8 embeddings")
            }
        };

        let mut sum_v = _mm256_setzero_si256();

        for (i, q) in query.iter().enumerate() {
            let va = *q;
            let vb = _mm256_loadu_si256(b.as_ptr().add(i * 32) as *const __m256i);

            let va_lo = _mm256_cvtepi8_epi16(_mm256_extracti128_si256(va, 0));
            let vb_lo = _mm256_cvtepi8_epi16(_mm256_extracti128_si256(vb, 0));
            let diff_lo = _mm256_sub_epi16(va_lo, vb_lo);

            let va_hi = _mm256_cvtepi8_epi16(_mm256_extracti128_si256(va, 1));
            let vb_hi = _mm256_cvtepi8_epi16(_mm256_extracti128_si256(vb, 1));
            let diff_hi = _mm256_sub_epi16(va_hi, vb_hi);

            let sq_lo = _mm256_madd_epi16(diff_lo, diff_lo);
            let sq_hi = _mm256_madd_epi16(diff_hi, diff_hi);

            sum_v = _mm256_add_epi32(sum_v, sq_lo);
            sum_v = _mm256_add_epi32(sum_v, sq_hi);
        }

        let mut chunks = [0i32; 8];
        _mm256_storeu_si256(chunks.as_mut_ptr() as *mut __m256i, sum_v);
        let total_sum: i32 = chunks.iter().sum();

        total_sum
    }
}

#[inline(always)]
pub(crate) fn dot_f32(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b).map(|(x, y)| x * y).sum()
}

#[inline(always)]
pub(crate) fn dot_i8(a: &[i8], b: &[i8]) -> i32 {
    a.iter()
        .zip(b.iter())
        .map(|(&x, &y)| x as i32 * y as i32)
        .sum()
}

#[cfg(target_arch = "x86_64")]
pub(crate) type QuerySimdLaneI = __m256i;
#[cfg(target_arch = "x86_64")]
pub(crate) type QuerySimdLaneF = __m256;

#[cfg(target_arch = "aarch64")]
pub(crate) type QuerySimdLaneI = int8x16_t;
#[cfg(target_arch = "aarch64")]
pub(crate) type QuerySimdLaneF = float32x4_t;

#[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
pub(crate) type QuerySimdLaneI = i8;
#[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
pub(crate) type QuerySimdLaneF = f32;

/// Query vector pre-packed into the platform-native SIMD lane type.
/// The variant set (`I` / `F`) is portable; only the lane storage type is
/// `cfg`-selected per target. On targets without a SIMD backend the lanes
/// degenerate to single scalar elements, which makes this enum simultaneously
/// the SIMD-pack and the scalar fallback.
#[derive(Clone, Debug)]
pub(crate) enum QuerySimd {
    /// Integer (i8) query, lane-packed for the active SIMD backend.
    I(Vec<QuerySimdLaneI>),
    /// f32 query, lane-packed for the active SIMD backend.
    F(Vec<QuerySimdLaneF>),
}

impl QuerySimd {
    /// Pre-pack `query` into the platform-native SIMD lane layout.
    /// Dispatches at compile time to the AVX2 implementation on x86_64, the
    /// NEON implementation on aarch64, and a plain `Vec` clone everywhere
    /// else.
    #[inline]
    pub(crate) unsafe fn new(query: &Embedding) -> Self {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            Self::new_avx2(query)
        }
        #[cfg(target_arch = "aarch64")]
        unsafe {
            Self::new_neon(query)
        }
        #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
        {
            match query {
                Embedding::I8(e) => QuerySimd::I(e.clone()),
                Embedding::F32(e) => QuerySimd::F(e.clone()),
            }
        }
    }

    #[cfg(target_arch = "x86_64")]
    unsafe fn new_avx2(query: &Embedding) -> Self {
        unsafe {
            match query {
                Embedding::I8(e) => {
                    let mut q = Vec::with_capacity(e.len() >> 5);
                    for i in (0..e.len()).step_by(32) {
                        q.push(_mm256_loadu_si256(e.as_ptr().add(i) as *const __m256i));
                    }

                    QuerySimd::I(q)
                }
                Embedding::F32(e) => {
                    let mut q = Vec::with_capacity(e.len() >> 3);
                    for i in (0..e.len()).step_by(8) {
                        q.push(_mm256_loadu_ps(e.as_ptr().add(i)));
                    }

                    QuerySimd::F(q)
                }
            }
        }
    }

    #[cfg(target_arch = "aarch64")]
    #[inline(always)]
    unsafe fn new_neon(query: &Embedding) -> Self {
        unsafe {
            match query {
                Embedding::I8(e) => {
                    let mut q = Vec::with_capacity(e.len() >> 4);
                    for i in (0..e.len()).step_by(16) {
                        q.push(vld1q_s8(e.as_ptr().add(i)));
                    }
                    QuerySimd::I(q)
                }
                Embedding::F32(e) => {
                    let mut q = Vec::with_capacity(e.len() >> 2);
                    for i in (0..e.len()).step_by(4) {
                        q.push(vld1q_f32(e.as_ptr().add(i)));
                    }
                    QuerySimd::F(q)
                }
            }
        }
    }
}

#[cfg(target_arch = "x86_64")]
#[inline(always)]
pub(crate) unsafe fn dot_f32_avx2(query: &QuerySimd, emb: &[f32]) -> f32 {
    unsafe {
        let query = match query {
            QuerySimd::F(e) => e,
            _ => {
                println!("{:?}", query);
                panic!("dot_f32 only supports f32 embeddings")
            }
        };

        let mut sum = _mm256_setzero_ps();

        for (i, q) in query.iter().enumerate() {
            let va = *q;
            let vb = _mm256_loadu_ps(emb.as_ptr().add(i * 8));
            sum = _mm256_fmadd_ps(va, vb, sum);
        }

        let mut tmp = [0.0f32; 8];
        _mm256_storeu_ps(tmp.as_mut_ptr(), sum);
        tmp.iter().sum()
    }
}

#[cfg(target_arch = "x86_64")]
#[inline(always)]
pub(crate) unsafe fn dot_i8_avx2(query: &QuerySimd, emb: &[i8]) -> i32 {
    unsafe {
        let query = match query {
            QuerySimd::I(e) => e,
            _ => {
                println!("{:?}", query);
                panic!("dot_i8 only supports i8 embeddings3")
            }
        };

        let mut acc = _mm256_setzero_si256();
        for (i, q) in query.iter().enumerate() {
            let v0 = _mm256_loadu_si256(emb.as_ptr().add(i * 32) as *const __m256i);

            let q0_lo = _mm256_cvtepi8_epi16(_mm256_castsi256_si128(*q));
            let v0_lo = _mm256_cvtepi8_epi16(_mm256_castsi256_si128(v0));

            let q0_hi = _mm256_cvtepi8_epi16(_mm256_extracti128_si256(*q, 1));
            let v0_hi = _mm256_cvtepi8_epi16(_mm256_extracti128_si256(v0, 1));

            let prod0_lo = _mm256_madd_epi16(q0_lo, v0_lo);
            let prod0_hi = _mm256_madd_epi16(q0_hi, v0_hi);

            acc = _mm256_add_epi32(acc, prod0_lo);
            acc = _mm256_add_epi32(acc, prod0_hi);
        }

        let mut tmp = [0i32; 8];
        _mm256_storeu_si256(tmp.as_mut_ptr() as *mut __m256i, acc);

        tmp.iter().sum()
    }
}

#[cfg(target_arch = "x86_64")]
#[inline(always)]
pub(crate) unsafe fn quantize_f32_to_i8_avx2(input: &[f32]) -> Embedding {
    unsafe {
        let mut output = vec![0i8; input.len()];
        assert_eq!(input.len(), output.len());

        let n = input.len();
        let mut i = 0;

        let scale_v = _mm256_set1_ps(127.0);

        while i + 32 <= n {
            let x0 = _mm256_loadu_ps(input.as_ptr().add(i));
            let x1 = _mm256_loadu_ps(input.as_ptr().add(i + 8));
            let x2 = _mm256_loadu_ps(input.as_ptr().add(i + 16));
            let x3 = _mm256_loadu_ps(input.as_ptr().add(i + 24));

            let x0 = _mm256_mul_ps(x0, scale_v);
            let x1 = _mm256_mul_ps(x1, scale_v);
            let x2 = _mm256_mul_ps(x2, scale_v);
            let x3 = _mm256_mul_ps(x3, scale_v);

            let i0 = _mm256_cvtps_epi32(x0);
            let i1 = _mm256_cvtps_epi32(x1);
            let i2 = _mm256_cvtps_epi32(x2);
            let i3 = _mm256_cvtps_epi32(x3);

            let i01 = _mm256_packs_epi32(i0, i1);
            let i23 = _mm256_packs_epi32(i2, i3);

            let packed = _mm256_packs_epi16(i01, i23);

            _mm256_storeu_si256(output.as_mut_ptr().add(i) as *mut __m256i, packed);

            i += 32;
        }

        for j in i..n {
            output[j] = (input[j] * 127.0).round().clamp(-127.0, 127.0) as i8;
        }
        Embedding::I8(output)
    }
}

#[inline(always)]
pub(crate) fn quantize_f32_to_i8(embedding: &[f32]) -> Embedding {
    let output = embedding
        .iter()
        .map(|&v| (v * 127.0).round().clamp(-127.0, 127.0) as i8)
        .collect();
    Embedding::I8(output)
}

#[cfg(target_arch = "x86_64")]
#[inline(always)]
unsafe fn round_away_from_zero_avx2(x: __m256) -> __m256i {
    unsafe {
        let half = _mm256_set1_ps(0.5);

        let sign = _mm256_and_ps(x, _mm256_set1_ps(-0.0));

        let signed_half = _mm256_or_ps(half, sign);

        let adjusted = _mm256_add_ps(x, signed_half);

        _mm256_cvttps_epi32(adjusted)
    }
}

#[cfg(target_arch = "x86_64")]
#[inline(always)]
pub(crate) unsafe fn quantize_avx2(values: &[f32], scale: f32) -> Vec<i8> {
    unsafe {
        let inv_scale = 1.0 / scale;
        let scale_vec = _mm256_set1_ps(inv_scale);

        let mut result = vec![0i8; values.len()];
        let mut i = 0;

        while i + 16 <= values.len() {
            let v0 = _mm256_loadu_ps(values.as_ptr().add(i));
            let v1 = _mm256_loadu_ps(values.as_ptr().add(i + 8));

            let s0 = _mm256_mul_ps(v0, scale_vec);
            let s1 = _mm256_mul_ps(v1, scale_vec);

            let i0 = round_away_from_zero_avx2(s0);
            let i1 = round_away_from_zero_avx2(s1);

            let p16 = _mm256_packs_epi32(i0, i1);

            let p8 = _mm256_packs_epi16(p16, p16);

            let lo = _mm256_extracti128_si256(p8, 0);
            let hi = _mm256_extracti128_si256(p8, 1);

            _mm_storeu_si128(result.as_mut_ptr().add(i) as *mut __m128i, lo);
            _mm_storeu_si128(result.as_mut_ptr().add(i + 8) as *mut __m128i, hi);

            i += 16;
        }

        for j in i..values.len() {
            result[j] = (values[j] * inv_scale).round() as i8;
        }

        result
    }
}

#[cfg(target_arch = "x86_64")]
#[inline(always)]
pub(crate) unsafe fn squared_norm_avx2(data: &[i8]) -> i32 {
    unsafe {
        let mut sum = _mm256_setzero_si256();
        let mut i = 0;

        while i + 32 <= data.len() {
            let v = _mm256_loadu_si256(data.as_ptr().add(i) as *const __m256i);

            let v_lo = _mm256_castsi256_si128(v);
            let v_hi = _mm256_extracti128_si256(v, 1);

            let lo = _mm256_cvtepi8_epi16(v_lo);
            let hi = _mm256_cvtepi8_epi16(v_hi);

            let lo_sq = _mm256_madd_epi16(lo, lo);
            let hi_sq = _mm256_madd_epi16(hi, hi);

            sum = _mm256_add_epi32(sum, lo_sq);
            sum = _mm256_add_epi32(sum, hi_sq);

            i += 32;
        }

        let mut tmp = [0i32; 8];
        _mm256_storeu_si256(tmp.as_mut_ptr() as *mut __m256i, sum);

        let mut total: i32 = tmp.iter().sum();

        for &x in &data[i..] {
            total += (x as i32) * (x as i32);
        }

        total
    }
}

#[derive(Debug, Clone)]
pub(crate) struct QuantizedVector {
    pub(crate) data: Vec<i8>,
    pub(crate) scale: f32,
    pub(crate) norm: f32,
    pub(crate) zero_point: i16,
    pub(crate) sum_q: i32,
}

impl QuantizedVector {
    #[inline(always)]
    pub(crate) fn new_scale(values: &[f32]) -> Self {
        let max_val = values.iter().map(|x| x.abs()).fold(0.0, f32::max);
        let scale = max_val / 127.0;

        let data: Vec<i8> = values.iter().map(|&x| (x / scale).round() as i8).collect();

        Self {
            data,
            scale,
            norm: 0.0,
            zero_point: 0,
            sum_q: 0,
        }
    }

    #[inline(always)]
    pub(crate) fn new_scale_norm(values: &[f32]) -> Self {
        let max_val = values.iter().map(|x| x.abs()).fold(0.0, f32::max);
        let scale = max_val / 127.0;

        let data: Vec<i8> = values.iter().map(|&x| (x / scale).round() as i8).collect();

        let norm: i32 = data.iter().map(|&x| x as i32 * x as i32).sum();

        Self {
            data,
            scale,
            norm: norm as f32 * scale * scale,
            zero_point: 0,
            sum_q: 0,
        }
    }

    #[cfg(target_arch = "x86_64")]
    #[inline(always)]
    pub(crate) fn new_scale_avx2(values: &[f32]) -> Self {
        unsafe {
            let max_val = Self::max_abs_avx2(values);
            let scale = max_val / 127.0;

            let data = quantize_avx2(values, scale);

            Self {
                data,
                scale,
                norm: 0.0,
                zero_point: 0,
                sum_q: 0,
            }
        }
    }

    #[cfg(target_arch = "x86_64")]
    #[inline(always)]
    pub(crate) fn new_scale_norm_avx2(values: &[f32]) -> Self {
        unsafe {
            let max_val = Self::max_abs_avx2(values);
            let scale = max_val / 127.0;

            let data = quantize_avx2(values, scale);

            let norm = squared_norm_avx2(&data);

            Self {
                data,
                scale,
                norm: norm as f32 * scale * scale,
                zero_point: 0,
                sum_q: 0,
            }
        }
    }

    #[inline(always)]
    pub(crate) fn new_scale_norm_affine(
        min_vector_value: &mut f32,
        max_vector_value: &mut f32,
        values: &[f32],
    ) -> Self {
        let (mut min_val, mut max_val) = values
            .iter()
            .copied()
            .fold((f32::INFINITY, f32::NEG_INFINITY), |(min_v, max_v), x| {
                (min_v.min(x), max_v.max(x))
            });

        if min_val < *min_vector_value {
            *min_vector_value = min_val;
        } else {
            min_val = *min_vector_value;
        }

        if max_val > *max_vector_value {
            let max_val_power_of_two = Self::raster_range(max_val - min_val);
            *max_vector_value = max_val_power_of_two;
        } else {
            max_val = *max_vector_value;
        }

        let range = Self::raster_range(max_val - min_val);
        let scale = range / 255.0;
        let zero_point_f = -128.0 - (min_val / scale);
        let zero_point = zero_point_f.round().clamp(-128.0, 127.0) as i16;

        let data: Vec<i8> = values
            .iter()
            .map(|&x| ((x / scale).round() as i32 + zero_point as i32).clamp(-128, 127) as i8)
            .collect();

        let norm: i32 = data.iter().map(|&x| x as i32 * x as i32).sum();
        let sum_q = data.iter().map(|x| *x as i32).sum::<i32>();

        let norm: i32 = norm - 2 * zero_point as i32 * sum_q
            + (data.len() as i32) * zero_point as i32 * zero_point as i32;

        Self {
            data,
            scale,
            norm: norm as f32 * scale * scale,
            zero_point,
            sum_q,
        }
    }

    #[inline(always)]
    pub(crate) fn raster_range(range: f32) -> f32 {
        if range > 1.0 {
            (((range as i64) as u64 + 1).next_power_of_two() - 1) as f32
        } else {
            range
        }
    }

    #[cfg(target_arch = "x86_64")]
    #[inline(always)]
    pub(crate) fn new_scale_norm_affine_avx2(
        min_vector_value: &mut f32,
        max_vector_value: &mut f32,
        values: &[f32],
    ) -> Self {
        unsafe {
            let mut max_val = Self::max_avx2(values);
            let mut min_val = Self::min_avx2(values);

            if min_val < *min_vector_value {
                *min_vector_value = min_val;
            } else {
                min_val = *min_vector_value;
            }

            if max_val > *max_vector_value {
                let max_val_power_of_two = Self::raster_range(max_val - min_val);
                *max_vector_value = max_val_power_of_two;
            } else {
                max_val = *max_vector_value;
            }

            let range = Self::raster_range(max_val - min_val);
            let scale = range / 255.0;
            let zero_point_f = -128.0 - (min_val / scale);
            let zero_point = zero_point_f.round().clamp(-128.0, 127.0) as i16;

            let data = Self::quantize_affine_avx2(values, scale, zero_point);

            let norm = squared_norm_avx2(&data);
            let sum_q = Self::sum_avx2(&data);

            let norm: i32 = norm - 2 * zero_point as i32 * sum_q
                + (data.len() as i32) * zero_point as i32 * zero_point as i32;

            Self {
                data,
                scale,
                norm: norm as f32 * scale * scale,
                zero_point,
                sum_q,
            }
        }
    }

    #[cfg(target_arch = "x86_64")]
    #[inline(always)]
    unsafe fn max_abs_avx2(values: &[f32]) -> f32 {
        unsafe {
            let mut max_vec = _mm256_setzero_ps();
            let mut i = 0;

            while i + 8 <= values.len() {
                let v = _mm256_loadu_ps(values.as_ptr().add(i));

                let abs = _mm256_andnot_ps(_mm256_set1_ps(-0.0), v);

                max_vec = _mm256_max_ps(max_vec, abs);
                i += 8;
            }

            let mut tmp = [0f32; 8];
            _mm256_storeu_ps(tmp.as_mut_ptr(), max_vec);

            let mut max_val = tmp.iter().copied().fold(0.0, f32::max);

            for &x in &values[i..] {
                max_val = max_val.max(x.abs());
            }

            max_val
        }
    }

    #[cfg(target_arch = "x86_64")]
    #[inline(always)]
    unsafe fn max_avx2(values: &[f32]) -> f32 {
        unsafe {
            if values.is_empty() {
                return f32::NEG_INFINITY;
            }

            let mut i = 0;

            let mut max_vec = if values.len() >= 8 {
                let v = _mm256_loadu_ps(values.as_ptr());
                i = 8;
                v
            } else {
                _mm256_set1_ps(f32::NEG_INFINITY)
            };

            while i + 8 <= values.len() {
                let v = _mm256_loadu_ps(values.as_ptr().add(i));
                max_vec = _mm256_max_ps(max_vec, v);
                i += 8;
            }

            let mut tmp = [0f32; 8];
            _mm256_storeu_ps(tmp.as_mut_ptr(), max_vec);

            let mut max_val = tmp.iter().copied().fold(f32::NEG_INFINITY, f32::max);

            for &x in &values[i..] {
                max_val = max_val.max(x);
            }

            max_val
        }
    }

    #[cfg(target_arch = "x86_64")]
    #[inline(always)]
    unsafe fn min_avx2(values: &[f32]) -> f32 {
        unsafe {
            if values.is_empty() {
                return f32::INFINITY;
            }

            let mut i = 0;

            let mut min_vec = if values.len() >= 8 {
                let v = _mm256_loadu_ps(values.as_ptr());
                i = 8;
                v
            } else {
                _mm256_set1_ps(f32::INFINITY)
            };

            while i + 8 <= values.len() {
                let v = _mm256_loadu_ps(values.as_ptr().add(i));
                min_vec = _mm256_min_ps(min_vec, v);
                i += 8;
            }

            let mut tmp = [0f32; 8];
            _mm256_storeu_ps(tmp.as_mut_ptr(), min_vec);

            let mut min_val = tmp.iter().copied().fold(f32::INFINITY, f32::min);

            for &x in &values[i..] {
                min_val = min_val.min(x);
            }

            min_val
        }
    }

    #[cfg(target_arch = "x86_64")]
    #[inline(always)]
    pub unsafe fn quantize_affine_avx2(values: &[f32], scale: f32, zero_point: i16) -> Vec<i8> {
        unsafe {
            let inv_scale = 1.0 / scale;

            let scale_vec = _mm256_set1_ps(inv_scale);
            let zp_vec = _mm256_set1_epi32(zero_point as i32);

            let min_i32 = _mm256_set1_epi32(-128);
            let max_i32 = _mm256_set1_epi32(127);

            let mut result = vec![0i8; values.len()];
            let mut i = 0;

            while i + 16 <= values.len() {
                let v0 = _mm256_loadu_ps(values.as_ptr().add(i));
                let v1 = _mm256_loadu_ps(values.as_ptr().add(i + 8));

                let s0 = _mm256_mul_ps(v0, scale_vec);
                let s1 = _mm256_mul_ps(v1, scale_vec);

                let mut i0 = round_away_from_zero_avx2(s0);
                let mut i1 = round_away_from_zero_avx2(s1);

                i0 = _mm256_add_epi32(i0, zp_vec);
                i1 = _mm256_add_epi32(i1, zp_vec);

                i0 = _mm256_max_epi32(i0, min_i32);
                i0 = _mm256_min_epi32(i0, max_i32);
                i1 = _mm256_max_epi32(i1, min_i32);
                i1 = _mm256_min_epi32(i1, max_i32);

                let i0_lo = _mm256_castsi256_si128(i0);
                let i0_hi = _mm256_extracti128_si256(i0, 1);
                let i1_lo = _mm256_castsi256_si128(i1);
                let i1_hi = _mm256_extracti128_si256(i1, 1);

                let p0 = _mm_packs_epi32(i0_lo, i0_hi);
                let p1 = _mm_packs_epi32(i1_lo, i1_hi);

                let p = _mm_packs_epi16(p0, p1);

                _mm_storeu_si128(result.as_mut_ptr().add(i) as *mut __m128i, p);

                i += 16;
            }

            for j in i..values.len() {
                let q =
                    ((values[j] * inv_scale).round() as i32 + zero_point as i32).clamp(-128, 127);
                result[j] = q as i8;
            }

            result
        }
    }

    #[cfg(target_arch = "x86_64")]
    #[inline(always)]
    unsafe fn sum_avx2(data: &[i8]) -> i32 {
        unsafe {
            let mut sum = _mm256_setzero_si256();
            let mut i = 0;

            while i + 32 <= data.len() {
                let v = _mm256_loadu_si256(data.as_ptr().add(i) as *const __m256i);

                let lo128 = _mm256_castsi256_si128(v);
                let hi128 = _mm256_extracti128_si256(v, 1);

                let lo = _mm256_cvtepi8_epi16(lo128);
                let hi = _mm256_cvtepi8_epi16(hi128);

                let ones = _mm256_set1_epi16(1);
                let lo_sum = _mm256_madd_epi16(lo, ones);
                let hi_sum = _mm256_madd_epi16(hi, ones);

                sum = _mm256_add_epi32(sum, lo_sum);
                sum = _mm256_add_epi32(sum, hi_sum);

                i += 32;
            }

            let mut tmp = [0i32; 8];
            _mm256_storeu_si256(tmp.as_mut_ptr() as *mut __m256i, sum);
            let mut total: i32 = tmp.iter().sum();

            for &x in &data[i..] {
                total += x as i32;
            }

            total
        }
    }
}

#[inline(always)]
pub(crate) fn euclidean_i8_quantized(
    v1: &[i8],
    scale1: f32,
    norm1: f32,
    v2: &[i8],
    scale2: f32,
    norm2: f32,
) -> f32 {
    let dot_i32: i32 = v1.iter().zip(v2).map(|(&a, &b)| a as i32 * b as i32).sum();

    let dot = dot_i32 as f32 * scale1 * scale2;

    (norm1 + norm2 - 2.0 * dot).max(0.0)
}

#[cfg(target_arch = "x86_64")]
#[inline(always)]
pub(crate) fn euclidean_i8_quantized_avx2(
    v1: &QuerySimd,
    scale1: f32,
    norm1: f32,
    v2: &[i8],
    scale2: f32,
    norm2: f32,
) -> f32 {
    let dot_i32: i32 = unsafe { dot_i8_avx2(v1, v2) };

    let dot = dot_i32 as f32 * scale1 * scale2;

    (norm1 + norm2 - 2.0 * dot).max(0.0)
}

#[inline(always)]
fn dot_i8_quantized(v1: &[i8], scale1: f32, v2: &[i8], scale2: f32) -> f32 {
    let dot_i32: i32 = v1.iter().zip(v2).map(|(&a, &b)| a as i32 * b as i32).sum();

    dot_i32 as f32 * scale1 * scale2
}

#[cfg(target_arch = "x86_64")]
#[inline(always)]
fn dot_i8_quantized_avx2(v1: &QuerySimd, scale1: f32, v2: &[i8], scale2: f32) -> f32 {
    let dot_i32: i32 = unsafe { dot_i8_avx2(v1, v2) };

    dot_i32 as f32 * scale1 * scale2
}

#[allow(clippy::too_many_arguments)]
#[inline(always)]
pub(crate) fn euclidean_i8_quantized_affine(
    v1: &[i8],
    scale1: f32,
    norm1: f32,
    zero_point1: i16,
    sum_q1: i32,
    v2: &[i8],
    scale2: f32,
    norm2: f32,
    zero_point2: i16,
    sum_q2: i32,
) -> f32 {
    let dot_i32: i32 = v1.iter().zip(v2).map(|(&a, &b)| a as i32 * b as i32).sum();

    let n = v1.len() as i32;

    let dot_i32 = dot_i32 - zero_point2 as i32 * sum_q1 - zero_point1 as i32 * sum_q2
        + n * zero_point1 as i32 * zero_point2 as i32;

    let dot = dot_i32 as f32 * scale1 * scale2;

    (norm1 + norm2 - 2.0 * dot).max(0.0)
}

#[allow(clippy::too_many_arguments)]
#[cfg(target_arch = "x86_64")]
#[inline(always)]
pub(crate) fn euclidean_i8_quantized_affine_avx2(
    v1: &QuerySimd,
    scale1: f32,
    norm1: f32,
    zero_point1: i16,
    sum_q1: i32,
    v2: &[i8],
    scale2: f32,
    norm2: f32,
    zero_point2: i16,
    sum_q2: i32,
) -> f32 {
    let dot_i32: i32 = unsafe { dot_i8_avx2(v1, v2) };

    let n = v2.len() as i32;

    let dot_i32 = dot_i32 - zero_point2 as i32 * sum_q1 - zero_point1 as i32 * sum_q2
        + n * zero_point1 as i32 * zero_point2 as i32;

    let dot = dot_i32 as f32 * scale1 * scale2;

    (norm1 + norm2 - 2.0 * dot).max(0.0)
}

use rand::RngExt;
use rand::SeedableRng;

#[derive(Debug, Clone, Default)]
pub(crate) struct TurboQuant {
    /// Dimension of the quantized vectors (must be a power of two for FWHT)
    pub(crate) dim: usize,
    /// Original dimension of the input vectors
    pub(crate) _original_dim: usize,
    /// Random sign mask for scrambling (same for all vectors, fixed by seed)
    pub(crate) seed_mask: Vec<f32>,
}

impl TurboQuant {
    #[inline(always)]
    pub(crate) fn next_power_of_two(n: usize) -> usize {
        if n.is_power_of_two() {
            n
        } else {
            n.next_power_of_two()
        }
    }

    #[inline(always)]
    pub(crate) fn new(original_dim: usize, seed: u64) -> Self {
        let dim = Self::next_power_of_two(original_dim);

        let mut rng = ChaCha8Rng::seed_from_u64(seed);
        let seed_mask = (0..dim)
            .map(|_| if rng.random_bool(0.5) { 1.0 } else { -1.0 })
            .collect();

        Self {
            dim,
            _original_dim: original_dim,
            seed_mask,
        }
    }

    #[inline(always)]
    fn fwht(a: &mut [f32]) {
        let n = a.len();
        let mut h = 1;
        while h < n {
            for i in (0..n).step_by(h * 2) {
                for j in i..i + h {
                    let x = a[j];
                    let y = a[j + h];
                    a[j] = x + y;
                    a[j + h] = x - y;
                }
            }
            h *= 2;
        }
        let norm = (n as f32).sqrt();
        for x in a.iter_mut() {
            *x /= norm;
        }
    }

    #[cfg(target_arch = "x86_64")]
    #[inline(always)]
    unsafe fn fwht_avx2(a: &mut [f32]) {
        let n = a.len();
        let mut h = 1;

        while h < n {
            if h < 8 {
                for i in (0..n).step_by(h * 2) {
                    for j in i..i + h {
                        let x = a[j];
                        let y = a[j + h];
                        a[j] = x + y;
                        a[j + h] = x - y;
                    }
                }
            } else {
                for i in (0..n).step_by(h * 2) {
                    for j in (i..i + h).step_by(8) {
                        unsafe {
                            let va = _mm256_loadu_ps(a.as_ptr().add(j));
                            let vb = _mm256_loadu_ps(a.as_ptr().add(j + h));

                            let v_add = _mm256_add_ps(va, vb);
                            let v_sub = _mm256_sub_ps(va, vb);

                            _mm256_storeu_ps(a.as_mut_ptr().add(j), v_add);
                            _mm256_storeu_ps(a.as_mut_ptr().add(j + h), v_sub);
                        }
                    }
                }
            }
            h *= 2;
        }

        let norm_val = (n as f32).sqrt();
        unsafe {
            let v_norm = _mm256_set1_ps(norm_val);
            for i in (0..n).step_by(8) {
                let v = _mm256_loadu_ps(a.as_ptr().add(i));
                let v_res = _mm256_div_ps(v, v_norm);
                _mm256_storeu_ps(a.as_mut_ptr().add(i), v_res);
            }
        }
    }

    /// Quantisizes a f32 vector of arbitrary size to the next power of two.
    #[inline(always)]
    pub(crate) fn quantize_f32_i8(&self, vec: &[f32]) -> QuantizedVector {
        let mut padded_data = vec![0.0; self.dim];
        let len_to_copy = vec.len().min(self.dim);
        padded_data[..len_to_copy].copy_from_slice(&vec[..len_to_copy]);

        for (i, p_data) in padded_data.iter_mut().enumerate() {
            *p_data *= self.seed_mask[i];
        }

        Self::fwht(&mut padded_data);

        let scale = self.calculate_scale(&padded_data);

        let quantized: Vec<i8> = padded_data
            .into_iter()
            .map(|x| (x / scale).round().clamp(-127.0, 127.0) as i8)
            .collect();

        let squared_norm: i32 = quantized.iter().map(|&x| x as i32 * x as i32).sum();

        QuantizedVector {
            data: quantized,
            scale,
            norm: squared_norm as f32 * scale * scale,
            zero_point: 0,
            sum_q: 0,
        }
    }

    /// Quantisizes a f32 vector of arbitrary size to the next power of two, using AVX2 for acceleration
    #[cfg(target_arch = "x86_64")]
    #[inline(always)]
    pub(crate) fn quantize_f32_i8_avx2(&self, vec: &[f32]) -> QuantizedVector {
        let mut padded_data = vec![0.0; self.dim];
        let len_to_copy = vec.len().min(self.dim);
        padded_data[..len_to_copy].copy_from_slice(&vec[..len_to_copy]);

        unsafe { Self::hadamard_product_avx2(&mut padded_data, &self.seed_mask) };

        unsafe { Self::fwht_avx2(&mut padded_data) };

        let scale = unsafe { self.calculate_scale_avx2(&padded_data) };

        let quantized = unsafe { quantize_avx2(&padded_data, scale) };

        let squared_norm = unsafe { squared_norm_avx2(&quantized) };

        QuantizedVector {
            data: quantized,
            scale,
            norm: squared_norm as f32 * scale * scale,
            zero_point: 0,
            sum_q: 0,
        }
    }

    #[cfg(target_arch = "x86_64")]
    #[inline(always)]
    unsafe fn hadamard_product_avx2(padded_data: &mut [f32], seed_mask: &[f32]) {
        let n = padded_data.len();
        let chunks = n / 8;

        for i in 0..chunks {
            unsafe {
                let offset = i * 8;

                let data_vec = _mm256_loadu_ps(padded_data.as_ptr().add(offset));
                let mask_vec = _mm256_loadu_ps(seed_mask.as_ptr().add(offset));

                let result_vec = _mm256_mul_ps(data_vec, mask_vec);

                _mm256_storeu_ps(padded_data.as_mut_ptr().add(offset), result_vec);
            }
        }

        for i in (chunks * 8)..n {
            padded_data[i] *= seed_mask[i];
        }
    }

    #[cfg(target_arch = "x86_64")]
    #[inline(always)]
    unsafe fn calculate_scale_avx2(&self, rotated: &[f32]) -> f32 {
        unsafe {
            let mut sum_sq;
            let chunks = rotated.chunks_exact(8);
            let rem = chunks.remainder();

            let mut sum_vec = _mm256_setzero_ps();
            for chunk in chunks {
                let v = _mm256_loadu_ps(chunk.as_ptr());
                sum_vec = _mm256_add_ps(sum_vec, _mm256_mul_ps(v, v));
            }

            sum_sq = horizontal_sum_avx2(sum_vec);
            for &x in rem {
                sum_sq += x * x;
            }

            let sigma = sum_sq.sqrt() / (self.dim as f32).sqrt();

            (sigma / 32.0).max(1e-8)
        }
    }

    #[inline(always)]
    fn calculate_scale(&self, rotated: &[f32]) -> f32 {
        let l2_norm = rotated.iter().map(|x| x * x).sum::<f32>().sqrt();
        let sigma = l2_norm / (self.dim as f32).sqrt();
        (sigma / 32.0).max(1e-8)
    }

    /// QJL-corrected Euclidean Distance (Squared)
    #[cfg(target_arch = "x86_64")]
    #[inline(always)]
    pub(crate) fn euclidean_i8_turboquant_avx2(
        v1_q: &QuerySimd,
        scale1: f32,
        norm1: f32,
        v2_q: &[i8],
        scale2: f32,
        norm2: f32,
    ) -> f32 {
        let dot_q = Self::dot_i8_turboquant_avx2(v1_q, scale1, v2_q, scale2);
        (norm1 + norm2 - (2.0 * dot_q)).max(0.0)
    }

    /// QJL-corrected Euclidean Distance (Squared)
    #[inline(always)]
    pub(crate) fn euclidean_i8_turboquant(
        v1_q: &[i8],
        scale1: f32,
        norm1: f32,
        v2_q: &[i8],
        scale2: f32,
        norm2: f32,
    ) -> f32 {
        let dot_q = Self::dot_i8_turboquant(v1_q, scale1, v2_q, scale2);
        (norm1 + norm2 - (2.0 * dot_q)).max(0.0)
    }

    /// Calculates the Dot Product between two vectors.
    #[inline(always)]
    pub(crate) fn dot_i8_turboquant(v1_q: &[i8], scale1: f32, v2_q: &[i8], scale2: f32) -> f32 {
        let dot = dot_i8(v1_q, v2_q);

        (dot as f32) * scale1 * scale2
    }

    /// Calculates the estimated Dot Product between two vectors.
    #[cfg(target_arch = "x86_64")]
    #[inline(always)]
    pub(crate) fn dot_i8_turboquant_avx2(v1_q: &QuerySimd, scale1: f32, v2_q: &[i8], scale2: f32) -> f32 {
        let dot = unsafe { dot_i8_avx2(v1_q, v2_q) };

        (dot as f32) * scale1 * scale2
    }
}

#[cfg(target_arch = "aarch64")]
#[inline(always)]
pub(crate) unsafe fn normalize_f32_neon(data: &mut [f32]) {
    unsafe {
        let len = data.len();
        if len == 0 {
            return;
        }

        let chunks = data.chunks_exact(4);
        let rem = chunks.remainder();
        let mut sum_vec = vdupq_n_f32(0.0);
        for chunk in chunks {
            let v = vld1q_f32(chunk.as_ptr());
            sum_vec = vfmaq_f32(sum_vec, v, v);
        }
        let mut sum_sq = vaddvq_f32(sum_vec);
        for &x in rem {
            sum_sq += x * x;
        }

        let inv_norm = 1.0 / sum_sq.sqrt();
        let inv_norm_vec = vdupq_n_f32(inv_norm);
        let chunks_mut = data.chunks_exact_mut(4);
        for chunk in chunks_mut {
            let v = vld1q_f32(chunk.as_ptr());
            let res = vmulq_f32(v, inv_norm_vec);
            vst1q_f32(chunk.as_mut_ptr(), res);
        }

        let tail_start = (len / 4) * 4;
        for x in data.iter_mut().skip(tail_start) {
            *x *= inv_norm;
        }
    }
}

#[cfg(target_arch = "aarch64")]
#[inline(always)]
pub(crate) unsafe fn quantize_f32_to_i8_neon(input: &[f32]) -> Embedding {
    unsafe {
        let n = input.len();
        let mut output = vec![0i8; n];

        let scale_v = vdupq_n_f32(127.0);
        let mut i = 0;
        while i + 16 <= n {
            let x0 = vld1q_f32(input.as_ptr().add(i));
            let x1 = vld1q_f32(input.as_ptr().add(i + 4));
            let x2 = vld1q_f32(input.as_ptr().add(i + 8));
            let x3 = vld1q_f32(input.as_ptr().add(i + 12));

            let i0 = vcvtnq_s32_f32(vmulq_f32(x0, scale_v));
            let i1 = vcvtnq_s32_f32(vmulq_f32(x1, scale_v));
            let i2 = vcvtnq_s32_f32(vmulq_f32(x2, scale_v));
            let i3 = vcvtnq_s32_f32(vmulq_f32(x3, scale_v));

            let i01 = vcombine_s16(vqmovn_s32(i0), vqmovn_s32(i1));
            let i23 = vcombine_s16(vqmovn_s32(i2), vqmovn_s32(i3));
            let packed = vcombine_s8(vqmovn_s16(i01), vqmovn_s16(i23));

            vst1q_s8(output.as_mut_ptr().add(i), packed);
            i += 16;
        }
        for j in i..n {
            output[j] = (input[j] * 127.0).round().clamp(-127.0, 127.0) as i8;
        }
        Embedding::I8(output)
    }
}

#[cfg(target_arch = "aarch64")]
#[inline(always)]
pub(crate) unsafe fn dot_f32_neon(query: &QuerySimd, emb: &[f32]) -> f32 {
    unsafe {
        let query = match query {
            QuerySimd::F(e) => e,
            _ => panic!("dot_f32_neon only supports f32 embeddings"),
        };
        let mut sum = vdupq_n_f32(0.0);
        for (i, q) in query.iter().enumerate() {
            let v = vld1q_f32(emb.as_ptr().add(i * 4));
            sum = vfmaq_f32(sum, *q, v);
        }
        vaddvq_f32(sum)
    }
}

#[cfg(target_arch = "aarch64")]
#[inline(always)]
pub(crate) unsafe fn dot_i8_neon(query: &QuerySimd, emb: &[i8]) -> i32 {
    unsafe {
        let query = match query {
            QuerySimd::I(e) => e,
            _ => panic!("dot_i8_neon only supports i8 embeddings"),
        };
        let mut acc = vdupq_n_s32(0);
        for (i, q) in query.iter().enumerate() {
            let v = vld1q_s8(emb.as_ptr().add(i * 16));
            let prod_low = vmull_s8(vget_low_s8(*q), vget_low_s8(v));
            let prod_high = vmull_s8(vget_high_s8(*q), vget_high_s8(v));
            acc = vpadalq_s16(acc, prod_low);
            acc = vpadalq_s16(acc, prod_high);
        }
        vaddvq_s32(acc)
    }
}

#[cfg(target_arch = "aarch64")]
#[inline(always)]
pub(crate) unsafe fn euclidean_f32_neon(query: &QuerySimd, b: &[f32]) -> f32 {
    unsafe {
        let query = match query {
            QuerySimd::F(e) => e,
            _ => panic!("euclidean_f32_neon only supports f32 embeddings"),
        };
        let mut sum = vdupq_n_f32(0.0);
        for (i, q) in query.iter().enumerate() {
            let v = vld1q_f32(b.as_ptr().add(i * 4));
            let diff = vsubq_f32(*q, v);
            sum = vfmaq_f32(sum, diff, diff);
        }
        vaddvq_f32(sum)
    }
}

#[cfg(target_arch = "aarch64")]
#[inline(always)]
pub(crate) unsafe fn euclidean_i8_neon(query: &QuerySimd, b: &[i8]) -> i32 {
    unsafe {
        let query = match query {
            QuerySimd::I(e) => e,
            _ => panic!("euclidean_i8_neon only supports i8 embeddings"),
        };
        let mut acc = vdupq_n_s32(0);
        for (i, q) in query.iter().enumerate() {
            let v = vld1q_s8(b.as_ptr().add(i * 16));
            let q_lo = vmovl_s8(vget_low_s8(*q));
            let q_hi = vmovl_s8(vget_high_s8(*q));
            let v_lo = vmovl_s8(vget_low_s8(v));
            let v_hi = vmovl_s8(vget_high_s8(v));
            let diff_lo = vsubq_s16(q_lo, v_lo);
            let diff_hi = vsubq_s16(q_hi, v_hi);
            acc = vmlal_s16(acc, vget_low_s16(diff_lo), vget_low_s16(diff_lo));
            acc = vmlal_s16(acc, vget_high_s16(diff_lo), vget_high_s16(diff_lo));
            acc = vmlal_s16(acc, vget_low_s16(diff_hi), vget_low_s16(diff_hi));
            acc = vmlal_s16(acc, vget_high_s16(diff_hi), vget_high_s16(diff_hi));
        }
        vaddvq_s32(acc)
    }
}

#[cfg(target_arch = "aarch64")]
#[inline(always)]
pub(crate) unsafe fn quantize_neon(values: &[f32], scale: f32) -> Vec<i8> {
    unsafe {
        let inv_scale = 1.0 / scale;
        let scale_vec = vdupq_n_f32(inv_scale);

        let mut result = vec![0i8; values.len()];
        let mut i = 0;
        while i + 16 <= values.len() {
            let v0 = vld1q_f32(values.as_ptr().add(i));
            let v1 = vld1q_f32(values.as_ptr().add(i + 4));
            let v2 = vld1q_f32(values.as_ptr().add(i + 8));
            let v3 = vld1q_f32(values.as_ptr().add(i + 12));
            let i0 = vcvtaq_s32_f32(vmulq_f32(v0, scale_vec));
            let i1 = vcvtaq_s32_f32(vmulq_f32(v1, scale_vec));
            let i2 = vcvtaq_s32_f32(vmulq_f32(v2, scale_vec));
            let i3 = vcvtaq_s32_f32(vmulq_f32(v3, scale_vec));
            let p01 = vcombine_s16(vqmovn_s32(i0), vqmovn_s32(i1));
            let p23 = vcombine_s16(vqmovn_s32(i2), vqmovn_s32(i3));
            let packed = vcombine_s8(vqmovn_s16(p01), vqmovn_s16(p23));
            vst1q_s8(result.as_mut_ptr().add(i), packed);
            i += 16;
        }
        for j in i..values.len() {
            result[j] = (values[j] * inv_scale).round().clamp(-128.0, 127.0) as i8;
        }
        result
    }
}

#[cfg(target_arch = "aarch64")]
#[inline(always)]
pub(crate) unsafe fn quantize_affine_neon(values: &[f32], scale: f32, zero_point: i16) -> Vec<i8> {
    unsafe {
        let inv_scale = 1.0 / scale;
        let scale_vec = vdupq_n_f32(inv_scale);
        let zp_vec = vdupq_n_s32(zero_point as i32);
        let min_i32 = vdupq_n_s32(-128);
        let max_i32 = vdupq_n_s32(127);

        let mut result = vec![0i8; values.len()];
        let mut i = 0;
        while i + 16 <= values.len() {
            let v0 = vld1q_f32(values.as_ptr().add(i));
            let v1 = vld1q_f32(values.as_ptr().add(i + 4));
            let v2 = vld1q_f32(values.as_ptr().add(i + 8));
            let v3 = vld1q_f32(values.as_ptr().add(i + 12));
            let mut i0 = vcvtaq_s32_f32(vmulq_f32(v0, scale_vec));
            let mut i1 = vcvtaq_s32_f32(vmulq_f32(v1, scale_vec));
            let mut i2 = vcvtaq_s32_f32(vmulq_f32(v2, scale_vec));
            let mut i3 = vcvtaq_s32_f32(vmulq_f32(v3, scale_vec));
            i0 = vaddq_s32(i0, zp_vec);
            i1 = vaddq_s32(i1, zp_vec);
            i2 = vaddq_s32(i2, zp_vec);
            i3 = vaddq_s32(i3, zp_vec);
            i0 = vminq_s32(vmaxq_s32(i0, min_i32), max_i32);
            i1 = vminq_s32(vmaxq_s32(i1, min_i32), max_i32);
            i2 = vminq_s32(vmaxq_s32(i2, min_i32), max_i32);
            i3 = vminq_s32(vmaxq_s32(i3, min_i32), max_i32);
            let p01 = vcombine_s16(vqmovn_s32(i0), vqmovn_s32(i1));
            let p23 = vcombine_s16(vqmovn_s32(i2), vqmovn_s32(i3));
            let packed = vcombine_s8(vqmovn_s16(p01), vqmovn_s16(p23));
            vst1q_s8(result.as_mut_ptr().add(i), packed);
            i += 16;
        }
        for j in i..values.len() {
            result[j] =
                ((values[j] * inv_scale).round() as i32 + zero_point as i32).clamp(-128, 127) as i8;
        }
        result
    }
}

#[cfg(target_arch = "aarch64")]
#[inline(always)]
pub(crate) unsafe fn squared_norm_neon(data: &[i8]) -> i32 {
    unsafe {
        let mut acc = vdupq_n_s32(0);
        let mut i = 0;
        while i + 16 <= data.len() {
            let v = vld1q_s8(data.as_ptr().add(i));
            let v_lo = vmovl_s8(vget_low_s8(v));
            let v_hi = vmovl_s8(vget_high_s8(v));
            acc = vmlal_s16(acc, vget_low_s16(v_lo), vget_low_s16(v_lo));
            acc = vmlal_s16(acc, vget_high_s16(v_lo), vget_high_s16(v_lo));
            acc = vmlal_s16(acc, vget_low_s16(v_hi), vget_low_s16(v_hi));
            acc = vmlal_s16(acc, vget_high_s16(v_hi), vget_high_s16(v_hi));
            i += 16;
        }
        let mut total = vaddvq_s32(acc);
        for &x in &data[i..] {
            total += (x as i32) * (x as i32);
        }
        total
    }
}

#[cfg(target_arch = "aarch64")]
#[inline(always)]
pub(crate) unsafe fn max_abs_neon(values: &[f32]) -> f32 {
    unsafe {
        let mut max_vec = vdupq_n_f32(0.0);
        let mut i = 0;
        while i + 4 <= values.len() {
            let v = vld1q_f32(values.as_ptr().add(i));
            max_vec = vmaxq_f32(max_vec, vabsq_f32(v));
            i += 4;
        }
        let mut max_val = vmaxvq_f32(max_vec);
        for &x in &values[i..] {
            max_val = max_val.max(x.abs());
        }
        max_val
    }
}

#[cfg(target_arch = "aarch64")]
#[inline(always)]
pub(crate) unsafe fn max_neon(values: &[f32]) -> f32 {
    unsafe {
        if values.is_empty() {
            return f32::NEG_INFINITY;
        }
        let mut i = 0;
        let mut max_vec = if values.len() >= 4 {
            let v = vld1q_f32(values.as_ptr());
            i = 4;
            v
        } else {
            vdupq_n_f32(f32::NEG_INFINITY)
        };
        while i + 4 <= values.len() {
            max_vec = vmaxq_f32(max_vec, vld1q_f32(values.as_ptr().add(i)));
            i += 4;
        }
        let mut max_val = vmaxvq_f32(max_vec);
        for &x in &values[i..] {
            max_val = max_val.max(x);
        }
        max_val
    }
}

#[cfg(target_arch = "aarch64")]
#[inline(always)]
pub(crate) unsafe fn min_neon(values: &[f32]) -> f32 {
    unsafe {
        if values.is_empty() {
            return f32::INFINITY;
        }
        let mut i = 0;
        let mut min_vec = if values.len() >= 4 {
            let v = vld1q_f32(values.as_ptr());
            i = 4;
            v
        } else {
            vdupq_n_f32(f32::INFINITY)
        };
        while i + 4 <= values.len() {
            min_vec = vminq_f32(min_vec, vld1q_f32(values.as_ptr().add(i)));
            i += 4;
        }
        let mut min_val = vminvq_f32(min_vec);
        for &x in &values[i..] {
            min_val = min_val.min(x);
        }
        min_val
    }
}

#[cfg(target_arch = "aarch64")]
#[inline(always)]
pub(crate) unsafe fn sum_i8_neon(data: &[i8]) -> i32 {
    unsafe {
        let mut acc = vdupq_n_s16(0);
        let mut i = 0;
        while i + 16 <= data.len() {
            let v = vld1q_s8(data.as_ptr().add(i));
            acc = vaddq_s16(acc, vmovl_s8(vget_low_s8(v)));
            acc = vaddq_s16(acc, vmovl_s8(vget_high_s8(v)));
            i += 16;
        }
        let mut total = vaddlvq_s16(acc);
        for &x in &data[i..] {
            total += x as i32;
        }
        total
    }
}

#[cfg(target_arch = "aarch64")]
#[inline(always)]
pub(crate) fn euclidean_i8_quantized_neon(
    v1: &QuerySimd,
    scale1: f32,
    norm1: f32,
    v2: &[i8],
    scale2: f32,
    norm2: f32,
) -> f32 {
    let dot_i32: i32 = unsafe { dot_i8_neon(v1, v2) };
    let dot = dot_i32 as f32 * scale1 * scale2;
    (norm1 + norm2 - 2.0 * dot).max(0.0)
}

#[cfg(target_arch = "aarch64")]
#[inline(always)]
pub(crate) fn dot_i8_quantized_neon(v1: &QuerySimd, scale1: f32, v2: &[i8], scale2: f32) -> f32 {
    let dot_i32: i32 = unsafe { dot_i8_neon(v1, v2) };
    dot_i32 as f32 * scale1 * scale2
}

#[allow(clippy::too_many_arguments)]
#[cfg(target_arch = "aarch64")]
#[inline(always)]
pub(crate) fn euclidean_i8_quantized_affine_neon(
    v1: &QuerySimd,
    scale1: f32,
    norm1: f32,
    zero_point1: i16,
    sum_q1: i32,
    v2: &[i8],
    scale2: f32,
    norm2: f32,
    zero_point2: i16,
    sum_q2: i32,
) -> f32 {
    let dot_i32: i32 = unsafe { dot_i8_neon(v1, v2) };
    let n = v2.len() as i32;
    let dot_i32 = dot_i32 - zero_point2 as i32 * sum_q1 - zero_point1 as i32 * sum_q2
        + n * zero_point1 as i32 * zero_point2 as i32;
    let dot = dot_i32 as f32 * scale1 * scale2;
    (norm1 + norm2 - 2.0 * dot).max(0.0)
}

#[cfg(target_arch = "aarch64")]
#[allow(clippy::type_complexity)]
#[inline(always)]
pub(crate) unsafe fn similarity_embedding_view_neon(
    query: &QuerySimd,
    emb: &EmbeddingView,
    scale_norm: Option<(f32, f32, i16, i32, f32, f32, i16, i32)>,
    vector_similarity: VectorSimilarity,
    quantization: Quantization,
    non_affine: bool,
) -> f32 {
    unsafe {
        match (emb, vector_similarity, quantization) {
            (EmbeddingView::I8(e), VectorSimilarity::Dot, Quantization::ScalarQuantizationI8) => {
                if let Some((qs, _, _, _, es, _, _, _)) = scale_norm {
                    dot_i8_quantized_neon(query, qs, e, es)
                } else {
                    dot_i8_neon(query, e) as f32
                }
            }
            (EmbeddingView::I8(e), VectorSimilarity::Dot, Quantization::TurboQuantI8) => {
                if let Some((qs, _, _, _, es, _, _, _)) = scale_norm {
                    -TurboQuant::dot_i8_turboquant_neon(query, qs, e, es)
                } else {
                    dot_i8_neon(query, e) as f32
                }
            }
            (EmbeddingView::I8(e), VectorSimilarity::Dot, Quantization::None) => {
                dot_i8_neon(query, e) as f32
            }
            (EmbeddingView::F32(e), VectorSimilarity::Dot, _) => dot_f32_neon(query, e),
            (
                EmbeddingView::I8(e),
                VectorSimilarity::Cosine,
                Quantization::ScalarQuantizationI8,
            ) => {
                if let Some((qs, _, _, _, es, _, _, _)) = scale_norm {
                    dot_i8_quantized_neon(query, qs, e, es)
                } else {
                    dot_i8_neon(query, e) as f32
                }
            }
            (EmbeddingView::I8(e), VectorSimilarity::Cosine, Quantization::TurboQuantI8) => {
                if let Some((qs, _, _, _, es, _, _, _)) = scale_norm {
                    -TurboQuant::dot_i8_turboquant_neon(query, qs, e, es)
                } else {
                    dot_i8_neon(query, e) as f32
                }
            }
            (EmbeddingView::I8(e), VectorSimilarity::Cosine, Quantization::None) => {
                dot_i8_neon(query, e) as f32
            }
            (EmbeddingView::F32(e), VectorSimilarity::Cosine, _) => dot_f32_neon(query, e),
            (
                EmbeddingView::I8(e),
                VectorSimilarity::Euclidean,
                Quantization::ScalarQuantizationI8,
            ) => {
                if let Some((qs, qn, qzp, qsq, es, en, ezp, esq)) = scale_norm {
                    if non_affine {
                        -euclidean_i8_quantized_neon(query, qs, qn, e, es, en)
                    } else {
                        -euclidean_i8_quantized_affine_neon(
                            query, qs, qn, qzp, qsq, e, es, en, ezp, esq,
                        )
                    }
                } else {
                    -euclidean_i8_neon(query, e) as f32
                }
            }
            (EmbeddingView::I8(e), VectorSimilarity::Euclidean, Quantization::TurboQuantI8) => {
                if let Some((qs, qn, _, _, es, en, _, _)) = scale_norm {
                    -TurboQuant::euclidean_i8_turboquant_neon(query, qs, qn, e, es, en)
                } else {
                    -euclidean_i8_neon(query, e) as f32
                }
            }
            (EmbeddingView::I8(e), VectorSimilarity::Euclidean, Quantization::None) => {
                -euclidean_i8_neon(query, e) as f32
            }
            (EmbeddingView::F32(e), VectorSimilarity::Euclidean, _) => {
                -euclidean_f32_neon(query, e)
            }
        }
    }
}

#[cfg(target_arch = "aarch64")]
#[allow(clippy::type_complexity)]
#[inline(always)]
pub(crate) unsafe fn similarity_embedding_neon(
    query: &QuerySimd,
    emb: &Embedding,
    scale_norm: Option<(f32, f32, i16, i32, f32, f32, i16, i32)>,
    vector_similarity: VectorSimilarity,
    quantization: Quantization,
    non_affine: bool,
) -> f32 {
    let view = match emb {
        Embedding::I8(e) => EmbeddingView::I8(e.as_slice()),
        Embedding::F32(e) => EmbeddingView::F32(e.as_slice()),
    };
    unsafe {
        similarity_embedding_view_neon(
            query,
            &view,
            scale_norm,
            vector_similarity,
            quantization,
            non_affine,
        )
    }
}

#[cfg(target_arch = "aarch64")]
impl QuantizedVector {
    #[inline(always)]
    pub(crate) fn new_scale_neon(values: &[f32]) -> Self {
        unsafe {
            let max_val = max_abs_neon(values);
            let scale = max_val / 127.0;
            let data = quantize_neon(values, scale);
            Self {
                data,
                scale,
                norm: 0.0,
                zero_point: 0,
                sum_q: 0,
            }
        }
    }

    #[inline(always)]
    pub(crate) fn new_scale_norm_neon(values: &[f32]) -> Self {
        unsafe {
            let max_val = max_abs_neon(values);
            let scale = max_val / 127.0;
            let data = quantize_neon(values, scale);
            let norm = squared_norm_neon(&data);
            Self {
                data,
                scale,
                norm: norm as f32 * scale * scale,
                zero_point: 0,
                sum_q: 0,
            }
        }
    }

    #[inline(always)]
    pub(crate) fn new_scale_norm_affine_neon(
        min_vector_value: &mut f32,
        max_vector_value: &mut f32,
        values: &[f32],
    ) -> Self {
        unsafe {
            let mut max_val = max_neon(values);
            let mut min_val = min_neon(values);

            if min_val < *min_vector_value {
                *min_vector_value = min_val;
            } else {
                min_val = *min_vector_value;
            }
            if max_val > *max_vector_value {
                let max_val_power_of_two = Self::raster_range(max_val - min_val);
                *max_vector_value = max_val_power_of_two;
            } else {
                max_val = *max_vector_value;
            }

            let range = Self::raster_range(max_val - min_val);
            let scale = range / 255.0;
            let zero_point_f = -128.0 - (min_val / scale);
            let zero_point = zero_point_f.round().clamp(-128.0, 127.0) as i16;

            let data = quantize_affine_neon(values, scale, zero_point);
            let norm = squared_norm_neon(&data);
            let sum_q = sum_i8_neon(&data);
            let norm: i32 = norm - 2 * zero_point as i32 * sum_q
                + (data.len() as i32) * zero_point as i32 * zero_point as i32;
            Self {
                data,
                scale,
                norm: norm as f32 * scale * scale,
                zero_point,
                sum_q,
            }
        }
    }
}

#[cfg(target_arch = "aarch64")]
impl TurboQuant {
    /// In-place Fast Walsh-Hadamard Transform, NEON-accelerated for offsets ≥ 4.
    /// Mirrors the AVX2 implementation but uses 128-bit (4 f32) lanes.
    #[inline(always)]
    unsafe fn fwht_neon(a: &mut [f32]) {
        unsafe {
            let n = a.len();
            let mut h = 1;
            while h < n {
                if h < 4 {
                    for i in (0..n).step_by(h * 2) {
                        for j in i..i + h {
                            let x = a[j];
                            let y = a[j + h];
                            a[j] = x + y;
                            a[j + h] = x - y;
                        }
                    }
                } else {
                    for i in (0..n).step_by(h * 2) {
                        for j in (i..i + h).step_by(4) {
                            let va = vld1q_f32(a.as_ptr().add(j));
                            let vb = vld1q_f32(a.as_ptr().add(j + h));
                            let v_add = vaddq_f32(va, vb);
                            let v_sub = vsubq_f32(va, vb);
                            vst1q_f32(a.as_mut_ptr().add(j), v_add);
                            vst1q_f32(a.as_mut_ptr().add(j + h), v_sub);
                        }
                    }
                }
                h *= 2;
            }

            let norm_val = (n as f32).sqrt();
            let inv_norm = 1.0 / norm_val;
            let v_inv = vdupq_n_f32(inv_norm);
            let mut i = 0;
            while i + 4 <= n {
                let v = vld1q_f32(a.as_ptr().add(i));
                vst1q_f32(a.as_mut_ptr().add(i), vmulq_f32(v, v_inv));
                i += 4;
            }
            for x in a.iter_mut().skip(i) {
                *x /= norm_val;
            }
        }
    }

    #[inline(always)]
    unsafe fn hadamard_product_neon(padded_data: &mut [f32], seed_mask: &[f32]) {
        unsafe {
            let n = padded_data.len();
            let chunks = n / 4;
            for i in 0..chunks {
                let offset = i * 4;
                let data_vec = vld1q_f32(padded_data.as_ptr().add(offset));
                let mask_vec = vld1q_f32(seed_mask.as_ptr().add(offset));
                let result_vec = vmulq_f32(data_vec, mask_vec);
                vst1q_f32(padded_data.as_mut_ptr().add(offset), result_vec);
            }
            for i in (chunks * 4)..n {
                padded_data[i] *= seed_mask[i];
            }
        }
    }

    #[inline(always)]
    unsafe fn calculate_scale_neon(&self, rotated: &[f32]) -> f32 {
        unsafe {
            let chunks = rotated.chunks_exact(4);
            let rem = chunks.remainder();
            let mut sum_vec = vdupq_n_f32(0.0);
            for chunk in chunks {
                let v = vld1q_f32(chunk.as_ptr());
                sum_vec = vfmaq_f32(sum_vec, v, v);
            }
            let mut sum_sq = vaddvq_f32(sum_vec);
            for &x in rem {
                sum_sq += x * x;
            }
            let sigma = sum_sq.sqrt() / (self.dim as f32).sqrt();
            (sigma / 32.0).max(1e-8)
        }
    }

    #[inline(always)]
    pub(crate) fn quantize_f32_i8_neon(&self, vec: &[f32]) -> QuantizedVector {
        let mut padded_data = vec![0.0; self.dim];
        let len_to_copy = vec.len().min(self.dim);
        padded_data[..len_to_copy].copy_from_slice(&vec[..len_to_copy]);

        unsafe {
            Self::hadamard_product_neon(&mut padded_data, &self.seed_mask);
            Self::fwht_neon(&mut padded_data);
            let scale = self.calculate_scale_neon(&padded_data);
            let quantized = quantize_neon(&padded_data, scale);
            let squared_norm = squared_norm_neon(&quantized);
            QuantizedVector {
                data: quantized,
                scale,
                norm: squared_norm as f32 * scale * scale,
                zero_point: 0,
                sum_q: 0,
            }
        }
    }

    #[inline(always)]
    pub(crate) fn dot_i8_turboquant_neon(v1_q: &QuerySimd, scale1: f32, v2_q: &[i8], scale2: f32) -> f32 {
        let dot = unsafe { dot_i8_neon(v1_q, v2_q) };
        (dot as f32) * scale1 * scale2
    }

    #[inline(always)]
    pub(crate) fn euclidean_i8_turboquant_neon(
        v1_q: &QuerySimd,
        scale1: f32,
        norm1: f32,
        v2_q: &[i8],
        scale2: f32,
        norm2: f32,
    ) -> f32 {
        let dot_q = Self::dot_i8_turboquant_neon(v1_q, scale1, v2_q, scale2);
        (norm1 + norm2 - (2.0 * dot_q)).max(0.0)
    }
}

#[inline(always)]
pub(crate) unsafe fn normalize_f32_simd(data: &mut [f32]) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        normalize_f32_avx2(data)
    }
    #[cfg(target_arch = "aarch64")]
    unsafe {
        normalize_f32_neon(data)
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        normalize_f32(data)
    }
}

#[inline(always)]
pub(crate) unsafe fn quantize_f32_to_i8_simd(input: &[f32]) -> Embedding {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        quantize_f32_to_i8_avx2(input)
    }
    #[cfg(target_arch = "aarch64")]
    unsafe {
        quantize_f32_to_i8_neon(input)
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        quantize_f32_to_i8(input)
    }
}

#[allow(clippy::type_complexity)]
#[inline(always)]
pub(crate) unsafe fn similarity_embedding_view_simd(
    query: &QuerySimd,
    emb: &EmbeddingView,
    scale_norm: Option<(f32, f32, i16, i32, f32, f32, i16, i32)>,
    vector_similarity: VectorSimilarity,
    quantization: Quantization,
    non_affine: bool,
) -> f32 {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        similarity_embedding_view_avx2(
            query,
            emb,
            scale_norm,
            vector_similarity,
            quantization,
            non_affine,
        )
    }
    #[cfg(target_arch = "aarch64")]
    unsafe {
        similarity_embedding_view_neon(
            query,
            emb,
            scale_norm,
            vector_similarity,
            quantization,
            non_affine,
        )
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        let query_emb = match query {
            QuerySimd::I(q) => Embedding::I8(q.clone()),
            QuerySimd::F(q) => Embedding::F32(q.clone()),
        };
        similarity_embedding_view(
            &query_emb,
            emb,
            scale_norm,
            vector_similarity,
            quantization,
            non_affine,
        )
    }
}

#[allow(clippy::type_complexity)]
#[inline(always)]
pub(crate) unsafe fn similarity_embedding_simd(
    query: &QuerySimd,
    emb: &Embedding,
    scale_norm: Option<(f32, f32, i16, i32, f32, f32, i16, i32)>,
    vector_similarity: VectorSimilarity,
    quantization: Quantization,
    non_affine: bool,
) -> f32 {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        similarity_embedding_avx2(
            query,
            emb,
            scale_norm,
            vector_similarity,
            quantization,
            non_affine,
        )
    }
    #[cfg(target_arch = "aarch64")]
    unsafe {
        similarity_embedding_neon(
            query,
            emb,
            scale_norm,
            vector_similarity,
            quantization,
            non_affine,
        )
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        let query_emb = match query {
            QuerySimd::I(q) => Embedding::I8(q.clone()),
            QuerySimd::F(q) => Embedding::F32(q.clone()),
        };
        similarity_embedding(
            &query_emb,
            emb,
            scale_norm,
            vector_similarity,
            quantization,
            non_affine,
        )
    }
}

impl QuantizedVector {
    #[inline(always)]
    pub(crate) fn new_scale_simd(values: &[f32]) -> Self {
        #[cfg(target_arch = "x86_64")]
        {
            Self::new_scale_avx2(values)
        }
        #[cfg(target_arch = "aarch64")]
        {
            Self::new_scale_neon(values)
        }
        #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
        {
            Self::new_scale(values)
        }
    }

    #[inline(always)]
    pub(crate) fn new_scale_norm_simd(values: &[f32]) -> Self {
        #[cfg(target_arch = "x86_64")]
        {
            Self::new_scale_norm_avx2(values)
        }
        #[cfg(target_arch = "aarch64")]
        {
            Self::new_scale_norm_neon(values)
        }
        #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
        {
            Self::new_scale_norm(values)
        }
    }

    #[inline(always)]
    pub(crate) fn new_scale_norm_affine_simd(
        min_vector_value: &mut f32,
        max_vector_value: &mut f32,
        values: &[f32],
    ) -> Self {
        #[cfg(target_arch = "x86_64")]
        {
            Self::new_scale_norm_affine_avx2(min_vector_value, max_vector_value, values)
        }
        #[cfg(target_arch = "aarch64")]
        {
            Self::new_scale_norm_affine_neon(min_vector_value, max_vector_value, values)
        }
        #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
        {
            Self::new_scale_norm_affine(min_vector_value, max_vector_value, values)
        }
    }
}

impl TurboQuant {
    #[inline(always)]
    pub(crate) fn quantize_f32_i8_simd(&self, vec: &[f32]) -> QuantizedVector {
        #[cfg(target_arch = "x86_64")]
        {
            self.quantize_f32_i8_avx2(vec)
        }
        #[cfg(target_arch = "aarch64")]
        {
            self.quantize_f32_i8_neon(vec)
        }
        #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
        {
            self.quantize_f32_i8(vec)
        }
    }
}

#[cfg(all(test, target_arch = "aarch64"))]
mod neon_parity_tests {
    use super::*;

    fn make_f32(n: usize) -> Vec<f32> {
        (0..n)
            .map(|i| ((i as f32) * 0.137).sin() * 0.5 + (i as f32 * 0.013).cos() * 0.5)
            .collect()
    }

    fn make_i8(n: usize) -> Vec<i8> {
        (0..n)
            .map(|i| (((i as i32) * 17 + 5) % 251 - 125) as i8)
            .collect()
    }

    #[test]
    fn dot_f32_neon_matches_scalar() {
        let a = make_f32(128);
        let b = make_f32(128);
        let q = unsafe { QuerySimd::new(&Embedding::F32(a.clone())) };
        let neon = unsafe { dot_f32_neon(&q, &b) };
        let scalar = dot_f32(&a, &b);
        assert!(
            (neon - scalar).abs() < 1e-3,
            "neon {} scalar {}",
            neon,
            scalar
        );
    }

    #[test]
    fn dot_i8_neon_matches_scalar() {
        let a = make_i8(128);
        let b = make_i8(128);
        let q = unsafe { QuerySimd::new(&Embedding::I8(a.clone())) };
        let neon = unsafe { dot_i8_neon(&q, &b) };
        let scalar = dot_i8(&a, &b);
        assert_eq!(neon, scalar);
    }

    #[test]
    fn euclidean_f32_neon_matches_scalar() {
        let a = make_f32(128);
        let b = make_f32(128);
        let q = unsafe { QuerySimd::new(&Embedding::F32(a.clone())) };
        let neon = unsafe { euclidean_f32_neon(&q, &b) };
        let scalar = euclidean_f32(&a, &b);
        assert!(
            (neon - scalar).abs() < 1e-3,
            "neon {} scalar {}",
            neon,
            scalar
        );
    }

    #[test]
    fn euclidean_i8_neon_matches_scalar() {
        let a = make_i8(128);
        let b = make_i8(128);
        let q = unsafe { QuerySimd::new(&Embedding::I8(a.clone())) };
        let neon = unsafe { euclidean_i8_neon(&q, &b) };
        let scalar: i32 = a
            .iter()
            .zip(&b)
            .map(|(x, y)| (*x as i32 - *y as i32).pow(2))
            .sum();
        assert_eq!(neon, scalar);
    }

    #[test]
    fn squared_norm_neon_matches_scalar() {
        let v = make_i8(128);
        let neon = unsafe { squared_norm_neon(&v) };
        let scalar: i32 = v.iter().map(|&x| x as i32 * x as i32).sum();
        assert_eq!(neon, scalar);
    }

    #[test]
    fn sum_i8_neon_matches_scalar() {
        let v = make_i8(128);
        let neon = unsafe { sum_i8_neon(&v) };
        let scalar: i32 = v.iter().map(|&x| x as i32).sum();
        assert_eq!(neon, scalar);
    }

    #[test]
    fn max_abs_neon_matches_scalar() {
        let v = make_f32(128);
        let neon = unsafe { max_abs_neon(&v) };
        let scalar = v.iter().fold(0.0f32, |acc, &x| acc.max(x.abs()));
        assert!((neon - scalar).abs() < 1e-6);
    }

    #[test]
    fn normalize_f32_neon_matches_scalar() {
        let v = make_f32(128);
        let mut a = v.clone();
        let mut b = v.clone();
        unsafe { normalize_f32_neon(&mut a) };
        normalize_f32(&mut b);
        for (x, y) in a.iter().zip(&b) {
            assert!((x - y).abs() < 1e-5, "neon {} scalar {}", x, y);
        }
    }

    #[test]
    fn quantize_neon_matches_scalar() {
        let v = make_f32(128);
        let scale = 0.01;
        let neon = unsafe { quantize_neon(&v, scale) };
        let scalar: Vec<i8> = v
            .iter()
            .map(|&x| (x / scale).round().clamp(-128.0, 127.0) as i8)
            .collect();
        assert_eq!(neon, scalar);
    }

    #[test]
    fn quantize_f32_to_i8_neon_matches_scalar() {
        let v: Vec<f32> = (0..128)
            .map(|i| ((i as f32) * 0.017 - 1.0).clamp(-1.0, 1.0))
            .collect();
        let Embedding::I8(neon) = (unsafe { quantize_f32_to_i8_neon(&v) }) else {
            panic!("expected I8")
        };
        let Embedding::I8(scalar) = quantize_f32_to_i8(&v) else {
            panic!("expected I8")
        };
        for (n, s) in neon.iter().zip(&scalar) {
            assert!(
                (*n as i32 - *s as i32).abs() <= 1,
                "neon {} scalar {} differ by >1",
                n,
                s
            );
        }
    }
}
