use crate::vector_similarity::VectorSimilarity;
use crate::{
    clustering::{ClusterHeader, Medoid, ParentMedoid},
    index::{Clustering, Document, FieldType, IS_SYSTEM_LE, Shard, ShardArc},
    min_heap,
    search::ResultObject,
    utils::decode_bytes_from_base64_string,
    vector_similarity::{
        AnnMode, QuantizedVector, QuerySimd, normalize_f32, normalize_f32_simd, quantize_f32_to_i8,
        quantize_f32_to_i8_simd, similarity_embedding, similarity_embedding_simd,
        similarity_embedding_view, similarity_embedding_view_simd,
    },
};
use ahash::AHashSet;
use bytemuck::{Pod, Zeroable, bytes_of, cast_slice, from_bytes, try_cast_slice};
use chunk::chunk;
use memmap2::{Mmap, MmapOptions};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    fmt,
    io::{Seek, SeekFrom, Write},
};
use utoipa::ToSchema;

/// Normalization factor for cosine similarity when using i8 quantization.
/// This is equal to the maximum possible dot product of two 64-dimensional vectors with values in the range [-128, 127],
/// which occurs when both vectors are identical and all values are 127. The dot product in that case is 64 * 127^2 = 16129.0.
pub const SIMILARITY_NORMALIZATION_64_I8: f32 = 1.0 / 16129.0;

/// Vector precision
#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Deserialize, Serialize, ToSchema, Debug)]
pub enum Precision {
    /// No vector embedding, e.g. for lexical fields or fields without indexing.
    None = 0,
    /// 32-bit floating point vector embedding
    F32 = 1,
    /// 8-bit integer vector embedding
    I8 = 4,
}

impl fmt::Display for Precision {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Precision::None => write!(f, "None"),
            Precision::F32 => write!(f, "F32"),
            Precision::I8 => write!(f, "I8"),
        }
    }
}

/// Embedding with different vector precisions
#[derive(Clone, Debug)]
pub enum Embedding {
    /// 32-bit floating point vector embedding
    F32(Vec<f32>),
    /// 8-bit integer vector embedding
    I8(Vec<i8>),
}

#[repr(C)]
#[repr(packed)]
#[derive(Pod, Zeroable, Clone, Copy)]
pub(crate) struct VectorHeader {
    pub doc_id: u16,
    pub field_id: u32,
    pub chunk_id: u32,
    pub scale: f32,
    pub norm: f32,
    pub zero_point: i16,
    pub sum_q: i32,
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum EmbeddingView<'a> {
    F32(&'a [f32]),
    I8(&'a [i8]),
}

pub(crate) struct VectorRecordView<'a> {
    pub header: &'a VectorHeader,
    pub embedding: EmbeddingView<'a>,
}

/// Convert an embedding to a JSON value for exchange between different systems.
pub fn embedding_to_json(embedding: Embedding) -> Value {
    match embedding {
        Embedding::F32(v) => Value::Array(v.iter().map(|v| Value::from(*v)).collect()),
        Embedding::I8(v) => Value::Array(v.iter().map(|v| Value::from(*v)).collect()),
    }
}

/// Convert an embedding to a byte vector in big endian (network order) for exchange between different systems.
pub fn embedding_to_bytes_be(embedding: &Embedding) -> Vec<u8> {
    match embedding {
        Embedding::F32(v) => {
            let mut byte_array = Vec::with_capacity(size_of_val(v));
            byte_array.extend(v.iter().flat_map(|v| v.to_be_bytes()));
            byte_array
        }
        Embedding::I8(v) => {
            let mut byte_array = Vec::with_capacity(size_of_val(v));
            byte_array.extend(v.iter().flat_map(|v| v.to_be_bytes()));
            byte_array
        }
    }
}

/// Convert a byte slice to an embedding based on the specified vector type, dimensions, in big endian (network order)
/// for exchange between different systems.
pub fn embedding_from_bytes_be(
    bytes: &[u8],
    vector_type: Precision,
    dimensions: usize,
    is_system_le: bool,
) -> Option<Embedding> {
    match (vector_type, is_system_le) {
        (Precision::F32, true) => {
            if bytes.len() == dimensions * 4 {
                let chunks = bytes.chunks_exact(4);
                let vector = chunks
                    .map(|chunk| f32::from_be_bytes(chunk.try_into().unwrap()))
                    .collect();
                Some(Embedding::F32(vector))
            } else {
                None
            }
        }
        (Precision::F32, false) => {
            if let Ok(vector) = try_cast_slice(bytes)
                && vector.len() == dimensions
            {
                Some(Embedding::F32(vector.to_vec()))
            } else {
                None
            }
        }
        (Precision::I8, _) => {
            if let Ok(vector) = try_cast_slice(bytes)
                && vector.len() == dimensions
            {
                Some(Embedding::I8(vector.to_vec()))
            } else {
                None
            }
        }
        (Precision::None, _) => None,
    }
}

/// Convert a JSON value to an embedding based on the specified vector type and dimensions.
pub fn embedding_from_json(
    value: &Value,
    vector_type: Precision,
    dimensions: usize,
) -> Option<Embedding> {
    match vector_type {
        Precision::F32 => {
            if let Ok(vector) = serde_json::from_value::<Vec<f32>>(value.clone())
                && vector.len() == dimensions
            {
                Some(Embedding::F32(vector.to_vec()))
            } else {
                None
            }
        }
        Precision::I8 => {
            if let Ok(vector) = serde_json::from_value::<Vec<i8>>(value.clone())
                && vector.len() == dimensions
            {
                Some(Embedding::I8(vector.to_vec()))
            } else {
                None
            }
        }
        Precision::None => None,
    }
}

pub(crate) fn read_min_max(bytes: &[u8], dimensions: usize) -> (f32, f32) {
    let size = size_of::<VectorHeader>() + dimensions;
    if bytes.len() < size {
        return (f32::MIN, f32::MAX);
    }
    let start_last_vector = bytes.len() - size;
    let header: &VectorHeader =
        from_bytes(&bytes[start_last_vector..start_last_vector + size_of::<VectorHeader>()]);

    if header.zero_point == 0 && header.sum_q == 0 {
        return (0.0, f32::MIN);
    }

    let min_val = header.scale * (-header.zero_point - 128) as f32;
    let max_val = (127 - header.zero_point) as f32 * header.scale;
    (min_val, max_val)
}

pub(crate) fn read_record(
    bytes: &[u8],
    dimensions: usize,
    vector_type: Precision,
) -> VectorRecordView<'_> {
    let header: &VectorHeader = from_bytes(&bytes[..size_of::<VectorHeader>()]);

    let embedding = match vector_type {
        Precision::F32 => {
            let record_len = size_of::<VectorHeader>() + (dimensions * 4);
            let vec_offset = size_of::<VectorHeader>();
            let vec_bytes = &bytes[vec_offset..record_len];
            let vec_slice = cast_slice(vec_bytes);

            EmbeddingView::F32(vec_slice)
        }
        Precision::I8 => {
            let record_len = size_of::<VectorHeader>() + dimensions;
            let vec_offset = size_of::<VectorHeader>();
            let vec_bytes = &bytes[vec_offset..record_len];
            let vec_slice = cast_slice(vec_bytes);
            EmbeddingView::I8(vec_slice)
        }
        Precision::None => {
            panic!("VectorPrecision::None does not contain an embedding");
        }
    };

    VectorRecordView { header, embedding }
}

/// Quantization method for embeddings.
#[derive(Clone, Copy, Default, Debug, PartialEq, Deserialize, Serialize, ToSchema)]
pub enum Quantization {
    /// Affine Scalar Quantization (SQ) f32 to i8 (8 bit per dimension).
    /// Affine quantization (or asymmetric quantization) maps high-precision floating-point numbers (e.g., FP32) to lower-precision integers (e.g., INT8)
    /// by linearly transforming them using a scaling factor and a zero-point offset.
    ScalarQuantizationI8,
    /// TurboQuant Quantization (TQ) f32 to i8 (8 bit per dimension).
    /// SeekStorms implementation uses Fast Walsh-Hadamard Transform (FWHT) instead of Gram-Schmidt orthogonalization, and Quantized Johnson-Lindenstrauss (QJL) transformation.
    /// TurboQuant is generally considered superior to traditional Product Quantization (PQ) for high-dimensional, nearest neighbor search.
    /// TurboQuant provides higher recall, faster (nearly zero) indexing time, and better compression ratios compared to standard PQ techniques.
    /// PQ requires a costly "training" phase to generate codebooks, which is impractical for real-time search.
    /// TurboQuant uses random rotations to achieve near-optimal scalar quantization on the fly, eliminating this training time.
    /// &#x26A0; **CAUTION**: TurboQuant is NOT always better than Scalar Quantization.
    /// For SIFT1M, SeekStorms affine scalar quantization (recall@10=100%) is better than TurboQuant (recall@10=97.26%).
    /// Those vectors have less than 256 distinct values, which SeekStorms affine Scalar Quantization preserves distortionlessly, while TurboQuant introduces additional distortion due to the random rotations.
    TurboQuantI8,
    /// no quantization, keep f32
    #[default]
    None,
}

impl fmt::Display for Quantization {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Quantization::None => write!(f, "None"),
            Quantization::ScalarQuantizationI8 => write!(f, "ScalarQuantizationI8"),
            Quantization::TurboQuantI8 => write!(f, "TurboQuantI8"),
        }
    }
}

/// Predefined model type for embeddings.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, ToSchema)]
pub enum Model {
    /// nomic-ai/CodeRankEmbed, 16M parameters, 256 dimensions, English only, code retrieval.
    PotionCode16M,
    /// bge-base-en-v1.5: 512 dimensions, 32.3M parameters, English only, general purpose
    PotionBase32M,
    /// bge-base-multilingual-v1.5: 256 dimensions, 128M parameters, multilingual, general purpose
    PotionMultilingual128M,
    /// bge-retrieval-en-v1.5: 512 dimensions, 32.3M parameters, English only, retrieval
    PotionRetrieval32M,
    /// bge-base-en-v1.5: 256 dimensions, 7.5M parameters, English only, general purpose
    PotionBase8M,
    /// bge-base-en-v1.5: 128 dimensions, 3.7M parameters, English only, general purpose
    PotionBase4M,
    /// bge-base-en-v1.5: 64 dimensions, 1.8M parameters, English only, general purpose
    PotionBase2M,
}

/// Inference type, to transform input text into vector embeddings.  
/// This can be a predefined model2vec model, a custom model2vec model, an external inference, or no inference.
#[derive(Clone, Default, Debug, PartialEq, Deserialize, Serialize, ToSchema)]
pub enum Inference {
    /// Predefined model2vec models, already normalized + dot product = cosine similarity, use the same similarity metric that was used during the training of the embedding model.
    Model2Vec {
        /// Predefined model type for embeddings.
        model: Model,
        /// Chunk size for splitting input text, e.g. 1000 characters. This should be the same chunk size that was used during the training of the embedding model.
        chunk_size: usize,
        /// Quantization method for embeddings.
        quantization: Quantization,
    },
    /// Custom model2vec models, already normalized + dot product = cosine similarity, use the same similarity metric that was used during the training of the embedding model.
    Model2VecCustom {
        /// Model ID from Hugging Face or local path to model directory, e.g. "minishlab/potion-base-2M"
        path: String,
        /// Chunk size for splitting input text, e.g. 1000 characters. This should be the same chunk size that was used during the training of the embedding model.
        chunk_size: usize,
        /// Quantization method for embeddings.
        quantization: Quantization,
    },
    /// External inference
    External {
        /// Number of dimensions for the embeddings.
        dimensions: usize,
        /// Data type for embeddings.
        precision: Precision,
        /// Quantization method for embeddings.
        quantization: Quantization,
        /// Similarity metric to use for comparing embeddings, e.g. cosine similarity or euclidean distance.
        /// This should be the same similarity metric that was used during the training of the embedding model.
        similarity: VectorSimilarity,
    },
    /// No inference
    #[default]
    None,
}

impl fmt::Display for Inference {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Inference::Model2Vec {
                model,
                chunk_size,
                quantization: _,
            } => write!(f, "Model2Vec: {:?}, chunks: {} byte", model, chunk_size),
            Inference::Model2VecCustom {
                path,
                chunk_size,
                quantization: _,
            } => write!(f, "Model2VecCustom: {}, chunks: {} byte", path, chunk_size),
            Inference::External {
                dimensions: _,
                precision: _,
                quantization: _,
                similarity: _,
            } => write!(f, "External"),
            Inference::None => write!(f, "None"),
        }
    }
}

#[derive(Clone)]
struct Item {
    doc_id: usize,
    field_id: u32,
    chunk_id: u32,
    cluster_id: u32,
    level_id: u32,
    cluster_score: f32,
    score: f32,
}

#[derive(Clone)]
pub(crate) struct TopK {
    items: Vec<Item>,
    len: usize,
    k: usize,
    similarity_threshold_precalculated: f32,
    result_count_total: usize,
    observed_vector_count: usize,
    lowest_similarity_score: f32,
}

impl TopK {
    fn new(
        k: usize,
        similarity_threshold_option: Option<f32>,
        vector_similarity: VectorSimilarity,
    ) -> Self {
        Self {
            items: vec![
                Item {
                    doc_id: 0,
                    field_id: 0,
                    chunk_id: 0,
                    cluster_id: 0,
                    level_id: 0,
                    cluster_score: f32::MIN,
                    score: f32::MIN
                };
                k
            ],
            len: 0,
            k,
            result_count_total: 0,
            similarity_threshold_precalculated: if let Some(similarity_threshold) =
                similarity_threshold_option
            {
                match vector_similarity {
                    VectorSimilarity::Dot => {
                        ((similarity_threshold * 2.0) - 1.0) / SIMILARITY_NORMALIZATION_64_I8
                    }
                    VectorSimilarity::Cosine => {
                        ((similarity_threshold * 2.0) - 1.0) / SIMILARITY_NORMALIZATION_64_I8
                    }
                    VectorSimilarity::Euclidean => -similarity_threshold,
                }
            } else {
                f32::MIN
            },
            observed_vector_count: 0,
            lowest_similarity_score: f32::MIN,
        }
    }

    #[allow(clippy::too_many_arguments)]
    #[inline(always)]
    fn push(
        &mut self,
        doc_id: usize,
        field_id: u32,
        chunk_id: u32,
        cluster_id: u32,
        level_id: u32,
        cluster_score: f32,
        score: f32,
        _shard_id: u64,
    ) -> bool {
        self.observed_vector_count += 1;

        if score < self.similarity_threshold_precalculated
            || (self.len == self.k && score <= self.lowest_similarity_score)
        {
            return false;
        }

        self.result_count_total += 1;

        if self.len < self.k {
            let new_item = Item {
                doc_id,
                field_id,
                chunk_id,
                cluster_id,
                level_id,
                cluster_score,
                score,
            };
            for item in self.items.iter_mut().take(self.len) {
                if item.doc_id == doc_id {
                    if score > item.score {
                        item.score = score;
                        item.field_id = field_id;
                        item.chunk_id = chunk_id;
                        item.cluster_id = cluster_id;
                        item.level_id = level_id;
                        item.cluster_score = cluster_score;
                    }
                    return true;
                }
            }

            self.items[self.len] = new_item;
            self.len += 1;
            return true;
        }

        let mut min_i = 0;
        let mut min_v = self.items[0].score;
        for (i, item) in self.items.iter_mut().enumerate().take(self.len) {
            if item.doc_id == doc_id {
                if score > item.score {
                    item.score = score;
                    item.field_id = field_id;
                    item.chunk_id = chunk_id;
                    item.cluster_id = cluster_id;
                    item.level_id = level_id;
                    item.cluster_score = cluster_score;
                }
                return true;
            }

            if item.score < min_v {
                min_v = item.score;
                min_i = i;
            }
        }

        if score > min_v {
            self.lowest_similarity_score = min_v;
            self.items[min_i] = Item {
                doc_id,
                field_id,
                chunk_id,
                cluster_id,
                level_id,
                cluster_score,
                score,
            };
            true
        } else {
            false
        }
    }
}

impl Shard {
    pub(crate) async fn embed_vector_shard(&mut self) {
        let index = self.index_option.as_ref().unwrap().read().await;
        let model = if let Some(model) = index.embedding_model_option.as_ref() {
            model
        } else {
            return;
        };

        let embeddings = model.encode(&self.chunks_string);
        for (i, embedding) in embeddings.iter().enumerate() {
            let embedding = if self.quantization == Quantization::ScalarQuantizationI8
                || self.quantization == Quantization::TurboQuantI8
            {
                if self.is_simd {
                    unsafe { quantize_f32_to_i8_simd(embedding) }
                } else {
                    quantize_f32_to_i8(embedding)
                }
            } else {
                Embedding::F32(embedding.to_vec())
            };

            let record = ParentMedoid {
                medoid_index: 0,
                similarity: 0.0,
                is_medoid: false,

                doc_id: self.chunks_meta[i].0,
                field_id: self.chunks_meta[i].1,
                chunk_id: self.chunks_meta[i].2,
                scale: 0.0,
                norm: 0.0,
                zero_point: 0,
                sum_q: 0,
                embedding,
            };

            self.block_vector_buffer.push(record);
        }

        self.chunks_meta.clear();
        self.chunks_string.clear();
    }

    pub(crate) async fn index_vector_shard(&mut self, doc_id: usize, document: &Document) {
        if !self.is_vector_indexing {
            return;
        }

        let doc_id = (doc_id & 0xFFFF) as u16;

        let schema = self.indexed_schema_vec.clone();
        for schema_field in schema.iter() {
            if schema_field.index_vector
                && let Some(field_value) = document.get(&schema_field.field)
            {
                match schema_field.field_type {
                    FieldType::Text => {
                        let text = serde_json::from_value::<String>(field_value.clone())
                            .unwrap_or(field_value.to_string());
                        let field_id = schema_field.indexed_field_id as u32;
                        let chunks: Vec<&[u8]> = chunk(text.as_bytes())
                            .delimiters(b"\n.?!")
                            .size(self.chunk_size)
                            .collect();

                        for (chunk_id, chunk) in chunks.iter().enumerate() {
                            let chunk_text = String::from_utf8_lossy(chunk).to_string();

                            self.chunks_meta.push((doc_id, field_id, chunk_id as u32));
                            self.chunks_string.push(chunk_text);
                            self.indexed_vector_count += 1;

                            if self.chunks_string.len() >= 256 {
                                self.embed_vector_shard().await;
                            }
                        }
                    }

                    FieldType::Json => {
                        if let Some(mut embedding) = embedding_from_json(
                            field_value,
                            self.vector_precision,
                            self.vector_dimensions_original,
                        ) {
                            if self.vector_similarity == VectorSimilarity::Cosine
                                && matches!(self.meta.inference, Inference::External { .. })
                                && let Embedding::F32(ref mut fvecs) = embedding
                            {
                                if self.is_simd {
                                    unsafe {
                                        normalize_f32_simd(fvecs);
                                    }
                                } else {
                                    normalize_f32(fvecs);
                                }
                            };

                            let (scale, norm, zero_point, sum_q) = if (self.quantization
                                == Quantization::ScalarQuantizationI8
                                || self.quantization == Quantization::TurboQuantI8)
                                && let Embedding::F32(ref fvecs) = embedding
                            {
                                match (self.vector_similarity, self.quantization, self.is_simd) {
                                    (
                                        VectorSimilarity::Cosine,
                                        Quantization::ScalarQuantizationI8,
                                        true,
                                    ) => {
                                        embedding = unsafe { quantize_f32_to_i8_simd(fvecs) };

                                        (1.0, 0.0, 0, 0)
                                    }
                                    (
                                        VectorSimilarity::Cosine,
                                        Quantization::ScalarQuantizationI8,
                                        false,
                                    ) => {
                                        embedding = quantize_f32_to_i8(fvecs);
                                        (1.0, 0.0, 0, 0)
                                    }

                                    (
                                        VectorSimilarity::Dot,
                                        Quantization::ScalarQuantizationI8,
                                        true,
                                    ) => {
                                        let quantized_vector =
                                            QuantizedVector::new_scale_simd(fvecs);
                                        embedding = Embedding::I8(quantized_vector.data);
                                        (
                                            quantized_vector.scale,
                                            quantized_vector.norm,
                                            quantized_vector.zero_point,
                                            quantized_vector.sum_q,
                                        )
                                    }
                                    (
                                        VectorSimilarity::Dot,
                                        Quantization::ScalarQuantizationI8,
                                        false,
                                    ) => {
                                        let quantized_vector = QuantizedVector::new_scale(fvecs);
                                        embedding = Embedding::I8(quantized_vector.data);
                                        (
                                            quantized_vector.scale,
                                            quantized_vector.norm,
                                            quantized_vector.zero_point,
                                            quantized_vector.sum_q,
                                        )
                                    }

                                    (
                                        VectorSimilarity::Euclidean,
                                        Quantization::ScalarQuantizationI8,
                                        true,
                                    ) => {
                                        let non_affine = if self.min_vector_value == f32::MAX {
                                            fvecs
                                                .iter()
                                                .any(|x| x.fract() != 0.0 || *x < 0.0 || *x > 255.0)
                                        } else {
                                            self.max_vector_value == f32::MIN
                                        };

                                        let quantized_vector = if non_affine {
                                            self.min_vector_value = 0.0;
                                            QuantizedVector::new_scale_norm_simd(fvecs)
                                        } else {
                                            QuantizedVector::new_scale_norm_affine_simd(
                                                &mut self.min_vector_value,
                                                &mut self.max_vector_value,
                                                fvecs,
                                            )
                                        };

                                        embedding = Embedding::I8(quantized_vector.data);
                                        (
                                            quantized_vector.scale,
                                            quantized_vector.norm,
                                            quantized_vector.zero_point,
                                            quantized_vector.sum_q,
                                        )
                                    }

                                    (_, Quantization::TurboQuantI8, true) => {
                                        let quantized_vector =
                                            self.turbo_quant.quantize_f32_i8_simd(fvecs);

                                        embedding = Embedding::I8(quantized_vector.data);
                                        (
                                            quantized_vector.scale,
                                            quantized_vector.norm,
                                            quantized_vector.zero_point,
                                            quantized_vector.sum_q,
                                        )
                                    }

                                    (
                                        VectorSimilarity::Euclidean,
                                        Quantization::ScalarQuantizationI8,
                                        false,
                                    ) => {
                                        let non_affine = if self.min_vector_value == f32::MAX {
                                            fvecs
                                                .iter()
                                                .any(|x| x.fract() != 0.0 || *x < 0.0 || *x > 255.0)
                                        } else {
                                            self.max_vector_value == f32::MIN
                                        };
                                        let quantized_vector = if non_affine {
                                            self.min_vector_value = 0.0;
                                            QuantizedVector::new_scale_norm(fvecs)
                                        } else {
                                            QuantizedVector::new_scale_norm_affine(
                                                &mut self.min_vector_value,
                                                &mut self.max_vector_value,
                                                fvecs,
                                            )
                                        };

                                        embedding = Embedding::I8(quantized_vector.data);
                                        (
                                            quantized_vector.scale,
                                            quantized_vector.norm,
                                            quantized_vector.zero_point,
                                            quantized_vector.sum_q,
                                        )
                                    }

                                    (_, Quantization::TurboQuantI8, false) => {
                                        let quantized_vector =
                                            self.turbo_quant.quantize_f32_i8(fvecs);

                                        embedding = Embedding::I8(quantized_vector.data);
                                        (
                                            quantized_vector.scale,
                                            quantized_vector.norm,
                                            quantized_vector.zero_point,
                                            quantized_vector.sum_q,
                                        )
                                    }
                                    (_, Quantization::None, _) => (0.0, 0.0, 0, 0),
                                }
                            } else {
                                (0.0, 0.0, 0, 0)
                            };

                            let record = ParentMedoid {
                                medoid_index: 0,
                                similarity: 0.0,
                                is_medoid: false,

                                doc_id,
                                field_id: schema_field.indexed_field_id as u32,
                                chunk_id: 0,
                                scale,
                                norm,
                                zero_point,
                                sum_q,
                                embedding,
                            };
                            self.block_vector_buffer.push(record);
                            self.indexed_vector_count += 1;
                        }
                    }

                    FieldType::Binary => {
                        if let Ok(string_base64) =
                            serde_json::from_value::<String>(field_value.clone())
                            && let Ok(bytes) = decode_bytes_from_base64_string(&string_base64)
                            && let Some(mut embedding) = embedding_from_bytes_be(
                                &bytes,
                                self.vector_precision,
                                self.vector_dimensions_original,
                                *IS_SYSTEM_LE,
                            )
                        {
                            if self.vector_similarity == VectorSimilarity::Cosine
                                && matches!(self.meta.inference, Inference::External { .. })
                                && let Embedding::F32(ref mut fvecs) = embedding
                            {
                                if self.is_simd {
                                    unsafe {
                                        normalize_f32_simd(fvecs);
                                    }
                                } else {
                                    normalize_f32(fvecs);
                                }
                            };

                            let (scale, norm, zero_point, sum_q) = if (self.quantization
                                == Quantization::ScalarQuantizationI8
                                || self.quantization == Quantization::TurboQuantI8)
                                && let Embedding::F32(ref fvecs) = embedding
                            {
                                match (self.vector_similarity, self.quantization, self.is_simd) {
                                    (
                                        VectorSimilarity::Cosine,
                                        Quantization::ScalarQuantizationI8,
                                        true,
                                    ) => {
                                        embedding = unsafe { quantize_f32_to_i8_simd(fvecs) };

                                        (1.0, 0.0, 0, 0)
                                    }

                                    (
                                        VectorSimilarity::Cosine,
                                        Quantization::ScalarQuantizationI8,
                                        false,
                                    ) => {
                                        embedding = quantize_f32_to_i8(fvecs);

                                        (1.0, 0.0, 0, 0)
                                    }

                                    (
                                        VectorSimilarity::Dot,
                                        Quantization::ScalarQuantizationI8,
                                        true,
                                    ) => {
                                        let quantized_vector =
                                            QuantizedVector::new_scale_simd(fvecs);

                                        embedding = Embedding::I8(quantized_vector.data);
                                        (
                                            quantized_vector.scale,
                                            quantized_vector.norm,
                                            quantized_vector.zero_point,
                                            quantized_vector.sum_q,
                                        )
                                    }
                                    (
                                        VectorSimilarity::Dot,
                                        Quantization::ScalarQuantizationI8,
                                        false,
                                    ) => {
                                        let quantized_vector = QuantizedVector::new_scale(fvecs);

                                        embedding = Embedding::I8(quantized_vector.data);
                                        (
                                            quantized_vector.scale,
                                            quantized_vector.norm,
                                            quantized_vector.zero_point,
                                            quantized_vector.sum_q,
                                        )
                                    }

                                    (
                                        VectorSimilarity::Euclidean,
                                        Quantization::ScalarQuantizationI8,
                                        true,
                                    ) => {
                                        let non_affine = if self.min_vector_value == f32::MAX {
                                            fvecs
                                                .iter()
                                                .any(|x| x.fract() != 0.0 || *x < 0.0 || *x > 255.0)
                                        } else {
                                            self.max_vector_value == f32::MIN
                                        };
                                        let quantized_vector = if non_affine {
                                            self.min_vector_value = 0.0;
                                            QuantizedVector::new_scale_norm_simd(fvecs)
                                        } else {
                                            QuantizedVector::new_scale_norm_affine_simd(
                                                &mut self.min_vector_value,
                                                &mut self.max_vector_value,
                                                fvecs,
                                            )
                                        };

                                        embedding = Embedding::I8(quantized_vector.data);
                                        (
                                            quantized_vector.scale,
                                            quantized_vector.norm,
                                            quantized_vector.zero_point,
                                            quantized_vector.sum_q,
                                        )
                                    }

                                    (_, Quantization::TurboQuantI8, true) => {
                                        let quantized_vector =
                                            self.turbo_quant.quantize_f32_i8_simd(fvecs);

                                        embedding = Embedding::I8(quantized_vector.data);
                                        (
                                            quantized_vector.scale,
                                            quantized_vector.norm,
                                            quantized_vector.zero_point,
                                            quantized_vector.sum_q,
                                        )
                                    }

                                    (
                                        VectorSimilarity::Euclidean,
                                        Quantization::ScalarQuantizationI8,
                                        false,
                                    ) => {
                                        let non_affine = if self.min_vector_value == f32::MAX {
                                            fvecs
                                                .iter()
                                                .any(|x| x.fract() != 0.0 || *x < 0.0 || *x > 255.0)
                                        } else {
                                            self.max_vector_value == f32::MIN
                                        };
                                        let quantized_vector = if non_affine {
                                            self.min_vector_value = 0.0;
                                            QuantizedVector::new_scale_norm(fvecs)
                                        } else {
                                            QuantizedVector::new_scale_norm_affine(
                                                &mut self.min_vector_value,
                                                &mut self.max_vector_value,
                                                fvecs,
                                            )
                                        };

                                        embedding = Embedding::I8(quantized_vector.data);
                                        (
                                            quantized_vector.scale,
                                            quantized_vector.norm,
                                            quantized_vector.zero_point,
                                            quantized_vector.sum_q,
                                        )
                                    }

                                    (_, Quantization::TurboQuantI8, false) => {
                                        let quantized_vector =
                                            self.turbo_quant.quantize_f32_i8(fvecs);

                                        embedding = Embedding::I8(quantized_vector.data);
                                        (
                                            quantized_vector.scale,
                                            quantized_vector.norm,
                                            quantized_vector.zero_point,
                                            quantized_vector.sum_q,
                                        )
                                    }
                                    (_, Quantization::None, _) => (0.0, 0.0, 0, 0),
                                }
                            } else {
                                (0.0, 0.0, 0, 0)
                            };

                            let record = ParentMedoid {
                                medoid_index: 0,
                                similarity: 0.0,
                                is_medoid: false,

                                doc_id,
                                field_id: schema_field.indexed_field_id as u32,
                                chunk_id: 0,
                                scale,
                                norm,
                                zero_point,
                                sum_q,
                                embedding,
                            };
                            self.block_vector_buffer.push(record);
                            self.indexed_vector_count += 1;
                        }
                    }

                    _ => {}
                }
            };
        }
    }

    pub(crate) async fn commit_vector_shard(&mut self) {
        if self.is_last_level_incomplete {
            let vector_dimensions = self.vector_dimensions;
            let vector_type = match self.quantization {
                Quantization::ScalarQuantizationI8 => Precision::I8,
                Quantization::TurboQuantI8 => Precision::I8,
                _ => self.vector_precision,
            };
            let vector_size = size_of::<VectorHeader>()
                + (vector_dimensions
                    * match vector_type {
                        Precision::F32 => 4,
                        Precision::I8 => 1,
                        Precision::None => 0,
                    });

            let mut offset = self.last_level_vector_file_start_pos as usize;

            let cluster_number_bytes = &self.vector_file_mmap[offset..offset + 4];
            let cluster_number =
                u32::from_le_bytes(cluster_number_bytes.try_into().unwrap()) as usize;
            offset += 4;

            let mut clusters = Vec::with_capacity(cluster_number);
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
            }

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

                        doc_id: record.header.doc_id,
                        field_id: record.header.field_id,
                        chunk_id: record.header.chunk_id,
                        scale: record.header.scale,
                        norm: record.header.norm,
                        zero_point: record.header.zero_point,
                        sum_q: record.header.sum_q,
                        embedding: match record.embedding {
                            EmbeddingView::I8(e) => Embedding::I8(e.to_vec()),
                            EmbeddingView::F32(e) => Embedding::F32(e.to_vec()),
                        },
                    });
                }
            }

            self.vector_file_mmap = unsafe {
                MmapOptions::new()
                    .len(0)
                    .map(&self.vector_file)
                    .expect("Unable to create Mmap")
            };

            if let Err(e) = self
                .vector_file
                .set_len(self.last_level_vector_file_start_pos)
            {
                println!(
                    "Unable to vector_file.set_len in commit_vector_shard {} {} {:?}",
                    self.index_path_string, self.indexed_doc_count, e
                )
            };

            let _ = self
                .vector_file
                .seek(SeekFrom::Start(self.last_level_vector_file_start_pos));
        } else {
            self.last_level_vector_file_start_pos = self.vector_file.stream_position().unwrap();
        }

        if self.chunks_string.is_empty() && self.block_vector_buffer.is_empty() {
            return;
        }

        if !self.chunks_string.is_empty() {
            self.embed_vector_shard().await;
        }

        let enable_clustering = if let Clustering::Fixed(size) = self.meta.clustering {
            size > 1 && self.block_vector_buffer.len() >= 100
        } else if let Clustering::None = self.meta.clustering {
            false
        } else {
            self.block_vector_buffer.len() >= 100
        };

        let medoids = if enable_clustering {
            self.cluster_vector_shard(true).await
        } else {
            vec![Medoid {
                medoid_index: 0,
                child_count: self.block_vector_buffer.len(),
            }]
        };
        self.indexed_cluster_count += medoids.len();

        let mut header = [0u8; 4];
        header[..4].copy_from_slice(&(medoids.len() as u32).to_le_bytes());
        let _ = self.vector_file.write_all(&header);

        for medoid in medoids.iter() {
            header[..4].copy_from_slice(&(medoid.child_count as u32).to_le_bytes());
            let _ = self.vector_file.write_all(&header);
        }

        for record in self.block_vector_buffer.iter() {
            let vec_bytes: &[u8] = match &record.embedding {
                Embedding::F32(v) => cast_slice(v.as_slice()),
                Embedding::I8(v) => cast_slice(v.as_slice()),
            };

            let header = VectorHeader {
                doc_id: record.doc_id,
                field_id: record.field_id,
                chunk_id: record.chunk_id,
                scale: record.scale,
                norm: record.norm,
                zero_point: record.zero_point,
                sum_q: record.sum_q,
            };

            let header_bytes = bytes_of(&header);
            let _ = self.vector_file.write_all(header_bytes);
            let _ = self.vector_file.write_all(vec_bytes);
        }
        self.block_vector_buffer.clear();

        self.vector_file.flush().expect("Unable to flush Mmap");
        self.vector_file_mmap =
            unsafe { Mmap::map(&self.vector_file).expect("Unable to create Mmap") };
    }
}

#[allow(clippy::too_many_arguments)]
#[allow(async_fn_in_trait)]
pub(crate) trait SearchVectorShard {
    async fn search_vector_shard(
        &self,
        query_vector: Option<(Embedding, f32, f32, i16, i32)>,
        length: usize,
        include_uncommitted: bool,
        similarity_threshold: Option<f32>,
        cluster_search: AnnMode,
        field_filter: Vec<String>,
    ) -> ResultObject;
}

/// Defines the source of search results, which can be lexical, vector-based, or a hybrid of both.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, Default)]
pub enum ResultSource {
    /// Results obtained from traditional lexical search methods, such as BM25.
    #[default]
    Lexical,
    /// Results obtained from vector-based search methods, such as ANN or exhaustive search.
    Vector,
    /// Results obtained from a combination of both lexical and vector-based search methods.
    Hybrid,
}

impl Shard {
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn search_vector_shard_uncommitted(
        &self,
        query_simd: &QuerySimd,
        query_embedding: &Embedding,
        scale: f32,
        norm: f32,
        zero_point: i16,
        sum_q: i32,
        vector_similarity: &VectorSimilarity,
        field_filter_set: &AHashSet<u16>,
        top_k: &mut TopK,
    ) {
        let level_id = self.level_index.len();
        let enable_scale = self.quantization != Quantization::None
            && self.vector_similarity != VectorSimilarity::Cosine;

        let non_affine = self.max_vector_value == f32::MIN;

        for record in self.block_vector_buffer.iter() {
            if field_filter_set.is_empty() || field_filter_set.contains(&(record.field_id as u16)) {
                let scale_norm = if enable_scale {
                    Some((
                        scale,
                        norm,
                        zero_point,
                        sum_q,
                        record.scale,
                        record.norm,
                        record.zero_point,
                        record.sum_q,
                    ))
                } else {
                    None
                };
                let similarity = if self.is_simd {
                    unsafe {
                        similarity_embedding_simd(
                            query_simd,
                            &record.embedding,
                            scale_norm,
                            *vector_similarity,
                            self.quantization,
                            non_affine,
                        )
                    }
                } else {
                    similarity_embedding(
                        query_embedding,
                        &record.embedding,
                        scale_norm,
                        *vector_similarity,
                        self.quantization,
                        non_affine,
                    )
                };
                let doc_id = (level_id << 16) | (record.doc_id as usize);
                top_k.push(
                    doc_id,
                    record.field_id,
                    record.chunk_id,
                    0,
                    level_id as u32,
                    0.0,
                    similarity,
                    self.meta.id,
                );
            }
        }
    }
}
impl SearchVectorShard for ShardArc {
    async fn search_vector_shard(
        &self,
        query_vector: Option<(Embedding, f32, f32, i16, i32)>,
        length: usize,
        include_uncommitted: bool,
        similarity_threshold: Option<f32>,
        ann_mode: AnnMode,
        field_filter: Vec<String>,
    ) -> ResultObject {
        let mut result_object: ResultObject = Default::default();

        if include_uncommitted && !self.read().await.chunks_string.is_empty() {
            self.write().await.embed_vector_shard().await;
        }

        let shard_ref = self.read().await;
        let mut observed_cluster_count = 0;

        let non_affine = shard_ref.max_vector_value == f32::MIN;

        if !shard_ref.is_vector_indexing || shard_ref.indexed_vector_count == 0 {
            return result_object;
        }

        let mut field_filter_set: AHashSet<u16> = AHashSet::new();
        for item in field_filter.iter() {
            match shard_ref.schema_map.get(item) {
                Some(value) => {
                    if value.index_lexical {
                        field_filter_set.insert(value.indexed_field_id as u16);
                    }
                }
                None => {
                    println!("field not found: {}", item)
                }
            }
        }

        let vector_similarity = shard_ref.vector_similarity;
        let vector_dimensions = shard_ref.vector_dimensions;
        let vector_type = match shard_ref.quantization {
            Quantization::ScalarQuantizationI8 => Precision::I8,
            Quantization::TurboQuantI8 => Precision::I8,
            _ => shard_ref.vector_precision,
        };
        let vector_size = size_of::<VectorHeader>()
            + (vector_dimensions
                * match vector_type {
                    Precision::F32 => 4,
                    Precision::I8 => 1,
                    Precision::None => 0,
                });

        let query_embedding = query_vector.unwrap();

        let query_simd = unsafe { QuerySimd::new(&query_embedding.0) };

        let mut top_k = TopK::new(length, similarity_threshold, vector_similarity);

        if include_uncommitted && shard_ref.uncommitted && !shard_ref.block_vector_buffer.is_empty()
        {
            shard_ref
                .search_vector_shard_uncommitted(
                    &query_simd,
                    &query_embedding.0,
                    query_embedding.1,
                    query_embedding.2,
                    query_embedding.3,
                    query_embedding.4,
                    &vector_similarity,
                    &field_filter_set,
                    &mut top_k,
                )
                .await;
        }

        let mut offset = 0;
        for level_id in 0..shard_ref.level_index.len() {
            let cluster_number_bytes = &shard_ref.vector_file_mmap[offset..offset + 4];
            let cluster_number =
                u32::from_le_bytes(cluster_number_bytes.try_into().unwrap()) as usize;
            offset += 4;

            let mut clusters = Vec::with_capacity(cluster_number);
            let mut level_vectors_count = 0;
            let mut start_index = 0;
            for _i in 0..cluster_number {
                let cluster_header_bytes = &shard_ref.vector_file_mmap[offset..offset + 4];
                let cluster_header = ClusterHeader {
                    start_index,
                    child_count: u32::from_le_bytes(cluster_header_bytes.try_into().unwrap()),
                };
                offset += 4;
                start_index += cluster_header.child_count;
                clusters.push(cluster_header);
                level_vectors_count += cluster_header.child_count;
            }

            let (n_probe, cluster_similarity_threshold) = match ann_mode {
                AnnMode::All => (clusters.len(), None),
                AnnMode::Similaritythreshold(threshold) => (clusters.len(), Some(threshold)),
                AnnMode::Nprobe(n_probe) => (n_probe.min(clusters.len()), None),
                AnnMode::NprobeSimilaritythreshold(n_probe, threshold) => {
                    (n_probe.min(clusters.len()), Some(threshold))
                }
            };

            let enable_scale = shard_ref.quantization != Quantization::None
                && shard_ref.vector_similarity != VectorSimilarity::Cosine;
            let selected_clusters: Vec<(u32, u32, f32, ClusterHeader)> = if ann_mode != AnnMode::All
            {
                let mut top_k_medoid =
                    TopK::new(n_probe, cluster_similarity_threshold, vector_similarity);
                for (cluster_id, cluster) in clusters.iter().enumerate() {
                    let medoid_offset = offset + cluster.start_index as usize * vector_size;
                    let medoid_record = read_record(
                        &shard_ref.vector_file_mmap[medoid_offset..],
                        vector_dimensions,
                        vector_type,
                    );

                    let scale_norm = if enable_scale {
                        Some((
                            query_embedding.1,
                            query_embedding.2,
                            query_embedding.3,
                            query_embedding.4,
                            medoid_record.header.scale,
                            medoid_record.header.norm,
                            medoid_record.header.zero_point,
                            medoid_record.header.sum_q,
                        ))
                    } else {
                        None
                    };
                    let similarity = if shard_ref.is_simd {
                        unsafe {
                            similarity_embedding_view_simd(
                                &query_simd,
                                &medoid_record.embedding,
                                scale_norm,
                                vector_similarity,
                                shard_ref.quantization,
                                non_affine,
                            )
                        }
                    } else {
                        similarity_embedding_view(
                            &query_embedding.0,
                            &medoid_record.embedding,
                            scale_norm,
                            vector_similarity,
                            shard_ref.quantization,
                            non_affine,
                        )
                    };

                    top_k_medoid.push(
                        cluster_id,
                        0,
                        0,
                        cluster_id as u32,
                        level_id as u32,
                        similarity,
                        similarity,
                        shard_ref.meta.id,
                    );
                }

                top_k_medoid.items[..top_k_medoid.len]
                    .sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap());

                let num_cluster = top_k_medoid.len;

                top_k_medoid.items[..num_cluster]
                    .iter()
                    .map(|item| {
                        (
                            item.cluster_id,
                            item.level_id,
                            item.cluster_score,
                            clusters[item.doc_id],
                        )
                    })
                    .collect()
            } else {
                clusters
                    .iter()
                    .map(|cluster| (0, level_id as u32, 0.0, *cluster))
                    .collect()
            };

            observed_cluster_count += selected_clusters.len();

            let _zero_hit_count = 0;
            for (cluster_id, _level_id2, cluster_score, cluster) in selected_clusters.iter() {
                let cluster_vectors_count = cluster.child_count as usize;

                let cluster_offset = cluster.start_index as usize * vector_size;

                unsafe {
                    for i in 0..cluster_vectors_count {
                        let record = read_record(
                            &shard_ref.vector_file_mmap
                                [offset + cluster_offset + (i * vector_size)..],
                            vector_dimensions,
                            vector_type,
                        );

                        if field_filter_set.is_empty()
                            || field_filter_set.contains(&(record.header.field_id as u16))
                        {
                            let scale_norm = if enable_scale {
                                Some((
                                    query_embedding.1,
                                    query_embedding.2,
                                    query_embedding.3,
                                    query_embedding.4,
                                    record.header.scale,
                                    record.header.norm,
                                    record.header.zero_point,
                                    record.header.sum_q,
                                ))
                            } else {
                                None
                            };
                            let similarity = if shard_ref.is_simd {
                                similarity_embedding_view_simd(
                                    &query_simd,
                                    &record.embedding,
                                    scale_norm,
                                    vector_similarity,
                                    shard_ref.quantization,
                                    non_affine,
                                )
                            } else {
                                similarity_embedding_view(
                                    &query_embedding.0,
                                    &record.embedding,
                                    scale_norm,
                                    vector_similarity,
                                    shard_ref.quantization,
                                    non_affine,
                                )
                            };

                            let doc_id = (level_id << 16) | (record.header.doc_id as usize);

                            if shard_ref.delete_hashset.is_empty()
                                || !shard_ref.delete_hashset.contains(&doc_id)
                            {
                                top_k.push(
                                    doc_id,
                                    record.header.field_id,
                                    record.header.chunk_id,
                                    *cluster_id,
                                    level_id as u32,
                                    *cluster_score,
                                    similarity,
                                    shard_ref.meta.id,
                                );
                            }
                        }
                    }
                }
            }

            offset += level_vectors_count as usize * vector_size;
        }

        top_k.items[..top_k.len].sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap());

        for item in top_k.items[..top_k.len].iter() {
            let result = min_heap::Result {
                doc_id: item.doc_id,
                score: item.score,
                #[cfg(feature = "vb")]
                field_id: item.field_id,
                #[cfg(feature = "vb")]
                chunk_id: item.chunk_id,
                #[cfg(feature = "vb")]
                level_id: item.level_id,
                #[cfg(feature = "vb")]
                shard_id: shard_ref.meta.id as u32,
                #[cfg(feature = "vb")]
                cluster_id: item.cluster_id,
                #[cfg(feature = "vb")]
                cluster_score: if shard_ref.vector_similarity == VectorSimilarity::Euclidean {
                    -item.cluster_score
                } else {
                    ((item.cluster_score * SIMILARITY_NORMALIZATION_64_I8) + 1.0) * 0.5
                },
                #[cfg(feature = "vb")]
                vector_score: if shard_ref.vector_similarity == VectorSimilarity::Euclidean {
                    -item.score
                } else {
                    ((item.score * SIMILARITY_NORMALIZATION_64_I8) + 1.0) * 0.5
                },
                #[cfg(feature = "vb")]
                lexical_score: 0.0,
                #[cfg(feature = "vb")]
                source: ResultSource::Vector,
            };
            result_object.results.push(result);
        }

        result_object.result_count = top_k.len;
        result_object.result_count_total = top_k.result_count_total;
        result_object.observed_vector_count = top_k.observed_vector_count;
        result_object.observed_cluster_count = observed_cluster_count;

        result_object
    }
}
