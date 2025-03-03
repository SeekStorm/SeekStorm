use ahash::AHashMap;
use serde::{Deserialize, Serialize};

use crate::{
    geo_search::morton_ordering,
    index::{FieldType, Index},
    search::{FacetValue, ResultSortIndex, SortOrder},
    utils::{
        read_f32, read_f64, read_i8, read_i16, read_i32, read_i64, read_u16, read_u32, read_u64,
    },
};

#[derive(Clone, Debug, Copy, Default, Deserialize, Serialize)]
pub struct Result {
    pub doc_id: usize,
    pub score: f32,
}

/// MinHeap implements an min-heap, which is a binary heap used as priority queue.
/// Maintains a list of the top-k most relevant result candidates.
/// Better performance than a ordered list with binary search, inserts, and deletes
pub(crate) struct MinHeap<'a> {
    pub _elements: Vec<Result>,
    pub current_heap_size: usize,
    pub docid_hashset: AHashMap<usize, f32>,

    pub index: &'a Index,
    pub result_sort: &'a Vec<ResultSortIndex<'a>>,
}

impl<'a> MinHeap<'a> {
    #[inline(always)]
    pub(crate) fn new(
        size: usize,
        index: &'a Index,
        result_sort: &'a Vec<ResultSortIndex>,
    ) -> MinHeap<'a> {
        MinHeap {
            current_heap_size: 0,
            docid_hashset: AHashMap::new(),
            _elements: vec![
                Result {
                    doc_id: 0,
                    score: 0.0,
                };
                size
            ],
            index,
            result_sort,
        }
    }

    #[inline]
    pub fn result_ordering(&self, result1: Result, result2: Result) -> core::cmp::Ordering {
        for field in self.result_sort.iter() {
            match self.index.facets[field.idx].field_type {
                FieldType::U8 => {
                    let offset = self.index.facets[field.idx].offset;

                    let facet_value_1 = &self.index.facets_file_mmap
                        [(self.index.facets_size_sum * result1.doc_id) + offset];

                    let facet_value_2 = &self.index.facets_file_mmap
                        [(self.index.facets_size_sum * result2.doc_id) + offset];

                    let order = if field.order == SortOrder::Descending {
                        facet_value_1.cmp(facet_value_2)
                    } else {
                        facet_value_2.cmp(facet_value_1)
                    };

                    if order != core::cmp::Ordering::Equal {
                        return order;
                    };
                }

                FieldType::U16 => {
                    let offset = self.index.facets[field.idx].offset;

                    let facet_value_1 = read_u16(
                        &self.index.facets_file_mmap,
                        (self.index.facets_size_sum * result1.doc_id) + offset,
                    );
                    let facet_value_2 = read_u16(
                        &self.index.facets_file_mmap,
                        (self.index.facets_size_sum * result2.doc_id) + offset,
                    );

                    let order = if field.order == SortOrder::Descending {
                        facet_value_1.cmp(&facet_value_2)
                    } else {
                        facet_value_2.cmp(&facet_value_1)
                    };

                    if order != core::cmp::Ordering::Equal {
                        return order;
                    };
                }
                FieldType::U32 => {
                    let offset = self.index.facets[field.idx].offset;

                    let facet_value_1 = read_u32(
                        &self.index.facets_file_mmap,
                        (self.index.facets_size_sum * result1.doc_id) + offset,
                    );
                    let facet_value_2 = read_u32(
                        &self.index.facets_file_mmap,
                        (self.index.facets_size_sum * result2.doc_id) + offset,
                    );

                    let order = if field.order == SortOrder::Descending {
                        facet_value_1.cmp(&facet_value_2)
                    } else {
                        facet_value_2.cmp(&facet_value_1)
                    };

                    if order != core::cmp::Ordering::Equal {
                        return order;
                    };
                }
                FieldType::U64 => {
                    let offset = self.index.facets[field.idx].offset;

                    let facet_value_1 = read_u64(
                        &self.index.facets_file_mmap,
                        (self.index.facets_size_sum * result1.doc_id) + offset,
                    );
                    let facet_value_2 = read_u64(
                        &self.index.facets_file_mmap,
                        (self.index.facets_size_sum * result2.doc_id) + offset,
                    );

                    let order = if field.order == SortOrder::Descending {
                        facet_value_1.cmp(&facet_value_2)
                    } else {
                        facet_value_2.cmp(&facet_value_1)
                    };

                    if order != core::cmp::Ordering::Equal {
                        return order;
                    };
                }

                FieldType::I8 => {
                    let offset = self.index.facets[field.idx].offset;

                    let facet_value_1 = read_i8(
                        &self.index.facets_file_mmap,
                        (self.index.facets_size_sum * result1.doc_id) + offset,
                    );
                    let facet_value_2 = read_i8(
                        &self.index.facets_file_mmap,
                        (self.index.facets_size_sum * result2.doc_id) + offset,
                    );

                    let order = if field.order == SortOrder::Descending {
                        facet_value_1.cmp(&facet_value_2)
                    } else {
                        facet_value_2.cmp(&facet_value_1)
                    };

                    if order != core::cmp::Ordering::Equal {
                        return order;
                    };
                }

                FieldType::I16 => {
                    let offset = self.index.facets[field.idx].offset;

                    let facet_value_1 = read_i16(
                        &self.index.facets_file_mmap,
                        (self.index.facets_size_sum * result1.doc_id) + offset,
                    );
                    let facet_value_2 = read_i16(
                        &self.index.facets_file_mmap,
                        (self.index.facets_size_sum * result2.doc_id) + offset,
                    );

                    let order = if field.order == SortOrder::Descending {
                        facet_value_1.cmp(&facet_value_2)
                    } else {
                        facet_value_2.cmp(&facet_value_1)
                    };

                    if order != core::cmp::Ordering::Equal {
                        return order;
                    };
                }
                FieldType::I32 => {
                    let offset = self.index.facets[field.idx].offset;

                    let facet_value_1 = read_i32(
                        &self.index.facets_file_mmap,
                        (self.index.facets_size_sum * result1.doc_id) + offset,
                    );
                    let facet_value_2 = read_i32(
                        &self.index.facets_file_mmap,
                        (self.index.facets_size_sum * result2.doc_id) + offset,
                    );

                    let order = if field.order == SortOrder::Descending {
                        facet_value_1.cmp(&facet_value_2)
                    } else {
                        facet_value_2.cmp(&facet_value_1)
                    };

                    if order != core::cmp::Ordering::Equal {
                        return order;
                    };
                }
                FieldType::I64 => {
                    let offset = self.index.facets[field.idx].offset;

                    let facet_value_1 = read_i64(
                        &self.index.facets_file_mmap,
                        (self.index.facets_size_sum * result1.doc_id) + offset,
                    );
                    let facet_value_2 = read_i64(
                        &self.index.facets_file_mmap,
                        (self.index.facets_size_sum * result2.doc_id) + offset,
                    );

                    let order = if field.order == SortOrder::Descending {
                        facet_value_1.cmp(&facet_value_2)
                    } else {
                        facet_value_2.cmp(&facet_value_1)
                    };

                    if order != core::cmp::Ordering::Equal {
                        return order;
                    };
                }

                FieldType::Timestamp => {
                    let offset = self.index.facets[field.idx].offset;

                    let facet_value_1 = read_i64(
                        &self.index.facets_file_mmap,
                        (self.index.facets_size_sum * result1.doc_id) + offset,
                    );
                    let facet_value_2 = read_i64(
                        &self.index.facets_file_mmap,
                        (self.index.facets_size_sum * result2.doc_id) + offset,
                    );

                    let order = if field.order == SortOrder::Descending {
                        facet_value_1.cmp(&facet_value_2)
                    } else {
                        facet_value_2.cmp(&facet_value_1)
                    };

                    if order != core::cmp::Ordering::Equal {
                        return order;
                    };
                }

                FieldType::F32 => {
                    let offset = self.index.facets[field.idx].offset;

                    let facet_value_1 = read_f32(
                        &self.index.facets_file_mmap,
                        (self.index.facets_size_sum * result1.doc_id) + offset,
                    );
                    let facet_value_2 = read_f32(
                        &self.index.facets_file_mmap,
                        (self.index.facets_size_sum * result2.doc_id) + offset,
                    );

                    let order = if field.order == SortOrder::Descending {
                        facet_value_1
                            .partial_cmp(&facet_value_2)
                            .unwrap_or(core::cmp::Ordering::Equal)
                    } else {
                        facet_value_2
                            .partial_cmp(&facet_value_1)
                            .unwrap_or(core::cmp::Ordering::Equal)
                    };

                    if order != core::cmp::Ordering::Equal {
                        return order;
                    };
                }

                FieldType::F64 => {
                    let offset = self.index.facets[field.idx].offset;

                    let facet_value_1 = read_f64(
                        &self.index.facets_file_mmap,
                        (self.index.facets_size_sum * result1.doc_id) + offset,
                    );
                    let facet_value_2 = read_f64(
                        &self.index.facets_file_mmap,
                        (self.index.facets_size_sum * result2.doc_id) + offset,
                    );

                    let order = if field.order == SortOrder::Descending {
                        facet_value_1
                            .partial_cmp(&facet_value_2)
                            .unwrap_or(core::cmp::Ordering::Equal)
                    } else {
                        facet_value_2
                            .partial_cmp(&facet_value_1)
                            .unwrap_or(core::cmp::Ordering::Equal)
                    };

                    if order != core::cmp::Ordering::Equal {
                        return order;
                    };
                }

                FieldType::String => {
                    let offset = self.index.facets[field.idx].offset;

                    let facet_id_1 = read_u16(
                        &self.index.facets_file_mmap,
                        (self.index.facets_size_sum * result1.doc_id) + offset,
                    );
                    let facet_id_2 = read_u16(
                        &self.index.facets_file_mmap,
                        (self.index.facets_size_sum * result2.doc_id) + offset,
                    );

                    let facet_value_1 = self.index.facets[field.idx]
                        .values
                        .get_index((facet_id_1).into())
                        .unwrap()
                        .1
                        .0[0]
                        .clone();

                    let facet_value_2 = self.index.facets[field.idx]
                        .values
                        .get_index((facet_id_2).into())
                        .unwrap()
                        .1
                        .0[0]
                        .clone();

                    let order = if field.order == SortOrder::Descending {
                        facet_value_1.cmp(&facet_value_2)
                    } else {
                        facet_value_2.cmp(&facet_value_1)
                    };

                    if order != core::cmp::Ordering::Equal {
                        return order;
                    };
                }

                FieldType::StringSet => {
                    let offset = self.index.facets[field.idx].offset;

                    let facet_id_1 = read_u16(
                        &self.index.facets_file_mmap,
                        (self.index.facets_size_sum * result1.doc_id) + offset,
                    );
                    let facet_id_2 = read_u16(
                        &self.index.facets_file_mmap,
                        (self.index.facets_size_sum * result2.doc_id) + offset,
                    );

                    let facet_value_1 = self.index.facets[field.idx]
                        .values
                        .get_index((facet_id_1).into())
                        .unwrap()
                        .1
                        .0[0]
                        .clone();

                    let facet_value_2 = self.index.facets[field.idx]
                        .values
                        .get_index((facet_id_2).into())
                        .unwrap()
                        .1
                        .0[0]
                        .clone();

                    let order = if field.order == SortOrder::Descending {
                        facet_value_1.cmp(&facet_value_2)
                    } else {
                        facet_value_2.cmp(&facet_value_1)
                    };

                    if order != core::cmp::Ordering::Equal {
                        return order;
                    };
                }

                FieldType::Point => {
                    if let FacetValue::Point(base) = &field.base {
                        let offset = self.index.facets[field.idx].offset;

                        let facet_value_1 = read_u64(
                            &self.index.facets_file_mmap,
                            (self.index.facets_size_sum * result1.doc_id) + offset,
                        );
                        let facet_value_2 = read_u64(
                            &self.index.facets_file_mmap,
                            (self.index.facets_size_sum * result2.doc_id) + offset,
                        );

                        let order =
                            morton_ordering(facet_value_1, facet_value_2, base, &field.order);

                        if order != core::cmp::Ordering::Equal {
                            return order;
                        };
                    }
                }

                _ => {}
            }
        }

        result1
            .score
            .partial_cmp(&result2.score)
            .unwrap_or(core::cmp::Ordering::Equal)
    }

    #[inline(always)]
    fn get_left_child_index(element_index: usize) -> usize {
        2 * element_index + 1
    }

    #[inline(always)]
    fn get_right_child_index(element_index: usize) -> usize {
        2 * element_index + 2
    }

    #[inline(always)]
    fn get_parent_index(element_index: usize) -> usize {
        (element_index - 1) / 2
    }

    #[inline(always)]
    fn has_left_child(&self, element_index: usize) -> bool {
        Self::get_left_child_index(element_index) < self.current_heap_size
    }

    #[inline(always)]
    fn has_right_child(&self, element_index: usize) -> bool {
        Self::get_right_child_index(element_index) < self.current_heap_size
    }

    #[inline(always)]
    fn is_root(element_index: usize) -> bool {
        element_index == 0
    }

    #[inline(always)]
    fn get_left_child(&self, element_index: usize) -> &Result {
        &self._elements[Self::get_left_child_index(element_index)]
    }

    #[inline(always)]
    fn get_right_child(&self, element_index: usize) -> &Result {
        &self._elements[Self::get_right_child_index(element_index)]
    }

    #[inline(always)]
    fn get_parent(&self, element_index: usize) -> &Result {
        &self._elements[Self::get_parent_index(element_index)]
    }

    #[inline(always)]
    fn swap(&mut self, first_index: usize, second_index: usize) {
        self._elements.swap(first_index, second_index);
    }

    #[inline(always)]
    fn add(&mut self, result: &Result) {
        self._elements[self.current_heap_size].score = result.score;
        self._elements[self.current_heap_size].doc_id = result.doc_id;
        self.current_heap_size += 1;

        self.heapify_up();
    }

    #[inline(always)]
    fn pop_add(&mut self, score: f32, doc_id: usize) {
        if !self.docid_hashset.is_empty() {
            self.docid_hashset.remove(&self._elements[0].doc_id);
        }

        self._elements[0].score = score;
        self._elements[0].doc_id = doc_id;
        self.heapify_down();
    }

    #[inline(always)]
    fn heapify_up(&mut self) {
        let mut index = self.current_heap_size - 1;
        while !Self::is_root(index)
            && self
                .result_ordering(self._elements[index], *Self::get_parent(self, index))
                .is_lt()
        {
            let parent_index = Self::get_parent_index(index);
            self.swap(parent_index, index);
            index = parent_index;
        }
    }

    #[inline(always)]
    fn heapify_down(&mut self) {
        let mut index: usize = 0;
        while self.has_left_child(index) {
            let mut smaller_index = Self::get_left_child_index(index);
            if self.has_right_child(index)
                && self
                    .result_ordering(*self.get_right_child(index), *self.get_left_child(index))
                    .is_lt()
            {
                smaller_index = Self::get_right_child_index(index);
            }
            if self
                .result_ordering(self._elements[smaller_index], self._elements[index])
                .is_ge()
            {
                break;
            }

            self.swap(smaller_index, index);
            index = smaller_index;
        }
    }

    #[inline(always)]
    fn heapify_down_index(&mut self, index: usize) {
        let mut index: usize = index;
        while self.has_left_child(index) {
            let mut smaller_index = Self::get_left_child_index(index);
            if self.has_right_child(index)
                && self
                    .result_ordering(*self.get_right_child(index), *self.get_left_child(index))
                    .is_lt()
            {
                smaller_index = Self::get_right_child_index(index);
            }

            if self
                .result_ordering(self._elements[smaller_index], self._elements[index])
                .is_ge()
            {
                break;
            }

            self.swap(smaller_index, index);
            index = smaller_index;
        }
    }

    #[inline(always)]
    pub(crate) fn add_topk(&mut self, result: Result, top_k: usize) -> bool {
        if self.current_heap_size > top_k && self.result_ordering(self._elements[0], result).is_ge()
        {
            return false;
        }

        if self.docid_hashset.len() > 0 && self.docid_hashset.contains_key(&result.doc_id) {
            if self._elements[0].doc_id == result.doc_id {
                if self.result_ordering(result, self._elements[0]).is_gt() {
                    self._elements[0].score = result.score;
                    self.heapify_down();
                    return true;
                } else {
                    return false;
                }
            } else {
                if self
                    .result_ordering(
                        Result {
                            doc_id: result.doc_id,
                            score: self.docid_hashset[&result.doc_id],
                        },
                        result,
                    )
                    .is_ge()
                {
                    return false;
                }

                let mut index = 0;
                while result.doc_id != self._elements[index].doc_id {
                    if index == self.current_heap_size - 1 {
                        self.pop_add(result.score, result.doc_id);
                        return true;
                    }
                    index += 1;
                }

                self._elements[index].score = result.score;
                self.heapify_down_index(index);
                return true;
            }
        }

        if self.current_heap_size < top_k {
            self.add(&result);
            true
        } else if self.result_ordering(result, self._elements[0]).is_gt() {
            self.pop_add(result.score, result.doc_id);
            true
        } else {
            false
        }
    }
}
