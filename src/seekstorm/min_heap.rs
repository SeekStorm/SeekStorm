use ahash::AHashMap;
use derivative::Derivative;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Copy, Default, Deserialize, Serialize, Derivative)]
pub struct Result {
    pub doc_id: usize,
    pub score: f32,
}

/// MinHeap implements an min-heap, which is a binary heap used as priority queue.
/// Maintains a list of the top-k most relevant result candidates.
/// Better performance than a ordered list with binary search, inserts, and deletes
pub(crate) struct MinHeap {
    pub _elements: Vec<Result>,
    pub current_heap_size: usize,
    pub docid_hashset: AHashMap<usize, f32>,
}

impl MinHeap {
    #[inline(always)]
    pub(crate) fn new(size: usize) -> MinHeap {
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
        }
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
    fn add(&mut self, score: f32, doc_id: usize) {
        self._elements[self.current_heap_size].score = score;
        self._elements[self.current_heap_size].doc_id = doc_id;
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
            && self._elements[index].score < Self::get_parent(self, index).score
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
                && self.get_right_child(index).score < self.get_left_child(index).score
            {
                smaller_index = Self::get_right_child_index(index);
            }

            if self._elements[smaller_index].score >= self._elements[index].score {
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
                && self.get_right_child(index).score < self.get_left_child(index).score
            {
                smaller_index = Self::get_right_child_index(index);
            }

            if self._elements[smaller_index].score >= self._elements[index].score {
                break;
            }

            self.swap(smaller_index, index);
            index = smaller_index;
        }
    }

    #[inline(always)]
    pub(crate) fn add_topk(&mut self, score: f32, doc_id: usize, top_k: usize) -> bool {
        if self.current_heap_size > top_k && self._elements[0].score >= score {
            return false;
        }

        if self.docid_hashset.len() > 0 && self.docid_hashset.contains_key(&doc_id) {
            if self._elements[0].doc_id == doc_id {
                if score > self._elements[0].score {
                    self._elements[0].score = score;
                    self.heapify_down();
                    return true;
                } else {
                    return false;
                }
            }
            // != position0: find the item, replace the score, swap it with the lowest item, heapify
            else {
                if self.docid_hashset[&doc_id] >= score {
                    return false;
                }

                let mut index = 0;
                while doc_id != self._elements[index].doc_id {
                    if index == self.current_heap_size - 1 {
                        self.pop_add(score, doc_id);
                        return true;
                    }
                    index += 1;
                }

                self._elements[index].score = score;
                self.heapify_down_index(index);
                return true;
            }
        }

        if self.current_heap_size < top_k {
            self.add(score, doc_id);
            true
        } else if score > self._elements[0].score {
            self.pop_add(score, doc_id);
            true
        } else {
            false
        }
    }
}
