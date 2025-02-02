use std::{
    cmp,
    io::{BufRead, BufReader},
};

use ahash::AHashMap;

static DICTIONARY_TXT: &str =
    include_str!("../../assets/dictionaries/frequency_dictionary_zh_cn_349_045.txt");

/// word_segmentation_tm: Fast Word Segmentation with Triangular Matrix
/// Rust port of the original C# implementation: https://github.com/wolfgarbe/WordSegmentationTM
/// Copyright (C) 2024 Wolf Garbe
/// Author: Wolf Garbe wolf.garbe@seekstorm.com
/// URL: //https://github.com/wolfgarbe/word_segmentation_tm
/// Description: https://seekstorm.com/blog/fast-word-segmentation-noisy-text/
/// Find best word segmentation for input string.
/// input_str: The string being word segmented.
/// maximum_dictionary_word_length=max_segmentation_word_length: The maximum word length that should be considered.
/// result: A tuple representing the suggested word segmented text and the sum of logarithmic word occurence probabilities.</returns>
pub struct WordSegmentationTM {
    pub n: f64,
    pub dictionary: AHashMap<Vec<char>, f64>,
    pub maximum_dictionary_word_length: usize,
    pub probability_log_estimation: Vec<f64>,
}

impl WordSegmentationTM {
    /// Create a new instanc of WordSegmentationTM
    pub(crate) fn new() -> Self {
        WordSegmentationTM {
            n: 0.0,
            dictionary: AHashMap::new(),
            maximum_dictionary_word_length: 0usize,
            probability_log_estimation: Vec::new(),
        }
    }

    /// Load multiple dictionary entries from a file of word/frequency count pairs
    /// Merges with any dictionary data already loaded.
    /// corpus: The path+filename of the file.
    /// term_index: The column position of the word.
    /// count_index: The column position of the frequency count.
    /// result: True if file loaded, or false if file not found.
    pub fn load_dictionary(
        &mut self,
        term_index: usize,
        count_index: usize,
        skip_ascii: bool,
    ) -> bool {
        let reader = BufReader::new(DICTIONARY_TXT.as_bytes());

        let mut count_sum = 0;

        for line in reader.lines() {
            let line_string = line.unwrap();

            let line_parts: Vec<&str> = line_string.split_ascii_whitespace().collect();
            if line_parts.len() >= 2 {
                let key = line_parts[term_index];
                if skip_ascii && key.is_ascii() {
                    continue;
                }

                if let Ok(count) = line_parts[count_index].parse::<usize>() {
                    let key_len = key.chars().count();

                    if key_len > self.maximum_dictionary_word_length {
                        self.maximum_dictionary_word_length = key_len;
                    }

                    self.dictionary.insert(key.chars().collect(), count as f64);
                    count_sum += count;
                }
            }
        }

        self.n = (count_sum * 3) as f64;

        for item in self.dictionary.iter_mut() {
            *item.1 = (*item.1 / self.n).log10();
        }

        for i in 0..self.maximum_dictionary_word_length {
            self.probability_log_estimation
                .push((10.0f64 / self.n / (i + 1).pow(10) as f64).log10() * 10.0f64);
        }

        true
    }

    pub fn segment(&self, input: &str, skip_ascii: bool) -> (Vec<String>, f64) {
        let mut result_array: Vec<String> = Vec::new();
        let mut probability_log_sum_best = 0.0;

        if !input.is_empty() {
            if skip_ascii && input.is_ascii() {
                return (vec![input.to_string()], 0.0);
            }

            let input_chars: Vec<char> = input.chars().collect();

            let array_size = cmp::min(self.maximum_dictionary_word_length, input_chars.len());
            let array_width = ((input_chars.len() - 1) >> 6) + 1;
            let array_width_byte = array_width << 3;
            let mut segmented_space_bits = vec![vec![0usize; array_width]; array_size];
            let mut probability_log_sum = vec![0.0; array_size];
            let mut circular_index = 0usize;

            for j in 0..input_chars.len() {
                let space_ulong_index = if j == 0 { 0 } else { (j - 1) >> 6 };
                let array_copy_byte = cmp::min((space_ulong_index + 1) << 3, array_width_byte);

                let array_copy_usize = array_copy_byte >> 3;

                if j > 0 {
                    segmented_space_bits[circular_index][space_ulong_index] |=
                        1usize << ((j - 1) & 0x3f);
                }

                let imax = cmp::min(input_chars.len() - j, self.maximum_dictionary_word_length);

                for i in 1..=imax {
                    let destination_index = (i + circular_index) % array_size;

                    let part1_chars = &input_chars[j..(j + i)];

                    let probability_log_part1 =
                        if let Some(probability_log) = self.dictionary.get(part1_chars) {
                            *probability_log
                        } else {
                            self.probability_log_estimation[part1_chars.len() - 1]
                        };

                    if j == 0 {
                        probability_log_sum[destination_index] = probability_log_part1;
                    } else if (i == self.maximum_dictionary_word_length)
                        || (probability_log_sum[destination_index]
                            < probability_log_sum[circular_index] + probability_log_part1)
                    {
                        for i in 0..array_copy_usize {
                            segmented_space_bits[destination_index][i] =
                                segmented_space_bits[circular_index][i];
                        }

                        probability_log_sum[destination_index] =
                            probability_log_sum[circular_index] + probability_log_part1;
                    }
                }

                circular_index += 1;
                if circular_index == array_size {
                    circular_index = 0;
                }
            }

            let mut last = 0;
            for i in 0..(input_chars.len() - 1) {
                if (segmented_space_bits[circular_index][i >> 6] & (1usize << (i & 0x3f))) > 0 {
                    if !result_array.is_empty() && ['+' , '-'].contains(&input_chars[last])  {result_array.push(input_chars[last..(i + 1)].iter().skip(1).collect());} else {result_array.push(input_chars[last..(i + 1)].iter().collect());}
                    last = i + 1;
                }
            }
            if !result_array.is_empty() && ['+' , '-'].contains(&input_chars[last]) {result_array.push(input_chars[last..].iter().skip(1).collect());} else {result_array.push(input_chars[last..].iter().collect());}

            probability_log_sum_best += probability_log_sum[circular_index];
        }

        (result_array, probability_log_sum_best)
    }
}
