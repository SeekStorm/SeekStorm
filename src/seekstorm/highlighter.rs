use crate::index::Document;
use crate::min_heap::MinHeap;
use aho_corasick::AhoCorasick;
use serde::{Deserialize, Serialize};

/// Specifies the number and size of fragments (snippets, summaries) to generate from each specified field to provide a "keyword in context" (KWIC) functionality.
/// With highlight_markup the matching query terms within the fragments can be highlighted with HTML markup.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Highlight {
    pub field: String,
    #[serde(default)]
    pub fragment_number: usize,
    #[serde(default)]
    pub fragment_size: usize,
    #[serde(default)]
    pub highlight_markup: bool,
}

impl Default for Highlight {
    fn default() -> Self {
        Highlight {
            field: String::new(),
            fragment_number: 1,
            fragment_size: usize::MAX,
            highlight_markup: true,
        }
    }
}

pub(crate) fn add_fragment<'a>(
    no_score_no_highlight: bool,
    mut fragment: Fragment<'a>,
    query_terms_ac: &AhoCorasick,
    fragments: &mut Vec<Fragment<'a>>,
    topk_candidates: &mut MinHeap,
    fragment_number: usize,
    fragment_size: usize,
) {
    let mut score = 0.0;
    let mut expected_pattern = usize::MAX;
    let mut expected_index = usize::MAX;

    let mut first_end = 0;
    let mut set = vec![0; query_terms_ac.patterns_len()];
    let mut sequence_length = 1;

    if no_score_no_highlight {
        score = 1.0;
    } else {
        for mat in query_terms_ac.find_iter(fragment.text) {
            if first_end == 0 {
                first_end = mat.end();
            }

            let id = mat.pattern().as_usize();
            score += if id == expected_pattern && expected_index == mat.start() {
                sequence_length += 1;
                set[id] = 1;
                sequence_length as f32 * 5.0
            } else if set[id] == 0 {
                sequence_length = 1;
                set[id] = 1;
                1.0
            } else {
                sequence_length = 1;
                0.3
            };

            expected_pattern = id + 1;
            expected_index = mat.end() + 1;
        }
    }

    if first_end > fragment_size {
        let mut idx = fragment.text.len() - fragment_size;

        while !fragment.text.is_char_boundary(idx) {
            idx -= 1;
        }

        match fragment.text[idx..].find(' ') {
            None => idx = 0,
            Some(value) => idx += value,
        }

        let adjusted_fragment = &fragment.text[idx..];
        fragment.text = adjusted_fragment;
        fragment.trim_left = true;
    } else if fragment.text.len() > fragment_size {
        let mut idx = fragment_size;

        while !fragment.text.is_char_boundary(idx) {
            idx -= 1;
        }

        match fragment.text[idx..].find(' ') {
            None => idx = fragment.text.len(),
            Some(value) => idx += value,
        }

        let adjusted_fragment = &fragment.text[..idx];
        fragment.text = adjusted_fragment;
        fragment.trim_right = true;
    }

    let section_index = fragments.len();

    let mut added = false;
    if score > 0.0 {
        added = topk_candidates.add_topk(score, section_index, fragment_number);
    }
    if fragments.is_empty() || added {
        fragments.push(fragment);
    }
}

const SENTENCE_BOUNDARY_CHARS: [char; 5] = ['!', '?', '.', '¿', '¡'];

pub(crate) struct Fragment<'a> {
    text: &'a str,
    trim_left: bool,
    trim_right: bool,
}
/// Extracts the most relevant fragments (snippets, summaries) from specified fields of the document to provide a "keyword in context" (KWIC) functionality.
/// I.e. the sentences containing the matches of the query terms within the field is displayed and the query term matches are optionally highlighted (e.g. bold) by injecting HTML tags in to the text.
/// Instead of showing the complete text only the relevant fragments containing keyword matches are extracted. The user is provided with concise visual feedback for relevancy of the document regarding to the query.
/// The fragment ranking score takes into account the number of matching terms, their order and proximity (phrase).
/// The score is used for the selection of top-k most relevant fragments, but the order of selected fragments is preserved how they originally appear in the field.
/// The field is fragmented into sentences, using punctuation marks '.?!' as sentence boundaries.
/// If the fragment length exceeds the specified fragment_size, then the fragment is truncated at the right or left side, so that the query term higlight positions are kept within the remaining fragment window.
/// Selecting the right fragment and the right fragment window is fundamental for the users perceived relevancy of the search results.
pub fn top_fragments_from_field(
    document: &Document,
    query_terms_ac: &AhoCorasick,
    field_name: &str,
    fragment_number: usize,
    highlight_markup: bool,
    fragment_size: usize,
) -> Result<String, String> {
    match document.get(field_name) {
        None => Ok("".to_string()),
        Some(value) => {
            let no_score_no_highlight =
                query_terms_ac.patterns_len() == 1 && query_terms_ac.max_pattern_len() == 1;
            let no_fragmentation = fragment_number == 0;
            let fragment_number = if no_fragmentation { 1 } else { fragment_number };
            let mut topk_candidates = MinHeap::new(fragment_number);

            let text: String = serde_json::from_str(&value.to_string()).unwrap();

            let mut fragments: Vec<Fragment> = Vec::new();

            let mut last = 0;
            if !no_fragmentation {
                for (character_index, matched) in text.match_indices(&SENTENCE_BOUNDARY_CHARS[..]) {
                    if last != character_index {
                        let section = Fragment {
                            text: &text[last..character_index + matched.len()],
                            trim_left: false,
                            trim_right: false,
                        };

                        add_fragment(
                            no_score_no_highlight,
                            section,
                            query_terms_ac,
                            &mut fragments,
                            &mut topk_candidates,
                            fragment_number,
                            fragment_size,
                        );

                        if no_score_no_highlight
                            && topk_candidates.current_heap_size == fragment_number
                        {
                            break;
                        }
                    }
                    last = character_index + matched.len();
                }
            }

            if last < text.len() - 1 {
                let section = Fragment {
                    text: &text[last..],
                    trim_left: false,
                    trim_right: false,
                };

                add_fragment(
                    no_score_no_highlight,
                    section,
                    query_terms_ac,
                    &mut fragments,
                    &mut topk_candidates,
                    fragment_number,
                    fragment_size,
                );
            }

            let mut combined_string = String::with_capacity(text.len());

            if !fragments.is_empty() {
                if topk_candidates.current_heap_size > 0 {
                    if topk_candidates.current_heap_size < fragment_number {
                        topk_candidates
                            ._elements
                            .truncate(topk_candidates.current_heap_size);
                    }

                    topk_candidates
                        ._elements
                        .sort_by(|a, b| a.doc_id.partial_cmp(&b.doc_id).unwrap());

                    let mut previous_docid = 0;
                    for candidate in topk_candidates._elements {
                        if (!combined_string.is_empty()
                            && !combined_string.ends_with("...")
                            && candidate.doc_id != previous_docid + 1)
                            || (fragments[candidate.doc_id].trim_left
                                && (combined_string.is_empty()
                                    || !combined_string.ends_with("...")))
                        {
                            combined_string.push_str("...")
                        };
                        combined_string.push_str(fragments[candidate.doc_id].text);
                        previous_docid = candidate.doc_id;

                        if fragments[candidate.doc_id].trim_right {
                            combined_string.push_str("...")
                        };
                    }
                } else {
                    combined_string.push_str(fragments[0].text);
                }
            }

            if highlight_markup && !no_score_no_highlight {
                highlight_terms(&mut combined_string, query_terms_ac);
            }

            Ok(combined_string)
        }
    }
}

pub(crate) fn highlight_terms(text: &mut String, query_terms_ac: &AhoCorasick) {
    let mut result = String::new();
    let mut prev_end = 0;

    for mat in query_terms_ac.find_iter(&text) {
        result.push_str(&text[prev_end..mat.start()]);
        result.push_str("<b>");
        result.push_str(&text[mat.start()..mat.end()]);
        result.push_str("</b>");
        prev_end = mat.end();
    }

    if prev_end < text.len() {
        result.push_str(&text[prev_end..text.len()]);
    }

    *text = result;
}
