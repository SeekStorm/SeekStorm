use crate::index::{Document, FieldType, Index, IndexArc, hash64};
use crate::min_heap::{self, MinHeap};
use aho_corasick::{AhoCorasick, MatchKind};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Specifies the number and size of fragments (snippets, summaries) to generate from each specified field to provide a "keyword in context" (KWIC) functionality.
/// With highlight_markup the matching query terms within the fragments can be highlighted with HTML markup.
#[derive(Debug, Clone, Deserialize, Serialize, ToSchema)]
pub struct Highlight {
    /// Specifies the field from which the fragments  (snippets, summaries) are created.
    pub field: String,
    /// Allows to specifiy multiple highlight result fields from the same source field, leaving the original field intact,
    /// Default: if name is empty then field is used instead, i.e the original field is overwritten with the highlight.
    #[serde(default)]
    #[serde(skip_serializing_if = "String::is_empty")]
    pub name: String,
    /// If 0/default then return the full original text without fragmenting.
    #[serde(default)]
    pub fragment_number: usize,
    /// Specifies the length of a highlight fragment.
    /// The default 0 returns the full original text without truncating, but still with highlighting if highlight_markup is enabled.
    #[serde(default)]
    pub fragment_size: usize,
    /// if true, the matching query terms within the fragments are highlighted with HTML markup **\<b\>term\<\/b\>**.
    #[serde(default)]
    pub highlight_markup: bool,
    /// Specifies the markup tags to insert **before** each highlighted term (e.g. \"\<b\>\" or \"\<em\>\"). This can be any string, but is most often an HTML or XML tag.
    /// Only used when **highlight_markup** is set to true.
    #[serde(default = "default_pre_tag")]
    pub pre_tags: String,
    /// Specifies the markup tags to insert **after** each highlighted term. (e.g. \"\<\/b\>\" or \"\<\/em\>\"). This can be any string, but is most often an HTML or XML tag.
    /// Only used when **highlight_markup** is set to true.
    #[serde(default = "default_post_tag")]
    pub post_tags: String,
}

impl Default for Highlight {
    fn default() -> Self {
        Highlight {
            field: String::new(),
            name: String::new(),
            fragment_number: 1,
            fragment_size: usize::MAX,
            highlight_markup: true,
            pre_tags: default_pre_tag(),
            post_tags: default_post_tag(),
        }
    }
}

fn default_pre_tag() -> String {
    "<b>".into()
}

fn default_post_tag() -> String {
    "</b>".into()
}

/// Highlighter object used as get_document parameter for extracting keyword-in-context (KWIC) fragments from fields in documents, and highlighting the query terms within.
#[derive(Debug)]
pub struct Highlighter {
    pub(crate) highlights: Vec<Highlight>,
    pub(crate) query_terms_ac: AhoCorasick,
}

/// Returns the Highlighter object used as get_document parameter for highlighting fields in documents
pub async fn highlighter(
    index_arc: &IndexArc,
    highlights: Vec<Highlight>,
    query_terms_vec: Vec<String>,
) -> Highlighter {
    let index_ref = index_arc.read().await;
    let query_terms = if !index_ref.synonyms_map.is_empty() {
        let mut query_terms_vec_mut = query_terms_vec.clone();
        for query_term in query_terms_vec.iter() {
            let term_hash = hash64(query_term.to_lowercase().as_bytes());
            if let Some(synonyms) = index_ref.synonyms_map.get(&term_hash) {
                for synonym in synonyms.iter() {
                    query_terms_vec_mut.push(synonym.0.clone());
                }
            }
        }
        query_terms_vec_mut
    } else {
        query_terms_vec
    };

    let query_terms_ac = AhoCorasick::builder()
        .ascii_case_insensitive(true)
        .match_kind(MatchKind::LeftmostLongest)
        .build(query_terms)
        .unwrap();

    Highlighter {
        highlights,
        query_terms_ac,
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
        added = topk_candidates.add_topk(
            min_heap::Result {
                doc_id: section_index,
                score,
            },
            fragment_number,
        );
    }
    if fragments.is_empty() || added {
        fragments.push(fragment);
    }
}

const SENTENCE_BOUNDARY_CHARS: [char; 11] =
    ['!', '?', '.', '¿', '¡', '。', '、', '！', '？', '︒', '。'];

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
pub(crate) fn top_fragments_from_field(
    index: &Index,
    document: &Document,
    query_terms_ac: &AhoCorasick,
    highlight: &Highlight,
) -> Result<String, String> {
    match document.get(&highlight.field) {
        None => Ok("".to_string()),
        Some(value) => {
            let no_score_no_highlight =
                query_terms_ac.patterns_len() == 1 && query_terms_ac.max_pattern_len() == 1;
            let no_fragmentation = highlight.fragment_number == 0;
            let fragment_number = if no_fragmentation {
                1
            } else {
                highlight.fragment_number
            };
            let result_sort = Vec::new();
            let mut topk_candidates = MinHeap::new(fragment_number, index, &result_sort);

            if let Some(schema_field) = index.schema_map.get(&highlight.field) {
                let text = match schema_field.field_type {
                    FieldType::Text | FieldType::String16 | FieldType::String32 => {
                        serde_json::from_value::<String>(value.clone()).unwrap_or(value.to_string())
                    }
                    _ => value.to_string(),
                };

                let mut fragments: Vec<Fragment> = Vec::new();

                let mut last = 0;
                if !no_fragmentation {
                    for (character_index, matched) in
                        text.match_indices(&SENTENCE_BOUNDARY_CHARS[..])
                    {
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
                                highlight.fragment_size,
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
                        highlight.fragment_size,
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

                if highlight.highlight_markup && !no_score_no_highlight {
                    highlight_terms(
                        &mut combined_string,
                        query_terms_ac,
                        &highlight.pre_tags,
                        &highlight.post_tags,
                    );
                }

                Ok(combined_string)
            } else {
                Ok("".to_string())
            }
        }
    }
}

pub(crate) fn highlight_terms(
    text: &mut String,
    query_terms_ac: &AhoCorasick,
    pre_tags: &str,
    post_tags: &str,
) {
    let mut result = String::new();
    let mut prev_end = 0;

    for mat in query_terms_ac.find_iter(&text) {
        result.push_str(&text[prev_end..mat.start()]);
        result.push_str(pre_tags);
        result.push_str(&text[mat.start()..mat.end()]);
        result.push_str(post_tags);
        prev_end = mat.end();
    }

    if prev_end < text.len() {
        result.push_str(&text[prev_end..text.len()]);
    }

    *text = result;
}
