use std::cmp;

use ahash::AHashMap;
use finl_unicode::categories::{CharacterCategories, MinorCategory};

use crate::{
    index::{
        MAX_QUERY_TERM_NUMBER, NgramSet, NgramType, NonUniqueTermObject, Shard, TermObject,
        TokenizerType, hash32, hash64,
    },
    search::QueryType,
};

const APOSTROPH: [char; 2] = ['\u{2019}', '\u{0027}'];
const ZALGO_CHAR_CATEGORIES: [MinorCategory; 2] = [MinorCategory::Mn, MinorCategory::Me];

/// fold_diacritics_accents_zalgo_umlaut() (used by TokenizerType::UnicodeAlphanumericFolded):
/// Converts text with diacritics, accents, zalgo text, umlaut, bold, italic, full-width UTF-8 characters into its basic representation.
/// Unicode UTF-8 has made life so much easier compared to the old code pages, but its endless possibilities also pose challenges in parsing and indexing.
/// The challenge is that the same basic letter might be represented by different UTF8 characters if they contain diacritics, accents, or are bold, italic, or full-width.
/// Sometimes, users can't search because the keyboard doesn't have these letters or they don't know how to enter, or they even don't know what that letter looks like.
/// Sometimes the document to be ingested is already written without diacritics for the same reasons.
/// We don't want to search for every variant separately, most often we even don't know that they exist in the index.
/// We want to have all results, for every variant, no matter which variant is entered in the query,
/// e.g. for indexing LinkedIn posts that make use of external bold/italic formatters or for indexing documents in accented languages.
/// It is important that the search engine supports the character folding rather than external preprocessing, as we want to have both: enter the query in any character form, receive all results independent from their character form, but have them returned in their original, unaltered characters.
pub fn fold_diacritics_accents_ligatures_zalgo_umlaut(string: &str) -> String {
    string
        .to_lowercase()
        .chars()
        .fold(String::with_capacity(string.len()), |mut folded, cc| {
            let mut base_char = None;
            let mut base_char2 = None;

            match cc {
                'ﬀ' => folded.push_str("ff"),
                'ﬃ' => folded.push_str("ffi"),
                'ﬄ' => folded.push_str("ffl"),
                'ﬁ' => folded.push_str("fi"),
                'ﬂ' => folded.push_str("fl"),
                'ﬆ' => folded.push_str("st"),
                'ﬅ' => folded.push_str("st"),

                'ⅰ' => folded.push('i'),
                'ⅱ' => folded.push_str("ii"),
                'ⅲ' => folded.push_str("iii"),
                'ⅳ' => folded.push_str("iv"),
                'ⅴ' => folded.push('v'),
                'ⅵ' => folded.push_str("vi"),
                'ⅶ' => folded.push_str("vii"),
                'ⅷ' => folded.push_str("viii"),
                'ⅸ' => folded.push_str("ix"),
                'ⅹ' => folded.push('x'),
                'ⅺ' => folded.push_str("xi"),
                'ⅻ' => folded.push_str("xii"),
                'ⅼ' => folded.push('l'),
                'ⅽ' => folded.push('c'),
                'ⅾ' => folded.push('d'),
                'ⅿ' => folded.push('m'),

                'ä' => folded.push_str("ae"),
                'ö' => folded.push_str("oe"),
                'ü' => folded.push_str("ue"),
                'ß' => folded.push_str("ss"),
                'ł' => folded.push('l'),
                'æ' => folded.push('a'),
                'œ' => folded.push('o'),
                'ø' => folded.push('o'),
                'ð' => folded.push('d'),
                'þ' => folded.push('t'),
                'đ' => folded.push('d'),
                'ɖ' => folded.push('d'),
                'ħ' => folded.push('h'),
                'ı' => folded.push('i'),
                'ƿ' => folded.push('w'),
                'ȝ' => folded.push('g'),
                'Ƿ' => folded.push('w'),
                'Ȝ' => folded.push('g'),

                _ => {
                    unicode_normalization::char::decompose_canonical(cc, |c| {
                        base_char.get_or_insert(c);
                    });
                    unicode_normalization::char::decompose_compatible(base_char.unwrap(), |c| {
                        if c.is_alphanumeric() {
                            base_char2.get_or_insert(c);
                        }
                    });
                    if base_char2.is_none() {
                        base_char2 = base_char
                    }

                    if !ZALGO_CHAR_CATEGORIES.contains(&base_char2.unwrap().get_minor_category()) {
                        match base_char2.unwrap() {
                            'ł' => folded.push('l'),
                            'æ' => folded.push('a'),
                            'œ' => folded.push('o'),
                            'ø' => folded.push('o'),
                            'ð' => folded.push('d'),
                            'þ' => folded.push('t'),
                            'đ' => folded.push('d'),
                            'ɖ' => folded.push('d'),
                            'ħ' => folded.push('h'),
                            'ı' => folded.push('i'),
                            'ƿ' => folded.push('w'),
                            'ȝ' => folded.push('g'),
                            'Ƿ' => folded.push('w'),
                            'Ȝ' => folded.push('g'),

                            _ => folded.push(base_char2.unwrap()),
                        }
                    }
                }
            }
            folded
        })
}

/// Tokenizer splits text to terms
#[allow(clippy::too_many_arguments)]
#[allow(clippy::assigning_clones)]
pub(crate) async fn tokenizer(
    index: &Shard,
    text: &str,
    unique_terms: &mut AHashMap<String, TermObject>,
    non_unique_terms: &mut Vec<NonUniqueTermObject>,
    tokenizer: TokenizerType,
    segment_number_mask1: u32,
    nonunique_terms_count: &mut u32,
    token_per_field_max: u32,
    position_per_term_max: usize,
    is_query: bool,
    query_type: &mut QueryType,
    ngram_indexing: u8,
    indexed_field_id: usize,
    indexed_field_number: usize,
) {
    let (max_completion_entries, completion_len) = if is_query {
        (0, 0)
    } else {
        let root_index = &index.index_option.as_ref().unwrap().read().await;
        if let Some(v) = root_index.completion_option.as_ref() {
            (root_index.max_completion_entries, v.read().await.len())
        } else {
            (0, 0)
        }
    };

    let token_per_field_max_capped = cmp::max(token_per_field_max, 65_536);

    let text_normalized;
    let mut non_unique_terms_line: Vec<&str> = Vec::new();
    let mut non_unique_terms_line_string: Vec<String> = Vec::new();

    let mut start = false;
    let mut start_pos = 0;
    let mut first_part = &text[0..0];

    if is_query {
        match tokenizer {
            TokenizerType::AsciiAlphabetic => {
                text_normalized = text.to_ascii_lowercase();
                for char in text_normalized.char_indices() {
                    start = match char.1 {
                        'a'..='z' | '"' | '+' | '-' => {
                            if !start {
                                start_pos = char.0;
                            }
                            true
                        }

                        _ => {
                            if start {
                                non_unique_terms_line.push(&text_normalized[start_pos..char.0]);
                            }
                            false
                        }
                    };
                }
            }

            TokenizerType::UnicodeAlphanumeric => {
                text_normalized = text.to_lowercase();
                for char in text_normalized.char_indices() {
                    start = match char.1 {
                        token if regex_syntax::is_word_character(token) => {
                            if !start {
                                start_pos = char.0;
                            }
                            true
                        }

                        '"' | '+' | '-' | '#' => {
                            if !start {
                                start_pos = char.0;
                            }
                            true
                        }
                        _ => {
                            if start {
                                non_unique_terms_line.push(&text_normalized[start_pos..char.0]);
                            }
                            false
                        }
                    };
                }
            }
            TokenizerType::UnicodeAlphanumericFolded => {
                text_normalized = fold_diacritics_accents_ligatures_zalgo_umlaut(text);
                for char in text_normalized.char_indices() {
                    start = match char.1 {
                        token if regex_syntax::is_word_character(token) => {
                            if !start {
                                start_pos = char.0;
                            }
                            true
                        }
                        '"' | '+' | '-' | '#' => {
                            if !start {
                                start_pos = char.0;
                            }
                            true
                        }

                        _ => {
                            let apostroph = APOSTROPH.contains(&char.1);
                            if start {
                                if apostroph {
                                    first_part = &text_normalized[start_pos..char.0];
                                } else {
                                    if first_part.len() >= 2 {
                                        non_unique_terms_line.push(first_part)
                                    } else {
                                        non_unique_terms_line
                                            .push(&text_normalized[start_pos..char.0]);
                                    }
                                    first_part = &text_normalized[0..0];
                                }
                            } else if !apostroph && !first_part.is_empty() {
                                non_unique_terms_line.push(first_part);
                                first_part = &text_normalized[0..0];
                            }

                            false
                        }
                    };
                }
            }

            TokenizerType::Whitespace => {
                text_normalized = text.to_owned();
                for char in text_normalized.char_indices() {
                    start = match char.1 {
                        token if !token.is_whitespace() => {
                            if !start {
                                start_pos = char.0;
                            }
                            true
                        }

                        _ => {
                            if start {
                                non_unique_terms_line.push(&text_normalized[start_pos..char.0]);
                            }
                            false
                        }
                    };
                }
            }

            TokenizerType::WhitespaceLowercase => {
                text_normalized = text.to_ascii_lowercase();
                for char in text_normalized.char_indices() {
                    start = match char.1 {
                        token if !token.is_whitespace() => {
                            if !start {
                                start_pos = char.0;
                            }
                            true
                        }

                        _ => {
                            if start {
                                non_unique_terms_line.push(&text_normalized[start_pos..char.0]);
                            }
                            false
                        }
                    };
                }
            }

            #[cfg(feature = "zh")]
            TokenizerType::UnicodeAlphanumericZH => {
                text_normalized = text.to_lowercase();
                for char in text_normalized.char_indices() {
                    start = match char.1 {
                        token if regex_syntax::is_word_character(token) => {
                            if !start {
                                start_pos = char.0;
                            }
                            true
                        }

                        '"' | '+' | '-' | '#' => {
                            if !start {
                                start_pos = char.0;
                            }
                            true
                        }
                        _ => {
                            if start {
                                let result = index
                                    .word_segmentation_option
                                    .as_ref()
                                    .unwrap()
                                    .segment(&text_normalized[start_pos..char.0], true);
                                non_unique_terms_line_string.extend(result.0);
                            }
                            false
                        }
                    };
                }
            }
        }
    } else {
        match tokenizer {
            TokenizerType::AsciiAlphabetic => {
                text_normalized = text.to_ascii_lowercase();
                for char in text_normalized.char_indices() {
                    start = match char.1 {
                        'a'..='z' => {
                            if !start {
                                start_pos = char.0;
                            }
                            true
                        }
                        _ => {
                            if start {
                                non_unique_terms_line.push(&text_normalized[start_pos..char.0]);
                            }
                            false
                        }
                    };
                }
            }
            TokenizerType::UnicodeAlphanumeric => {
                text_normalized = text.to_lowercase();
                for char in text_normalized.char_indices() {
                    start = match char.1 {
                        token if regex_syntax::is_word_character(token) => {
                            if !start {
                                start_pos = char.0;
                            }
                            true
                        }

                        '+' | '-' | '#' => start,

                        _ => {
                            if start {
                                non_unique_terms_line.push(&text_normalized[start_pos..char.0]);
                            }
                            false
                        }
                    };
                }
            }

            TokenizerType::UnicodeAlphanumericFolded => {
                text_normalized = fold_diacritics_accents_ligatures_zalgo_umlaut(text);

                for char in text_normalized.char_indices() {
                    start = match char.1 {
                        token if regex_syntax::is_word_character(token) => {
                            if !start {
                                start_pos = char.0;
                            }
                            true
                        }

                        '+' | '-' | '#' => start,

                        _ => {
                            let apostroph = APOSTROPH.contains(&char.1);
                            if start {
                                if apostroph {
                                    first_part = &text_normalized[start_pos..char.0];
                                } else {
                                    if first_part.len() >= 2 {
                                        non_unique_terms_line.push(first_part)
                                    } else {
                                        non_unique_terms_line
                                            .push(&text_normalized[start_pos..char.0]);
                                    }
                                    first_part = &text_normalized[0..0];
                                }
                            } else if !apostroph && !first_part.is_empty() {
                                non_unique_terms_line.push(first_part);
                                first_part = &text_normalized[0..0];
                            }

                            false
                        }
                    };
                }
            }

            TokenizerType::Whitespace => {
                text_normalized = text.to_owned();
                for char in text_normalized.char_indices() {
                    start = match char.1 {
                        token if !token.is_whitespace() => {
                            if !start {
                                start_pos = char.0;
                            }
                            true
                        }
                        _ => {
                            if start {
                                non_unique_terms_line.push(&text_normalized[start_pos..char.0]);
                            }
                            false
                        }
                    };
                }
            }

            TokenizerType::WhitespaceLowercase => {
                text_normalized = text.to_ascii_lowercase();
                for char in text_normalized.char_indices() {
                    start = match char.1 {
                        token if !token.is_whitespace() => {
                            if !start {
                                start_pos = char.0;
                            }
                            true
                        }
                        _ => {
                            if start {
                                non_unique_terms_line.push(&text_normalized[start_pos..char.0]);
                            }
                            false
                        }
                    };
                }
            }

            #[cfg(feature = "zh")]
            TokenizerType::UnicodeAlphanumericZH => {
                text_normalized = text.to_lowercase();
                for char in text_normalized.char_indices() {
                    start = match char.1 {
                        token if regex_syntax::is_word_character(token) => {
                            if !start {
                                start_pos = char.0;
                            }
                            true
                        }

                        '+' | '-' | '#' => start,

                        _ => {
                            if start {
                                let result = index
                                    .word_segmentation_option
                                    .as_ref()
                                    .unwrap()
                                    .segment(&text_normalized[start_pos..char.0], true);
                                non_unique_terms_line_string.extend(result.0);
                            }
                            false
                        }
                    };
                }
            }
        }
    }

    #[cfg(feature = "zh")]
    if tokenizer == TokenizerType::UnicodeAlphanumericZH {
        if start {
            if first_part.len() >= 2 {
                let result = index
                    .word_segmentation_option
                    .as_ref()
                    .unwrap()
                    .segment(first_part, true);
                non_unique_terms_line_string.extend(result.0);
            } else {
                non_unique_terms_line.push(&text_normalized[start_pos..text_normalized.len()]);
                let result = index
                    .word_segmentation_option
                    .as_ref()
                    .unwrap()
                    .segment(&text_normalized[start_pos..text_normalized.len()], true);
                non_unique_terms_line_string.extend(result.0);
            }
        } else if !first_part.is_empty() {
            let result = index
                .word_segmentation_option
                .as_ref()
                .unwrap()
                .segment(first_part, true);
            non_unique_terms_line_string.extend(result.0);
        }
        non_unique_terms_line = non_unique_terms_line_string
            .iter()
            .map(|s| s.as_str())
            .collect();
    }

    if tokenizer == TokenizerType::AsciiAlphabetic
        || tokenizer == TokenizerType::UnicodeAlphanumeric
        || tokenizer == TokenizerType::UnicodeAlphanumericFolded
        || tokenizer == TokenizerType::Whitespace
        || tokenizer == TokenizerType::WhitespaceLowercase
    {
        if start {
            if first_part.len() >= 2 {
                non_unique_terms_line.push(first_part)
            } else {
                non_unique_terms_line.push(&text_normalized[start_pos..text_normalized.len()]);
            }
        } else if !first_part.is_empty() {
            non_unique_terms_line.push(first_part)
        }
    }

    if is_query && non_unique_terms_line.len() > MAX_QUERY_TERM_NUMBER {
        non_unique_terms_line.truncate(MAX_QUERY_TERM_NUMBER);
    }

    let mut position: u32 = 0;
    let mut is_phrase = query_type == &QueryType::Phrase;
    let mut term_string_1 = "".to_string();
    let mut term_frequent_1 = false;
    let mut term_string_2 = "".to_string();
    let mut term_frequent_2 = false;

    let mut term_len_1 = 0;
    let mut term_len_2 = 0;

    let mut non_unique_terms_raw = Vec::new();

    for term_string in non_unique_terms_line.iter_mut() {
        if is_query {
            let mut query_type_term = if is_phrase {
                QueryType::Phrase
            } else {
                query_type.clone()
            };
            if term_string.starts_with('+') {
                if query_type != &QueryType::Phrase {
                    *query_type = QueryType::Intersection;
                }
                query_type_term = QueryType::Intersection;
                *term_string = &term_string[1..];
            } else if term_string.starts_with('-') {
                query_type_term = QueryType::Not;
                *term_string = &term_string[1..];
            }
            if term_string.starts_with('\"') {
                is_phrase = true;
                *query_type = QueryType::Phrase;
                query_type_term = QueryType::Phrase;
                *term_string = &term_string[1..];
            }
            if term_string.ends_with('\"') {
                *query_type = QueryType::Phrase;
                *term_string = &term_string[0..term_string.len() - 1];
                is_phrase = false;
            }

            if term_string.is_empty() {
                continue;
            }

            if !index.stop_words.is_empty() && index.stop_words.contains(*term_string) {
                continue;
            }

            let term_string = if let Some(stemmer) = index.stemmer.as_ref() {
                stemmer.stem(term_string).to_string()
            } else {
                term_string.to_string()
            };

            non_unique_terms_raw.push((term_string, query_type_term));
        } else {
            if !index.stop_words.is_empty() && index.stop_words.contains(*term_string) {
                continue;
            }

            let term_string_0 = if let Some(stemmer) = index.stemmer.as_ref() {
                stemmer.stem(term_string).to_string()
            } else {
                term_string.to_string()
            };

            let mut term_positions_len;
            let term_hash_0 = hash64(term_string_0.as_bytes());
            let term_frequent_0 = index.frequent_hashset.contains(&term_hash_0);

            let term_number_0 = term_string_0.chars().next().unwrap().is_ascii_digit()
                && term_string_0.chars().last().unwrap().is_ascii_digit();
            let term_len_0 = term_string_0.chars().count();

            if index.indexed_schema_vec[indexed_field_id].completion_source {
                let mut level_completions = index.level_completions.write().await;

                if !term_number_0 && term_len_0 > 1 {
                    let unigram_string = vec![term_string_0.clone()];
                    if completion_len < max_completion_entries {
                        level_completions
                            .entry(unigram_string)
                            .and_modify(|v| {
                                *v += 1;
                            })
                            .or_insert(1);
                    }
                }

                if !term_string_1.is_empty() {
                    if term_len_1 > 1 {
                        let bigram_string = vec![term_string_1.clone(), term_string_0.clone()];
                        if completion_len < max_completion_entries {
                            level_completions
                                .entry(bigram_string)
                                .and_modify(|v| {
                                    *v += 1;
                                })
                                .or_insert(1);
                        }
                    }

                    if !term_string_2.is_empty() && term_len_2 > 1 {
                        let trigram_string = vec![
                            term_string_2.clone(),
                            term_string_1.clone(),
                            term_string_0.clone(),
                        ];
                        if completion_len < max_completion_entries {
                            level_completions
                                .entry(trigram_string)
                                .and_modify(|v| {
                                    *v += 1;
                                })
                                .or_insert(1);
                        }
                    }
                }

                drop(level_completions);

                term_len_2 = term_len_1;
                term_len_1 = term_len_0;
            }

            let term_object = unique_terms
                .entry(term_string_0.clone())
                .or_insert_with(|| {
                    let term_bytes = term_string_0.as_bytes();
                    TermObject {
                        term: term_string_0.clone(),

                        key0: hash32(term_bytes) & segment_number_mask1,
                        key_hash: hash64(term_bytes),

                        field_positions_vec: vec![Vec::new(); indexed_field_number],
                        ngram_type: NgramType::SingleTerm,

                        ..Default::default()
                    }
                });

            term_object.field_positions_vec[indexed_field_id].push(position as u16);
            term_positions_len = term_object.field_positions_vec[indexed_field_id].len();

            if !term_string_1.is_empty()
                && (ngram_indexing & NgramSet::NgramFF as u8 != 0
                    && term_frequent_1
                    && term_frequent_0)
            {
                let term_string = [term_string_1.as_str(), term_string_0.as_str()].join(" ");

                let term_object = unique_terms.entry(term_string.clone()).or_insert_with(|| {
                    let term_bytes = term_string.as_bytes();
                    TermObject {
                        term: term_string.clone(),
                        key0: hash32(term_bytes) & segment_number_mask1,
                        key_hash: hash64(term_bytes) | NgramType::NgramFF as u64,
                        field_positions_vec: vec![Vec::new(); indexed_field_number],
                        ngram_type: NgramType::NgramFF,
                        term_ngram_1: term_string_1.clone(),
                        term_ngram_0: term_string_0.clone(),

                        ..Default::default()
                    }
                });
                term_object.field_positions_vec[indexed_field_id].push(position as u16 - 1);
                term_positions_len = term_object.field_positions_vec[indexed_field_id].len();
            }

            if !term_string_1.is_empty()
                && (ngram_indexing & NgramSet::NgramRF as u8 != 0
                    && !term_frequent_1
                    && term_frequent_0)
            {
                let term_string = [term_string_1.as_str(), term_string_0.as_str()].join(" ");

                let term_object = unique_terms.entry(term_string.clone()).or_insert_with(|| {
                    let term_bytes = term_string.as_bytes();
                    TermObject {
                        term: term_string.clone(),
                        key0: hash32(term_bytes) & segment_number_mask1,
                        key_hash: hash64(term_bytes) | NgramType::NgramRF as u64,
                        field_positions_vec: vec![Vec::new(); indexed_field_number],
                        ngram_type: NgramType::NgramRF,
                        term_ngram_1: term_string_1.clone(),
                        term_ngram_0: term_string_0.clone(),

                        ..Default::default()
                    }
                });
                term_object.field_positions_vec[indexed_field_id].push(position as u16 - 1);
                term_positions_len = term_object.field_positions_vec[indexed_field_id].len();
            }

            if !term_string_1.is_empty()
                && (ngram_indexing & NgramSet::NgramFR as u8 != 0
                    && term_frequent_1
                    && !term_frequent_0)
            {
                let term_string = [term_string_1.as_str(), term_string_0.as_str()].join(" ");

                let term_object = unique_terms.entry(term_string.clone()).or_insert_with(|| {
                    let term_bytes = term_string.as_bytes();
                    TermObject {
                        term: term_string.clone(),
                        key0: hash32(term_bytes) & segment_number_mask1,
                        key_hash: hash64(term_bytes) | NgramType::NgramFR as u64,
                        field_positions_vec: vec![Vec::new(); indexed_field_number],
                        ngram_type: NgramType::NgramFR,
                        term_ngram_1: term_string_1.clone(),
                        term_ngram_0: term_string_0.clone(),

                        ..Default::default()
                    }
                });

                term_object.field_positions_vec[indexed_field_id].push(position as u16 - 1);
                term_positions_len = term_object.field_positions_vec[indexed_field_id].len();
            }

            if !term_string_2.is_empty()
                && !term_string_1.is_empty()
                && (ngram_indexing & NgramSet::NgramFFF as u8 != 0
                    && term_frequent_2
                    && term_frequent_1
                    && term_frequent_0)
            {
                let term_string = [
                    term_string_2.as_str(),
                    term_string_1.as_str(),
                    term_string_0.as_str(),
                ]
                .join(" ");

                let term_object = unique_terms.entry(term_string.clone()).or_insert_with(|| {
                    let term_bytes = term_string.as_bytes();
                    TermObject {
                        term: term_string.clone(),
                        key0: hash32(term_bytes) & segment_number_mask1,
                        key_hash: hash64(term_bytes) | NgramType::NgramFFF as u64,
                        field_positions_vec: vec![Vec::new(); indexed_field_number],
                        ngram_type: NgramType::NgramFFF,
                        term_ngram_2: term_string_2.clone(),
                        term_ngram_1: term_string_1.clone(),
                        term_ngram_0: term_string_0.clone(),

                        ..Default::default()
                    }
                });
                term_object.field_positions_vec[indexed_field_id].push(position as u16 - 2);
                term_positions_len = term_object.field_positions_vec[indexed_field_id].len();
            }

            if !term_string_2.is_empty()
                && !term_string_1.is_empty()
                && (ngram_indexing & NgramSet::NgramRFF as u8 != 0
                    && !term_frequent_2
                    && term_frequent_1
                    && term_frequent_0)
            {
                let term_string = [
                    term_string_2.as_str(),
                    term_string_1.as_str(),
                    term_string_0.as_str(),
                ]
                .join(" ");

                let term_object = unique_terms.entry(term_string.clone()).or_insert_with(|| {
                    let term_bytes = term_string.as_bytes();
                    TermObject {
                        term: term_string.clone(),
                        key0: hash32(term_bytes) & segment_number_mask1,
                        key_hash: hash64(term_bytes) | NgramType::NgramRFF as u64,
                        field_positions_vec: vec![Vec::new(); indexed_field_number],
                        ngram_type: NgramType::NgramRFF,
                        term_ngram_2: term_string_2.clone(),
                        term_ngram_1: term_string_1.clone(),
                        term_ngram_0: term_string_0.clone(),

                        ..Default::default()
                    }
                });
                term_object.field_positions_vec[indexed_field_id].push(position as u16 - 2);
                term_positions_len = term_object.field_positions_vec[indexed_field_id].len();
            }

            if !term_string_2.is_empty()
                && !term_string_1.is_empty()
                && (ngram_indexing & NgramSet::NgramRFF as u8 != 0
                    && term_frequent_2
                    && term_frequent_1
                    && !term_frequent_0)
            {
                let term_string = [
                    term_string_2.as_str(),
                    term_string_1.as_str(),
                    term_string_0.as_str(),
                ]
                .join(" ");

                let term_object = unique_terms.entry(term_string.clone()).or_insert_with(|| {
                    let term_bytes = term_string.as_bytes();
                    TermObject {
                        term: term_string.clone(),
                        key0: hash32(term_bytes) & segment_number_mask1,
                        key_hash: hash64(term_bytes) | NgramType::NgramFFR as u64,
                        field_positions_vec: vec![Vec::new(); indexed_field_number],
                        ngram_type: NgramType::NgramFFR,
                        term_ngram_2: term_string_2.clone(),
                        term_ngram_1: term_string_1.clone(),
                        term_ngram_0: term_string_0.clone(),

                        ..Default::default()
                    }
                });
                term_object.field_positions_vec[indexed_field_id].push(position as u16 - 2);
                term_positions_len = term_object.field_positions_vec[indexed_field_id].len();
            }

            if !term_string_2.is_empty()
                && !term_string_1.is_empty()
                && (ngram_indexing & NgramSet::NgramRFF as u8 != 0
                    && term_frequent_2
                    && !term_frequent_1
                    && term_frequent_0)
            {
                let term_string = [
                    term_string_2.as_str(),
                    term_string_1.as_str(),
                    term_string_0.as_str(),
                ]
                .join(" ");

                let term_object = unique_terms.entry(term_string.clone()).or_insert_with(|| {
                    let term_bytes = term_string.as_bytes();
                    TermObject {
                        term: term_string.clone(),
                        key0: hash32(term_bytes) & segment_number_mask1,
                        key_hash: hash64(term_bytes) | NgramType::NgramFRF as u64,
                        field_positions_vec: vec![Vec::new(); indexed_field_number],
                        ngram_type: NgramType::NgramFRF,
                        term_ngram_2: term_string_2,
                        term_ngram_1: term_string_1.clone(),
                        term_ngram_0: term_string_0.clone(),

                        ..Default::default()
                    }
                });
                term_object.field_positions_vec[indexed_field_id].push(position as u16 - 2);
                term_positions_len = term_object.field_positions_vec[indexed_field_id].len();
            }

            term_string_2 = term_string_1;
            term_string_1 = term_string_0;

            term_frequent_2 = term_frequent_1;
            term_frequent_1 = term_frequent_0;

            position += 1;

            if position >= token_per_field_max_capped {
                break;
            }
            if term_positions_len >= position_per_term_max {
                continue;
            }
        };
    }

    if is_query {
        let len = non_unique_terms_raw.len();

        let mut term_0;
        let mut term_frequent_0;
        let mut term_phrase_0;

        if len > 0 {
            let item = &non_unique_terms_raw[0];
            term_0 = item.0.clone();
            let term_hash_0 = hash64(term_0.as_bytes());
            term_frequent_0 = index.frequent_hashset.contains(&term_hash_0);
            term_phrase_0 = item.1 == QueryType::Phrase;
        } else {
            term_0 = "".to_string();
            term_frequent_0 = false;
            term_phrase_0 = false;
        }

        let mut term_1;
        let mut term_frequent_1;
        let mut term_phrase_1;
        if len > 1 {
            let item = &non_unique_terms_raw[1];
            term_1 = item.0.clone();
            let term_hash_1 = hash64(term_1.as_bytes());
            term_frequent_1 = index.frequent_hashset.contains(&term_hash_1);
            term_phrase_1 = item.1 == QueryType::Phrase;
        } else {
            term_1 = "".to_string();
            term_frequent_1 = false;
            term_phrase_1 = false;
        }

        let len = non_unique_terms_raw.len();
        let mut i = 0;
        while i < len {
            let term_2;
            let term_frequent_2;
            let term_phrase_2;
            if len > i + 2 {
                let item = &non_unique_terms_raw[i + 2];
                term_2 = item.0.clone();
                let term_hash_2 = hash64(term_2.as_bytes());
                term_frequent_2 = index.frequent_hashset.contains(&term_hash_2);
                term_phrase_2 = item.1 == QueryType::Phrase;
            } else {
                term_2 = "".to_string();
                term_frequent_2 = false;
                term_phrase_2 = false;
            }
            if i + 2 < len
                && (ngram_indexing & NgramSet::NgramFFF as u8 != 0
                    && term_frequent_0
                    && term_frequent_1
                    && term_frequent_2
                    && term_phrase_0
                    && term_phrase_1
                    && term_phrase_2)
            {
                let term_string = [term_0.as_str(), term_1.as_str(), term_2.as_str()].join(" ");

                let term_object = unique_terms.entry(term_string.clone()).or_insert_with(|| {
                    let term_bytes = term_string.as_bytes();
                    TermObject {
                        term: term_string.clone(),
                        key0: hash32(term_bytes) & segment_number_mask1,
                        key_hash: hash64(term_bytes) | NgramType::NgramFFF as u64,
                        field_positions_vec: vec![Vec::new(); indexed_field_number],
                        ngram_type: NgramType::NgramFFF,
                        term_ngram_2: term_0.clone(),
                        term_ngram_1: term_1.clone(),
                        term_ngram_0: term_2.clone(),

                        ..Default::default()
                    }
                });
                term_object.field_positions_vec[indexed_field_id].push(position as u16);

                non_unique_terms.push(NonUniqueTermObject {
                    term: term_string,
                    ngram_type: NgramType::NgramFFF,
                    op: QueryType::Phrase,
                    term_ngram_2: term_0.clone(),
                    term_ngram_1: term_1.clone(),
                    term_ngram_0: term_2.clone(),
                });

                i += 3;

                if len > i {
                    let item = &non_unique_terms_raw[i];
                    term_0 = item.0.clone();
                    let term_hash_0 = hash64(term_0.as_bytes());
                    term_frequent_0 = index.frequent_hashset.contains(&term_hash_0);
                    term_phrase_0 = item.1 == QueryType::Phrase;
                } else {
                    term_0 = "".to_string();
                    term_frequent_0 = false;
                    term_phrase_0 = false;
                }

                if len > i + 1 {
                    let item = &non_unique_terms_raw[i + 1];
                    term_1 = item.0.clone();
                    let term_hash_1 = hash64(term_1.as_bytes());
                    term_frequent_1 = index.frequent_hashset.contains(&term_hash_1);
                    term_phrase_1 = item.1 == QueryType::Phrase;
                } else {
                    term_1 = "".to_string();
                    term_frequent_1 = false;
                    term_phrase_1 = false;
                }
            } else if i + 2 < len
                && (ngram_indexing & NgramSet::NgramRFF as u8 != 0
                    && !term_frequent_0
                    && term_frequent_1
                    && term_frequent_2
                    && term_phrase_0
                    && term_phrase_1
                    && term_phrase_2)
            {
                let term_string = [term_0.as_str(), term_1.as_str(), term_2.as_str()].join(" ");

                let term_object = unique_terms.entry(term_string.clone()).or_insert_with(|| {
                    let term_bytes = term_string.as_bytes();
                    TermObject {
                        term: term_string.clone(),
                        key0: hash32(term_bytes) & segment_number_mask1,
                        key_hash: hash64(term_bytes) | NgramType::NgramRFF as u64,
                        field_positions_vec: vec![Vec::new(); indexed_field_number],
                        ngram_type: NgramType::NgramRFF,
                        term_ngram_2: term_0.clone(),
                        term_ngram_1: term_1.clone(),
                        term_ngram_0: term_2.clone(),

                        ..Default::default()
                    }
                });
                term_object.field_positions_vec[indexed_field_id].push(position as u16);

                non_unique_terms.push(NonUniqueTermObject {
                    term: term_string,
                    ngram_type: NgramType::NgramRFF,
                    op: QueryType::Phrase,
                    term_ngram_2: term_0.clone(),
                    term_ngram_1: term_1.clone(),
                    term_ngram_0: term_2.clone(),
                });

                i += 3;

                if len > i {
                    let item = &non_unique_terms_raw[i];
                    term_0 = item.0.clone();
                    let term_hash_0 = hash64(term_0.as_bytes());
                    term_frequent_0 = index.frequent_hashset.contains(&term_hash_0);
                    term_phrase_0 = item.1 == QueryType::Phrase;
                } else {
                    term_0 = "".to_string();
                    term_frequent_0 = false;
                    term_phrase_0 = false;
                }

                if len > i + 1 {
                    let item = &non_unique_terms_raw[i + 1];
                    term_1 = item.0.clone();
                    let term_hash_1 = hash64(term_1.as_bytes());
                    term_frequent_1 = index.frequent_hashset.contains(&term_hash_1);
                    term_phrase_1 = item.1 == QueryType::Phrase;
                } else {
                    term_1 = "".to_string();
                    term_frequent_1 = false;
                    term_phrase_1 = false;
                }
            } else if i + 2 < len
                && (ngram_indexing & NgramSet::NgramFFR as u8 != 0
                    && term_frequent_0
                    && term_frequent_1
                    && !term_frequent_2
                    && term_phrase_0
                    && term_phrase_1
                    && term_phrase_2)
            {
                let term_string = [term_0.as_str(), term_1.as_str(), term_2.as_str()].join(" ");

                let term_object = unique_terms.entry(term_string.clone()).or_insert_with(|| {
                    let term_bytes = term_string.as_bytes();
                    TermObject {
                        term: term_string.clone(),
                        key0: hash32(term_bytes) & segment_number_mask1,
                        key_hash: hash64(term_bytes) | NgramType::NgramFFR as u64,
                        field_positions_vec: vec![Vec::new(); indexed_field_number],
                        ngram_type: NgramType::NgramFFR,
                        term_ngram_2: term_0.clone(),
                        term_ngram_1: term_1.clone(),
                        term_ngram_0: term_2.clone(),

                        ..Default::default()
                    }
                });
                term_object.field_positions_vec[indexed_field_id].push(position as u16);

                non_unique_terms.push(NonUniqueTermObject {
                    term: term_string,
                    ngram_type: NgramType::NgramFFR,
                    op: QueryType::Phrase,
                    term_ngram_2: term_0.clone(),
                    term_ngram_1: term_1.clone(),
                    term_ngram_0: term_2.clone(),
                });

                i += 3;

                if len > i {
                    let item = &non_unique_terms_raw[i];
                    term_0 = item.0.clone();
                    let term_hash_0 = hash64(term_0.as_bytes());
                    term_frequent_0 = index.frequent_hashset.contains(&term_hash_0);
                    term_phrase_0 = item.1 == QueryType::Phrase;
                } else {
                    term_0 = "".to_string();
                    term_frequent_0 = false;
                    term_phrase_0 = false;
                }

                if len > i + 1 {
                    let item = &non_unique_terms_raw[i + 1];
                    term_1 = item.0.clone();
                    let term_hash_1 = hash64(term_1.as_bytes());
                    term_frequent_1 = index.frequent_hashset.contains(&term_hash_1);
                    term_phrase_1 = item.1 == QueryType::Phrase;
                } else {
                    term_1 = "".to_string();
                    term_frequent_1 = false;
                    term_phrase_1 = false;
                }
            } else if i + 2 < len
                && (ngram_indexing & NgramSet::NgramFRF as u8 != 0
                    && term_frequent_0
                    && !term_frequent_1
                    && term_frequent_2
                    && term_phrase_0
                    && term_phrase_1
                    && term_phrase_2)
            {
                let term_string = [term_0.as_str(), term_1.as_str(), term_2.as_str()].join(" ");

                let term_object = unique_terms.entry(term_string.clone()).or_insert_with(|| {
                    let term_bytes = term_string.as_bytes();
                    TermObject {
                        term: term_string.clone(),
                        key0: hash32(term_bytes) & segment_number_mask1,
                        key_hash: hash64(term_bytes) | NgramType::NgramFRF as u64,
                        field_positions_vec: vec![Vec::new(); indexed_field_number],
                        ngram_type: NgramType::NgramFRF,
                        term_ngram_2: term_0.clone(),
                        term_ngram_1: term_1.clone(),
                        term_ngram_0: term_2.clone(),

                        ..Default::default()
                    }
                });
                term_object.field_positions_vec[indexed_field_id].push(position as u16);

                non_unique_terms.push(NonUniqueTermObject {
                    term: term_string,
                    ngram_type: NgramType::NgramFRF,
                    op: QueryType::Phrase,
                    term_ngram_2: term_0.clone(),
                    term_ngram_1: term_1.clone(),
                    term_ngram_0: term_2.clone(),
                });

                i += 3;

                if len > i {
                    let item = &non_unique_terms_raw[i];
                    term_0 = item.0.clone();
                    let term_hash_0 = hash64(term_0.as_bytes());
                    term_frequent_0 = index.frequent_hashset.contains(&term_hash_0);
                    term_phrase_0 = item.1 == QueryType::Phrase;
                } else {
                    term_0 = "".to_string();
                    term_frequent_0 = false;
                    term_phrase_0 = false;
                }

                if len > i + 1 {
                    let item = &non_unique_terms_raw[i + 1];
                    term_1 = item.0.clone();
                    let term_hash_1 = hash64(term_1.as_bytes());
                    term_frequent_1 = index.frequent_hashset.contains(&term_hash_1);
                    term_phrase_1 = item.1 == QueryType::Phrase;
                } else {
                    term_1 = "".to_string();
                    term_frequent_1 = false;
                    term_phrase_1 = false;
                }
            } else if i + 1 < len
                && (ngram_indexing & NgramSet::NgramFF as u8 != 0
                    && term_frequent_0
                    && term_frequent_1
                    && term_phrase_0
                    && term_phrase_1)
            {
                let term_string = [term_0.as_str(), term_1.as_str()].join(" ");

                let term_object = unique_terms.entry(term_string.clone()).or_insert_with(|| {
                    let term_bytes = term_string.as_bytes();
                    TermObject {
                        term: term_string.clone(),
                        key0: hash32(term_bytes) & segment_number_mask1,
                        key_hash: hash64(term_bytes) | NgramType::NgramFF as u64,
                        field_positions_vec: vec![Vec::new(); indexed_field_number],
                        ngram_type: NgramType::NgramFF,
                        term_ngram_1: term_0.clone(),
                        term_ngram_0: term_1.clone(),

                        ..Default::default()
                    }
                });
                term_object.field_positions_vec[indexed_field_id].push(position as u16);

                non_unique_terms.push(NonUniqueTermObject {
                    term: term_string,
                    ngram_type: NgramType::NgramFF,
                    op: QueryType::Phrase,
                    term_ngram_1: term_0.clone(),
                    term_ngram_0: term_1.clone(),

                    ..Default::default()
                });

                i += 2;

                term_0 = term_2.clone();
                term_frequent_0 = term_frequent_2;

                if len > i + 1 {
                    let item = &non_unique_terms_raw[i + 1];
                    term_1 = item.0.clone();
                    let term_hash_1 = hash64(term_1.as_bytes());
                    term_frequent_1 = index.frequent_hashset.contains(&term_hash_1);
                    term_phrase_1 = item.1 == QueryType::Phrase;
                } else {
                    term_1 = "".to_string();
                    term_frequent_1 = false;
                    term_phrase_1 = false;
                }
            } else if i + 1 < len
                && (ngram_indexing & NgramSet::NgramRF as u8 != 0
                    && !term_frequent_0
                    && term_frequent_1
                    && term_phrase_0
                    && term_phrase_1)
            {
                let term_string = [term_0.as_str(), term_1.as_str()].join(" ");

                let term_object = unique_terms.entry(term_string.clone()).or_insert_with(|| {
                    let term_bytes = term_string.as_bytes();
                    TermObject {
                        term: term_string.clone(),
                        key0: hash32(term_bytes) & segment_number_mask1,
                        key_hash: hash64(term_bytes) | NgramType::NgramRF as u64,
                        field_positions_vec: vec![Vec::new(); indexed_field_number],
                        ngram_type: NgramType::NgramRF,
                        term_ngram_1: term_0.clone(),
                        term_ngram_0: term_1.clone(),

                        ..Default::default()
                    }
                });
                term_object.field_positions_vec[indexed_field_id].push(position as u16);

                non_unique_terms.push(NonUniqueTermObject {
                    term: term_string,
                    ngram_type: NgramType::NgramRF,
                    op: QueryType::Phrase,
                    term_ngram_1: term_0.clone(),
                    term_ngram_0: term_1.clone(),

                    ..Default::default()
                });

                i += 2;

                term_0 = term_2.clone();
                term_frequent_0 = term_frequent_2;

                if len > i + 1 {
                    let item = &non_unique_terms_raw[i + 1];
                    term_1 = item.0.clone();
                    let term_hash_1 = hash64(term_1.as_bytes());
                    term_frequent_1 = index.frequent_hashset.contains(&term_hash_1);
                    term_phrase_1 = item.1 == QueryType::Phrase;
                } else {
                    term_1 = "".to_string();
                    term_frequent_1 = false;
                    term_phrase_1 = false;
                }
            } else if i + 1 < len
                && (ngram_indexing & NgramSet::NgramFR as u8 != 0
                    && term_frequent_0
                    && !term_frequent_1
                    && term_phrase_0
                    && term_phrase_1)
            {
                let term_string = [term_0.as_str(), term_1.as_str()].join(" ");

                let term_object = unique_terms.entry(term_string.clone()).or_insert_with(|| {
                    let term_bytes = term_string.as_bytes();
                    TermObject {
                        term: term_string.clone(),
                        key0: hash32(term_bytes) & segment_number_mask1,
                        key_hash: hash64(term_bytes) | NgramType::NgramFR as u64,
                        field_positions_vec: vec![Vec::new(); indexed_field_number],
                        ngram_type: NgramType::NgramFR,
                        term_ngram_1: term_0.clone(),
                        term_ngram_0: term_1.clone(),

                        ..Default::default()
                    }
                });
                term_object.field_positions_vec[indexed_field_id].push(position as u16);

                non_unique_terms.push(NonUniqueTermObject {
                    term: term_string,
                    ngram_type: NgramType::NgramFR,
                    op: QueryType::Phrase,
                    term_ngram_1: term_0.clone(),
                    term_ngram_0: term_1.clone(),

                    ..Default::default()
                });

                i += 2;

                term_0 = term_2.clone();
                term_frequent_0 = term_frequent_2;

                if len > i + 1 {
                    let item = &non_unique_terms_raw[i + 1];
                    term_1 = item.0.clone();
                    let term_hash_1 = hash64(term_1.as_bytes());
                    term_frequent_1 = index.frequent_hashset.contains(&term_hash_1);
                    term_phrase_1 = item.1 == QueryType::Phrase;
                } else {
                    term_1 = "".to_string();
                    term_frequent_1 = false;
                    term_phrase_1 = false;
                }
            } else {
                let term_string = term_0.clone();

                let term_object = unique_terms.entry(term_string.clone()).or_insert_with(|| {
                    let term_bytes = term_string.as_bytes();
                    TermObject {
                        term: term_string.to_string(),
                        key0: hash32(term_bytes) & segment_number_mask1,
                        key_hash: hash64(term_bytes),
                        field_positions_vec: vec![Vec::new(); indexed_field_number],
                        ngram_type: NgramType::SingleTerm,
                        ..Default::default()
                    }
                });
                term_object.field_positions_vec[indexed_field_id].push(position as u16);

                non_unique_terms.push(NonUniqueTermObject {
                    term: term_string,
                    ngram_type: NgramType::SingleTerm,
                    op: non_unique_terms_raw[i].1.clone(),
                    ..Default::default()
                });

                i += 1;

                term_0.clone_from(&term_1);
                term_1.clone_from(&term_2);

                term_frequent_0 = term_frequent_1;
                term_frequent_1 = term_frequent_2;

                term_phrase_0 = term_phrase_1;
                term_phrase_1 = term_phrase_2;
            };

            position += 1;
        }
    }

    *nonunique_terms_count = position;
}

/// Parse a string into words, using the specified tokenizer type.
pub fn tokenizer_lite(
    text: &str,
    tokenizer: &TokenizerType,
    index: &Shard,
) -> Vec<(String, QueryType)> {
    let text_normalized;
    let mut non_unique_terms_line: Vec<String> = Vec::new();

    let mut start = false;
    let mut start_pos = 0;
    let mut first_part = &text[0..0];

    match tokenizer {
        TokenizerType::AsciiAlphabetic => {
            text_normalized = text.to_ascii_lowercase();
            for char in text_normalized.char_indices() {
                start = match char.1 {
                    'a'..='z' | '"' | '+' | '-' => {
                        if !start {
                            start_pos = char.0;
                        }
                        true
                    }

                    _ => {
                        if start {
                            non_unique_terms_line
                                .push(text_normalized[start_pos..char.0].to_string());
                        }
                        false
                    }
                };
            }
        }
        TokenizerType::UnicodeAlphanumeric => {
            text_normalized = text.to_lowercase();
            for char in text_normalized.char_indices() {
                start = match char.1 {
                    token if regex_syntax::is_word_character(token) => {
                        if !start {
                            start_pos = char.0;
                        }
                        true
                    }

                    '"' | '+' | '-' | '#' => {
                        if !start {
                            start_pos = char.0;
                        }
                        true
                    }
                    _ => {
                        if start {
                            non_unique_terms_line
                                .push(text_normalized[start_pos..char.0].to_string());
                        }
                        false
                    }
                };
            }
        }
        TokenizerType::UnicodeAlphanumericFolded => {
            text_normalized = fold_diacritics_accents_ligatures_zalgo_umlaut(text);
            for char in text_normalized.char_indices() {
                start = match char.1 {
                    token if regex_syntax::is_word_character(token) => {
                        if !start {
                            start_pos = char.0;
                        }
                        true
                    }
                    '"' | '+' | '-' | '#' => {
                        if !start {
                            start_pos = char.0;
                        }
                        true
                    }

                    _ => {
                        let apostroph = APOSTROPH.contains(&char.1);
                        if start {
                            if apostroph {
                                first_part = &text_normalized[start_pos..char.0];
                            } else {
                                if first_part.len() >= 2 {
                                    non_unique_terms_line.push(first_part.to_string())
                                } else {
                                    non_unique_terms_line
                                        .push(text_normalized[start_pos..char.0].to_string());
                                }
                                first_part = &text_normalized[0..0];
                            }
                        } else if !apostroph && !first_part.is_empty() {
                            non_unique_terms_line.push(first_part.to_string());
                            first_part = &text_normalized[0..0];
                        }

                        false
                    }
                };
            }
        }

        TokenizerType::Whitespace => {
            text_normalized = text.to_owned();
            for char in text_normalized.char_indices() {
                start = match char.1 {
                    token if !token.is_whitespace() => {
                        if !start {
                            start_pos = char.0;
                        }
                        true
                    }

                    _ => {
                        if start {
                            non_unique_terms_line
                                .push(text_normalized[start_pos..char.0].to_string());
                        }
                        false
                    }
                };
            }
        }

        TokenizerType::WhitespaceLowercase => {
            text_normalized = text.to_ascii_lowercase();
            for char in text_normalized.char_indices() {
                start = match char.1 {
                    token if !token.is_whitespace() => {
                        if !start {
                            start_pos = char.0;
                        }
                        true
                    }

                    _ => {
                        if start {
                            non_unique_terms_line
                                .push(text_normalized[start_pos..char.0].to_string());
                        }
                        false
                    }
                };
            }
        }

        #[cfg(feature = "zh")]
        TokenizerType::UnicodeAlphanumericZH => {
            text_normalized = text.to_lowercase();
            for char in text_normalized.char_indices() {
                start = match char.1 {
                    token if regex_syntax::is_word_character(token) => {
                        if !start {
                            start_pos = char.0;
                        }
                        true
                    }

                    '"' | '+' | '-' | '#' => {
                        if !start {
                            start_pos = char.0;
                        }
                        true
                    }
                    _ => {
                        if start {
                            let result = index
                                .word_segmentation_option
                                .as_ref()
                                .unwrap()
                                .segment(&text_normalized[start_pos..char.0], true);
                            non_unique_terms_line.extend(result.0);
                        }
                        false
                    }
                };
            }
        }
    }

    #[cfg(feature = "zh")]
    if tokenizer == &TokenizerType::UnicodeAlphanumericZH {
        if start {
            if first_part.len() >= 2 {
                let result = index
                    .word_segmentation_option
                    .as_ref()
                    .unwrap()
                    .segment(first_part, true);
                non_unique_terms_line.extend(result.0);
            } else {
                non_unique_terms_line
                    .push(text_normalized[start_pos..text_normalized.len()].to_string());
                let result = index
                    .word_segmentation_option
                    .as_ref()
                    .unwrap()
                    .segment(&text_normalized[start_pos..text_normalized.len()], true);
                non_unique_terms_line.extend(result.0);
            }
        } else if !first_part.is_empty() {
            let result = index
                .word_segmentation_option
                .as_ref()
                .unwrap()
                .segment(first_part, true);
            non_unique_terms_line.extend(result.0);
        }
    }

    if tokenizer != &TokenizerType::AsciiAlphabetic
        || tokenizer == &TokenizerType::UnicodeAlphanumeric
        || tokenizer == &TokenizerType::UnicodeAlphanumericFolded
        || tokenizer == &TokenizerType::Whitespace
        || tokenizer == &TokenizerType::WhitespaceLowercase
    {
        if start {
            if first_part.len() >= 2 {
                non_unique_terms_line.push(first_part.to_string())
            } else {
                non_unique_terms_line
                    .push(text_normalized[start_pos..text_normalized.len()].to_string());
            }
        } else if !first_part.is_empty() {
            non_unique_terms_line.push(first_part.to_string())
        }
    }

    let mut non_unique_terms_raw = Vec::new();
    let query_type = &mut QueryType::Union;
    let mut is_phrase = query_type == &QueryType::Phrase;
    let mut is_endswith_quote = false;
    for term_string in non_unique_terms_line.iter_mut() {
        if is_endswith_quote {
            return Vec::new();
        }

        let mut query_type_term = if is_phrase {
            QueryType::Phrase
        } else {
            query_type.clone()
        };
        if term_string.starts_with('+') || term_string.starts_with('-') {
            return Vec::new();
        }
        if term_string.starts_with('\"') {
            if !non_unique_terms_raw.is_empty() {
                return Vec::new();
            }

            is_phrase = true;
            *query_type = QueryType::Phrase;
            query_type_term = QueryType::Phrase;
            *term_string = term_string[1..].to_string();
        }
        if term_string.ends_with('\"') {
            *query_type = QueryType::Phrase;
            *term_string = term_string[0..term_string.len() - 1].to_string();
            is_phrase = false;
            is_endswith_quote = true;
        }

        if term_string.is_empty() {
            continue;
        }

        if !index.stop_words.is_empty() && index.stop_words.contains(term_string) {
            continue;
        }

        let term_string = if let Some(stemmer) = index.stemmer.as_ref() {
            stemmer.stem(term_string).to_string()
        } else {
            term_string.to_string()
        };

        non_unique_terms_raw.push((term_string, query_type_term));
    }

    non_unique_terms_raw
}
