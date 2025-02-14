use std::cmp;

use ahash::AHashMap;
use finl_unicode::categories::{CharacterCategories, MinorCategory};

use crate::{
    index::{
        Index, NonUniqueTermObject, TermObject, TokenizerType, HASHER_32, HASHER_64,
        MAX_TERM_NUMBER, STOPWORD_HASHSET,
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
pub fn fold_diacritics_accents_zalgo_umlaut(string: &str) -> String {
    string
        .to_lowercase()
        .chars()
        .fold(String::with_capacity(string.len()), |mut folded, cc| {
            let mut base_char = None;
            let mut base_char2 = None;

            match cc {
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
pub(crate) fn tokenizer(
    index: &Index,
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
    enable_bigram: bool,
    indexed_field_id: usize,
    indexed_field_number: usize,
) {
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
                text_normalized = fold_diacritics_accents_zalgo_umlaut(text);
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
                text_normalized = fold_diacritics_accents_zalgo_umlaut(text);

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
    {
        if start {
            if first_part.len() >= 2 {
                non_unique_terms_line.push(first_part)
            } else {
                non_unique_terms_line.push(&text_normalized[start_pos..text_normalized.len()]);
            }
        } else if !first_part.is_empty() {
            non_unique_terms_line.push(first_part);
        }
    }

    if is_query && non_unique_terms_line.len() > MAX_TERM_NUMBER {
        non_unique_terms_line.truncate(MAX_TERM_NUMBER);
    }

    let mut position: u32 = 0;
    let mut is_phrase = query_type == &QueryType::Phrase;
    let mut previous_term_string = "".to_string();
    let mut previous_term_hash = 0;

    let mut bigrams: Vec<TermObject> = Vec::new();
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

            non_unique_terms.push(NonUniqueTermObject {
                term: term_string.to_string(),
                term_bigram1: "".to_string(),
                term_bigram2: "".to_string(),
                is_bigram: false,
                op: query_type_term,
            });
        }

        let term_hash;
        let term_positions_len;
        {
            let term_object = unique_terms
                .entry(term_string.to_string())
                .or_insert_with(|| {
                    let term_bytes = term_string.as_bytes();
                    TermObject {
                        term: term_string.to_string(),
                        key0: HASHER_32.hash_one(term_bytes) as u32 & segment_number_mask1,
                        key_hash: HASHER_64.hash_one(term_bytes),

                        field_positions_vec: vec![Vec::new(); indexed_field_number],

                        ..Default::default()
                    }
                });

            term_object.field_positions_vec[indexed_field_id].push(position as u16);
            term_hash = term_object.key_hash;
            term_positions_len = term_object.field_positions_vec[indexed_field_id].len();
        }

        if enable_bigram
            && position > 0
            && STOPWORD_HASHSET.contains(&term_hash)
            && STOPWORD_HASHSET.contains(&previous_term_hash)
            && (!is_query
                || (non_unique_terms[non_unique_terms.len() - 1].op == QueryType::Phrase
                    && non_unique_terms[non_unique_terms.len() - 2].op == QueryType::Phrase))
        {
            let bigram_term_string = previous_term_string.to_string() + " " + term_string;
            let bigram_term_hash = HASHER_64.hash_one(bigram_term_string.as_bytes());

            if is_query {
                if unique_terms[&term_string.to_string()].field_positions_vec[indexed_field_id]
                    .len()
                    == 1
                {
                    unique_terms.remove(&term_string.to_string());
                } else {
                    unique_terms
                        .get_mut(&term_string.to_string())
                        .unwrap()
                        .field_positions_vec[indexed_field_id]
                        .pop();
                }
                if unique_terms[&previous_term_string.to_string()].field_positions_vec
                    [indexed_field_id]
                    .len()
                    == 1
                {
                    unique_terms.remove(&previous_term_string.to_string());
                } else {
                    unique_terms
                        .get_mut(&previous_term_string.to_string())
                        .unwrap()
                        .field_positions_vec[indexed_field_id]
                        .pop();
                }
                non_unique_terms.pop();

                let non_unique_term = non_unique_terms.last_mut().unwrap();
                non_unique_term.term.clone_from(&bigram_term_string);
                non_unique_term.term_bigram1 = previous_term_string;
                non_unique_term.term_bigram2 = term_string.to_string();
                non_unique_term.is_bigram = true;
                previous_term_string = bigram_term_string.clone();
                previous_term_hash = bigram_term_hash;
            }

            let bigram_term_object = unique_terms
                .entry(bigram_term_string.clone())
                .or_insert_with(|| TermObject {
                    term: bigram_term_string.clone(),
                    key0: HASHER_32.hash_one(bigram_term_string.as_bytes()) as u32
                        & segment_number_mask1,
                    key_hash: bigram_term_hash,
                    is_bigram: true,
                    term_bigram1: previous_term_string.clone(),
                    term_bigram2: term_string.to_string(),

                    field_positions_vec: vec![Vec::new(); indexed_field_number],

                    ..Default::default()
                });

            if !is_query {
                previous_term_string = term_string.to_string();
                previous_term_hash = term_hash;
            }
            if bigram_term_object.field_positions_vec[indexed_field_id].is_empty() {
                bigrams.push(bigram_term_object.clone());
            }

            bigram_term_object.field_positions_vec[indexed_field_id].push(position as u16 - 1);
        } else {
            previous_term_string = term_string.to_string();
            previous_term_hash = term_hash;
        }

        position += 1;

        if position >= token_per_field_max_capped {
            break;
        }
        if term_positions_len >= position_per_term_max {
            continue;
        }
    }
    *nonunique_terms_count = position;
}
