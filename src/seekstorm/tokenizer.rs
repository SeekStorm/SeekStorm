use std::cmp;

use ahash::AHashMap;

use crate::{
    index::{
        NonUniqueTermObject, TermObject, TokenizerType, HASHER_32, HASHER_64, STOPWORD_HASHSET,
    },
    search::QueryType,
};

/// Tokenizer splits text to terms
#[allow(clippy::too_many_arguments)]
pub(crate) fn tokenizer(
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

    let mut start = false;
    let mut start_pos = 0;

    if is_query {
        if tokenizer == TokenizerType::AsciiAlphabetic {
            text_normalized = text.to_ascii_lowercase();
            for char in text_normalized.char_indices() {
                start = match char.1 {
                    'a'..='z' | '"' | '+' | '-' => {
                        if !start {
                            start_pos = char.0;
                        }
                        true
                    }
                    //end of term
                    _ => {
                        if start {
                            non_unique_terms_line.push(&text_normalized[start_pos..char.0]);
                        }
                        false
                    }
                };
            }
        } else {
            text_normalized = text.to_lowercase();
            for char in text_normalized.char_indices() {
                start = match char.1 {
                    //start of term
                    token if regex_syntax::is_word_character(token) => {
                        if !start {
                            start_pos = char.0;
                        }
                        true
                    }
                    '"' | '+' | '-' => {
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
    } else if tokenizer == TokenizerType::AsciiAlphabetic {
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
    } else {
        text_normalized = text.to_lowercase();
        for char in text_normalized.char_indices() {
            start = match char.1 {
                token if regex_syntax::is_word_character(token) => {
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

    if start {
        non_unique_terms_line.push(&text_normalized[start_pos..text_normalized.len()]);
    };

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
                QueryType::Union
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
            let bigram_term_string = previous_term_string.to_string() + " " + &term_string;
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
                non_unique_term.term = bigram_term_string.clone();
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
