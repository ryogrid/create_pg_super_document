# r_mark_sUn

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_turkish.c:801-809](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_turkish.c#L801-L809)

## Overview
A static function in the Turkish stemmer that identifies and marks the suffix "sUn" (meaning "you are" in Turkish) and its variations, used in Turkish verb conjugation processing.

## Definition
```c
static int r_mark_sUn(struct SN_env * z)
```

## Detailed Description
This function is part of the Turkish language stemmer in PostgreSQL's Snowball implementation. It identifies the Turkish suffix "sUn" and its phonetic variations ("sin", "sun", "sın", "sün") which represent the second person singular present tense form "you are" in Turkish verbs. The function performs vowel harmony checking before attempting to match the suffix pattern, ensuring linguistic correctness according to Turkish grammar rules.

The function works by:
1. First checking vowel harmony using `r_check_vowel_harmony()`
2. Checking if the current character is 'n' (ASCII 110) 
3. Using `find_among_b()` to match against the predefined suffix array `a_13`
4. Returns 1 on successful match

## Parameters / Member Variables
- `z`: Pointer to the SN_env structure containing the stemming environment, including the string being processed, cursor position, and other state information

## Dependencies
- Functions called/Symbols referenced:
  - [r_check_vowel_harmony](r_check_vowel_harmony.md)
  - [find_among_b](../f/find_among_b.md)
  - a_13 (static array containing suffix patterns: "sin", "sun", "sın", "sün")
- Called from (representative examples):
  - [r_stem_nominal_verb_suffixes](r_stem_nominal_verb_suffixes.md) (at lines 962, 1066, 1126)

## Notes and Other Information
- The `a_13` array contains 4 suffix variations: "sin", "sun", "sın" (with dotless ı), and "sün"
- These suffixes follow Turkish vowel harmony rules for second person singular conjugation
- The function checks that there are at least 2 characters before the current position (z->c - 2 <= z->lb)
- Unlike r_mark_yUm, this function doesn't call r_mark_suffix_with_optional_y_consonant as the 's' is always present
- The function returns 1 on successful match, 0 on failure, and propagates negative return values from called functions
- This is part of the Turkish morphological analysis system used for full-text search indexing in PostgreSQL