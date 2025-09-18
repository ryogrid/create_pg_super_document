# r_mark_yUm

## Location
src/backend/snowball/libstemmer/stem_UTF_8_turkish.c: 789 - 800

## Overview
A static function in the Turkish stemmer that identifies and marks the suffix "yUm" (meaning "I am" in Turkish) and its variations, used in Turkish verb conjugation processing.

## Definition
```c
static int r_mark_yUm(struct SN_env * z)
```

## Detailed Description
This function is part of the Turkish language stemmer in PostgreSQL's Snowball implementation. It identifies the Turkish suffix "yUm" and its phonetic variations ("im", "um", "ım", "üm") which represent the first person singular present tense form "I am" in Turkish verbs. The function performs vowel harmony checking before attempting to match the suffix pattern, ensuring linguistic correctness according to Turkish grammar rules.

The function works by:
1. First checking vowel harmony using `r_check_vowel_harmony()`
2. Checking if the current character is 'm' (ASCII 109)
3. Using `find_among_b()` to match against the predefined suffix array `a_12`
4. Finally calling `r_mark_suffix_with_optional_y_consonant()` to handle the optional 'y' consonant insertion

## Parameters / Member Variables
- `z`: Pointer to the SN_env structure containing the stemming environment, including the string being processed, cursor position, and other state information

## Dependencies
- Functions called/Symbols referenced:
  - [r_check_vowel_harmony](r_check_vowel_harmony.md)
  - [find_among_b](../f/find_among_b.md)
  - [r_mark_suffix_with_optional_y_consonant](r_mark_suffix_with_optional_y_consonant.md)
  - a_12 (static array containing suffix patterns: "im", "um", "ım", "üm")
- Called from (representative examples):
  - [r_stem_nominal_verb_suffixes](r_stem_nominal_verb_suffixes.md) (at lines 955, 1073, 1119)

## Notes and Other Information
- The `a_12` array contains 4 suffix variations: "im", "um", "ım" (with dotless ı), and "üm"
- These suffixes follow Turkish vowel harmony rules where the vowel selection depends on the preceding vowels in the word
- The function returns 1 on successful match, 0 on failure, and propagates negative return values from called functions
- This is part of the Turkish morphological analysis system used for full-text search indexing in PostgreSQL