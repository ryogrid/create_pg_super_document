# r_mark_ndAn

## Location
src/backend/snowball/libstemmer/stem_UTF_8_turkish.c: 751 - 759

## Overview
A static function in the Turkish snowball stemmer that checks for the ablative case suffix "ndAn" while ensuring vowel harmony compliance.

## Definition
```c
static int r_mark_ndAn(struct SN_env * z)
```

## Detailed Description
This function is part of the Turkish language stemmer implementation in PostgreSQL's snowball library. It specifically identifies and validates the ablative case suffix "ndAn" (and its vowel harmony variants like "ndan", "nden") in Turkish words. The function performs a two-step validation: first checking vowel harmony rules, then verifying the suffix pattern against a predefined set of suffixes.

The function operates by:
1. Calling r_check_vowel_harmony() to ensure the suffix follows Turkish vowel harmony rules
2. Checking that the current position has at least 3 characters before the left boundary (since "ndAn" is longer than "DAn")
3. Verifying the last character is 'n' (ASCII 110)
4. Using find_among_b() to match against suffix patterns in array a_9

## Parameters / Member Variables
- `z`: Pointer to SN_env structure containing the stemming environment, including the word being processed, current position, and boundaries

## Dependencies
- Functions called/Symbols referenced:
  - r_check_vowel_harmony
  - find_among_b (with array a_9)
- Called from (representative examples):
  - r_stem_noun_suffixes

## Notes and Other Information
- Returns 1 on successful match, 0 on failure
- Part of the Turkish noun suffix stemming process
- The "ndAn" suffix in Turkish indicates the ablative case ("from" in English) with an additional "n" component
- The array a_9 contains 2 different suffix variants to accommodate vowel harmony
- This function is automatically generated code from snowball stemming algorithms
- Requires at least 3 characters before left boundary, unlike r_mark_DAn which requires 2