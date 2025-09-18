# r_mark_ncA

## Location
src/backend/snowball/libstemmer/stem_UTF_8_turkish.c: 777 - 788

## Overview
A static function in the Turkish snowball stemmer that checks for the possessive case suffix "ncA" while ensuring vowel harmony compliance and handling optional consonant insertion.

## Definition
```c
static int r_mark_ncA(struct SN_env * z)
```

## Detailed Description
This function is part of the Turkish language stemmer implementation in PostgreSQL's snowball library. It specifically identifies and validates the possessive case suffix "ncA" (and its vowel harmony variants like "nca", "nce") in Turkish words. The function performs a three-step validation: checking vowel harmony rules, verifying the suffix pattern, and handling optional 'n' consonant insertion that occurs in Turkish morphology.

The function operates by:
1. Calling r_check_vowel_harmony() to ensure the suffix follows Turkish vowel harmony rules
2. Checking that the current position has at least 1 character before the left boundary
3. Verifying the last character is either 'a' (ASCII 97) or 'e' (ASCII 101)
4. Using find_among_b() to match against suffix patterns in array a_11
5. Calling r_mark_suffix_with_optional_n_consonant() to handle the optional 'n' consonant insertion

## Parameters / Member Variables
- `z`: Pointer to SN_env structure containing the stemming environment, including the word being processed, current position, and boundaries

## Dependencies
- Functions called/Symbols referenced:
  - r_check_vowel_harmony
  - find_among_b (with array a_11)
  - r_mark_suffix_with_optional_n_consonant
- Called from (representative examples):
  - r_stem_noun_suffixes

## Notes and Other Information
- Returns 1 on successful match, 0 on failure
- Part of the Turkish noun suffix stemming process
- The "ncA" suffix in Turkish indicates possession or associative relationships
- The array a_11 contains 2 different suffix variants to accommodate vowel harmony
- This function is automatically generated code from snowball stemming algorithms
- Handles the morphophonological process where 'n' may be inserted as a buffer consonant
- Similar in structure to r_mark_ylA but handles 'n' consonant insertion instead of 'y'