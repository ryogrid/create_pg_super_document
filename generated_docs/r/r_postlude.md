# r_postlude

## Location
src/backend/snowball/libstemmer/stem_UTF_8_turkish.c: 2039 - 2066

## Overview
Performs final post-processing operations on Turkish words after stemming, including reserved word checking, vowel harmony restoration, and consonant transformations.

## Definition


## Detailed Description
This function serves as the final cleanup phase in the Turkish stemming process, orchestrating three critical post-processing operations:

1. **Reserved Word Check**: First calls  to check if the word should not be stemmed (e.g., "ad", "soyad"). If a reserved word is detected, stemming is terminated and the word is returned unchanged.

2. **Vowel Harmony Restoration**: Calls  to append appropriate vowels to stems ending with 'd' or 'g' consonants, maintaining Turkish vowel harmony rules.

3. **Consonant Processing**: Calls  to apply final consonant transformations, particularly devoicing operations (b→p, c→ç, d→t, ğ→k).

The function processes the string from right to left (backward processing) by setting the left boundary to current position and cursor to the end of the string.

## Parameters / Member Variables
- : Pointer to the Snowball environment structure containing the string being processed and cursor positions

## Dependencies
- Functions called/Symbols referenced:
  - r_is_reserved_word (checks for protected words that shouldn't be stemmed)
  - r_append_U_to_stems_ending_with_d_or_g (applies vowel harmony rules)
  - r_post_process_last_consonants (applies final consonant transformations)
- Called from:
  - turkish_UTF_8_stem (at src/backend/snowball/libstemmer/stem_UTF_8_turkish.c:2087)

## Notes and Other Information
- This is a static function within the Turkish stemmer implementation
- Returns 0 if a reserved word is detected (no further processing), 1 on successful completion, or negative value on error
- Critical final phase ensuring Turkish morphological and phonological rules are properly applied
- Part of the main stemming pipeline in Turkish word processing
- Generated automatically by Snowball 2.2.0 stemmer generator
- Uses backward processing pattern typical of Snowball stemmers