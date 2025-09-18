# r_case_special

## Location
src/backend/snowball/libstemmer/stem_ISO_8859_2_hungarian.c: 598 - 622

## Overview
The r_case_special function handles special Hungarian case endings that require transformation rather than simple deletion, specifically dealing with endings that contain 'án', 'én', and 'ánként'.

## Definition


## Detailed Description
This function processes special case endings in Hungarian that require vowel substitution instead of complete removal. It handles three specific patterns:

1. 'án' ending → transforms to 'a'
2. 'én' ending → transforms to 'e'  
3. 'ánként' ending → transforms to 'a'

The function first checks that the word ends in 'n' or 't' (ASCII 110, 116), then uses find_among_b to match against the special case patterns. Unlike regular case processing that removes suffixes entirely, this function replaces them with specific vowels to maintain proper Hungarian word formation.

The switch statement handles the transformations:
- Case 1: 'én' → 'e' (using s_2)
- Case 2: 'án' and 'ánként' → 'a' (using s_3)

This specialized handling is necessary because some Hungarian case forms require vowel preservation to maintain morphological validity when stemmed.

## Parameters / Member Variables
- : Pointer to the SN_env structure containing the word being processed, cursor positions, and string boundaries

## Dependencies
- Functions called/Symbols referenced:
  - [find_among_b](../f/find_among_b.md) (searches for special case patterns from array a_5 containing 'án', 'én', 'ánként')
  - [r_R1](r_R1.md) (checks if position is in R1 region)
  - [slice_from_s](../s/slice_from_s.md) (replaces suffix with specified vowel: 'a' or 'e')
- Called from (representative examples):
  - [hungarian_ISO_8859_2_stem](../h/hungarian_ISO_8859_2_stem.md)
  - [hungarian_UTF_8_stem](../h/hungarian_UTF_8_stem.md)

## Notes and Other Information
- This function handles morphologically complex Hungarian endings that cannot be simply deleted
- The function specifically deals with adverbial and special case forms ending in '-án', '-én', and the distributive case '-ánként'
- Returns 1 on successful transformation, 0 if no pattern matches, and negative values on errors
- This transformation approach preserves the phonological constraints of Hungarian morphology
- The pre-check for 'n' or 't' endings optimizes performance by avoiding unnecessary pattern matching