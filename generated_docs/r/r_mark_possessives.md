# r_mark_possessives

## Location
src/backend/snowball/libstemmer/stem_UTF_8_turkish.c: 646 - 654

## Overview
A static function in the Turkish stemmer that identifies and marks possessive suffixes in Turkish words as part of the Snowball stemming algorithm.

## Definition


## Detailed Description
This function is part of the Turkish language stemming implementation in PostgreSQL's Snowball stemmer library. It identifies possessive suffixes in Turkish words by performing backwards pattern matching against a predefined set of possessive suffix patterns. The function uses bit manipulation to optimize character checking and employs the  function to match against an array of 10 possessive suffix patterns (). Upon successful pattern identification, it calls  to handle vowel harmony rules specific to Turkish morphology.

## Parameters / Member Variables
- : Pointer to the Snowball environment structure containing the word being processed, current position markers, and other stemming state information

## Dependencies
- Functions called/Symbols referenced:
  - find_among_b (Snowball library function for backwards pattern matching)
  - r_mark_suffix_with_optional_U_vowel (handles Turkish vowel harmony rules)
  - a_0 (array of 10 possessive suffix patterns)
- Called from (representative examples):
  - r_stem_suffix_chain_before_ki
  - r_stem_noun_suffixes

## Notes and Other Information
- Returns 1 on successful possessive suffix identification and marking, 0 on failure
- Uses bit manipulation (67133440 >> (z->p[z->c - 1] & 0x1f)) for efficient character class checking
- Part of the larger Turkish stemming algorithm that handles the complex morphology of the Turkish language
- The function operates in backwards mode, processing the word from right to left which is typical for suffix identification