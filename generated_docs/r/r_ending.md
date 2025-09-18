# r_ending

## Location
src/backend/snowball/libstemmer/stem_UTF_8_armenian.c: 502 - 514

## Overview
A static function that identifies and removes Armenian ending suffixes from words during the Armenian text stemming process.

## Definition


## Detailed Description
The  function is part of the Armenian language stemming algorithm implementation in PostgreSQL's Snowball stemmer. It identifies and removes specific Armenian ending patterns by matching against a predefined set of 57 different Armenian suffixes stored in the  array. The function operates by searching backwards from the current cursor position to find matching suffixes, and only removes them if they occur within the R2 morphological region of the word.

The function follows the typical Snowball stemming pattern: it sets the  marker at the current cursor position, searches for matching patterns using , sets the  marker, validates that the match is in the R2 region, and then deletes the matched suffix if all conditions are met.

## Parameters / Member Variables
- : Pointer to the SN_env structure containing the stemming environment, including the word being processed and cursor positions

## Dependencies
- Functions called/Symbols referenced:
  - find_among_b (searches backwards through the  array of 57 Armenian ending patterns)
  - r_R2 (validates that the match occurs within the R2 morphological region)
  - slice_del (removes the matched suffix from the word)
- Called from:
  - armenian_UTF_8_stem (main Armenian stemming function at line 527)

## Notes and Other Information
- Returns 1 on successful suffix removal, 0 if no matching pattern found, or negative values on error
- The  array contains 57 predefined Armenian ending patterns encoded in UTF-8
- Part of the Snowball stemming algorithm generated code for Armenian language support
- Only removes suffixes that occur in the R2 region, ensuring morphologically appropriate stemming
- This function is called as part of the multi-step Armenian stemming process which includes noun, verb, and adjective handling