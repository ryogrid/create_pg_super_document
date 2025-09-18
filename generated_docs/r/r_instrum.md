# r_instrum

## Location
src/backend/snowball/libstemmer/stem_ISO_8859_2_hungarian.c: 562 - 581

## Overview
The r_instrum function handles instrumental case endings in Hungarian words, specifically removing 'al' and 'el' suffixes when certain morphological conditions are met.

## Definition


## Detailed Description
This function is part of the Hungarian stemmer that processes instrumental case endings. It operates by first setting the ending position (ket), then checking for the presence of specific instrumental suffixes 'al' or 'el'. The function applies several morphological checks:

1. Verifies the word boundary contains 'l' (ASCII 108)
2. Uses find_among_b to match against the instrumental suffix patterns ('al', 'el')
3. Ensures the suffix is in the R1 region (morphologically significant part)
4. Checks for doubled consonants before the suffix
5. Removes the suffix and handles any consonant undoubling

The function follows the standard Hungarian stemming rules where instrumental case markers are removed only when they appear in morphologically appropriate contexts.

## Parameters / Member Variables
- : Pointer to the SN_env structure containing the word being processed, cursor positions, and string boundaries

## Dependencies
- Functions called/Symbols referenced:
  - find_among_b (searches for suffixes from array a_3 containing 'al', 'el')
  - r_R1 (checks if position is in R1 region)
  - r_double (detects doubled consonants)
  - slice_del (removes the matched suffix)
  - r_undouble (removes doubled consonants)
- Called from (representative examples):
  - hungarian_ISO_8859_2_stem
  - hungarian_UTF_8_stem

## Notes and Other Information
- This function specifically handles the Hungarian instrumental case, which is formed by adding '-val/-vel' suffixes that can appear as '-al/-el' after certain consonants
- The function returns 1 on successful processing, 0 if conditions aren't met, and negative values on errors
- The instrumental case removal is part of the broader Hungarian morphological analysis in PostgreSQL's full-text search capabilities