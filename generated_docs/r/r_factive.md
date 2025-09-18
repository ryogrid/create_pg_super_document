# r_factive

## Location
src/backend/snowball/libstemmer/stem_ISO_8859_2_hungarian.c: 653 - 672

## Overview
The r_factive function handles Hungarian factive case endings by removing 'á' and 'é' suffixes that indicate the cause or reason for an action, while managing consonant doubling patterns.

## Definition


## Detailed Description
This function processes factive case endings in Hungarian, which express causation or reason ("because of" or "for the sake of"). The factive case in Hungarian is marked by the suffixes 'á' and 'é' that are added to word stems.

The function operates through several steps:
1. First checks that the word ends with 'á' (ASCII 225) or 'é' (ASCII 233)  
2. Uses find_among_b to match against the factive patterns in array a_7
3. Verifies the suffix is in the R1 morphological region
4. Checks for doubled consonants before the suffix using r_double
5. Removes the factive suffix with slice_del
6. Applies consonant undoubling with r_undouble to restore proper morphology

This processing is essential for Hungarian morphological analysis since factive forms are common in expressing causal relationships and need to be reduced to their base forms for effective full-text search and linguistic processing.

## Parameters / Member Variables
- : Pointer to the SN_env structure containing the word being processed, cursor positions, and string boundaries

## Dependencies
- Functions called/Symbols referenced:
  - find_among_b (searches for factive patterns from array a_7 containing 'á', 'é')
  - r_R1 (checks if position is in R1 region)
  - r_double (detects doubled consonants before the suffix)
  - slice_del (removes the factive suffix)
  - r_undouble (removes doubled consonants after suffix removal)
- Called from (representative examples):
  - hungarian_ISO_8859_2_stem
  - hungarian_UTF_8_stem

## Notes and Other Information
- The factive case is a relatively rare case in Hungarian but important for complete morphological coverage
- The function handles both front vowel (é) and back vowel (á) harmony variants
- Consonant doubling/undoubling is crucial because Hungarian morphophonology often involves consonant alternations
- Returns 1 on successful factive processing, 0 if conditions aren't met, and negative values on errors
- The factive case often appears in formal or literary Hungarian and legal/administrative texts
- This function is part of PostgreSQL's comprehensive Hungarian stemming for full-text search capabilities