# r_case

## Location
src/backend/snowball/libstemmer/stem_ISO_8859_2_hungarian.c: 582 - 597

## Overview
The r_case function handles Hungarian case endings by removing various grammatical case suffixes from words when morphological conditions are satisfied.

## Definition


## Detailed Description
This function is a core component of the Hungarian stemmer that processes case endings. Hungarian has an extensive case system with multiple forms, and this function handles the removal of 44 different case suffix patterns including:

- Sublative case: 'ba', 'be', 'ra', 're' (onto, to)
- Instrumental case: 'val', 'vel' (with/by)
- Dative case: 'nak', 'nek' (to/for)
- Ablative case: 'ból', 'ből', 'ról', 'ről', 'tól', 'től' (from)
- Superessive case: 'n', 'an', 'en', 'on', 'ön' (on/at)
- Accusative case: 't', 'at', 'et' (direct object marker)
- And various other case forms

The function operates by matching against these patterns using find_among_b, ensuring the suffix is in the morphologically active R1 region, removing the suffix, and then applying vowel ending adjustments.

## Parameters / Member Variables
- : Pointer to the SN_env structure containing the word being processed, cursor positions, and string boundaries

## Dependencies
- Functions called/Symbols referenced:
  - find_among_b (searches for case suffixes from array a_4 containing 44 patterns)
  - r_R1 (checks if position is in R1 region)
  - slice_del (removes the matched case suffix)
  - r_v_ending (handles vowel ending adjustments after suffix removal)
- Called from (representative examples):
  - hungarian_ISO_8859_2_stem
  - hungarian_UTF_8_stem

## Notes and Other Information
- Hungarian case morphology is highly complex with 18-35 cases depending on linguistic analysis
- The function handles both front and back vowel harmony variants (e.g., 'ba'/'be', 'nak'/'nek')
- Case suffix removal is essential for Hungarian full-text search as it reduces inflected forms to their base stems
- The function returns 1 on successful case processing, 0 if no case pattern matches, and negative values on errors
- This is one of the most comprehensive case handling functions in the Hungarian stemmer due to the language's rich morphological system