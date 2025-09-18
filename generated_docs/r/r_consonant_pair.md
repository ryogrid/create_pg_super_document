# r_consonant_pair

## Location
src/backend/snowball/libstemmer/stem_ISO_8859_1_danish.c: 187 - 209

## Overview
This function removes doubled consonants (specifically 'd' and 't') that occur after suffix removal in Scandinavian language stemming algorithms.

## Definition


## Detailed Description
The r_consonant_pair function handles a specific morphological cleanup step in Scandinavian stemming by removing doubled consonants that may remain after suffix removal. The function operates in two phases:

1. **Detection Phase**: Uses a test position to search backward from the current cursor position for specific doubled consonant patterns. It checks if the character at the current position is either 'd' (100) or 't' (116), then uses pattern matching against a small set of known doubled consonant patterns (array a_1 with 4 entries).

2. **Removal Phase**: If a doubled consonant is detected, the function moves back one character and deletes the duplicate consonant using slice_del.

The function includes boundary checking to ensure operations occur only within the appropriate region (beyond I[1] boundary) and uses temporary position saving (m_test1) to maintain state during the detection process.

## Parameters / Member Variables
- : Pointer to SN_env structure containing the word being processed
  - : Current cursor position in the word  
  - : Length of the word
  - : Region boundary marker (from r_mark_regions)
  - : Left boundary limit
  - : End position of matched substring
  - : Beginning position of matched substring
  - : Pointer to the word being processed

## Dependencies
- Functions called/Symbols referenced:
  - find_among_b (backward pattern matching against doubled consonant list a_1)
  - slice_del (consonant deletion operation)
  - a_1 (array of 4 doubled consonant patterns)
- Called from (representative examples):
  - r_other_suffix (within the same stemming process)
  - danish_ISO_8859_1_stem (main Danish stemming function)
  - norwegian_ISO_8859_1_stem (main Norwegian stemming function)
  - swedish_ISO_8859_1_stem (main Swedish stemming function)
  - danish_UTF_8_stem, norwegian_UTF_8_stem, swedish_UTF_8_stem (UTF-8 variants)

## Notes and Other Information
- Specifically targets characters 100 ('d') and 116 ('t') which are commonly doubled in Scandinavian languages
- The function is part of a cleanup process that occurs after main suffix removal to handle morphological remnants
- Uses test position mechanism (m_test1) to preserve cursor state during detection
- Returns 0 if no doubled consonant is found or if boundary conditions aren't met, 1 if successful
- The boundary checking ensures consonant pair removal only occurs in appropriate word regions
- This function is called both independently in main stemming routines and as part of other suffix processing (r_other_suffix)