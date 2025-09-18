# r_remove_um

## Location
src/backend/snowball/libstemmer/stem_UTF_8_tamil.c: 1124 - 1147

## Overview
Removes the specific Tamil "um" suffix pattern and replaces it with a standardized form while applying morphological corrections.

## Definition


## Detailed Description
This function handles a specific Tamil morphological pattern involving the "um" suffix. The function:

1. Validates minimum word length using r_has_min_length to prevent over-stemming
2. Sets up backward processing from the word end
3. Looks for a specific 9-character pattern (s_54) ending the word using exact backward matching
4. Replaces the matched pattern with a 3-character standardized form (s_55)
5. Applies morphological correction through r_fix_ending to ensure proper word formation
6. Uses cursor position preservation to maintain processing state

Unlike other suffix functions that may handle multiple patterns, this function targets one very specific morphological transformation.

## Parameters / Member Variables
- : Pointer to the Snowball environment structure containing:
  - : State flag set to 1 when the "um" pattern is successfully processed
  - //: Cursor positions for boundary/current/limit management
  - /: Bracket positions marking the pattern for replacement
  - : Temporary cursor position for state preservation during post-processing

## Dependencies
- Functions called/Symbols referenced:
  - r_has_min_length (validates minimum word length)
  - eq_s_b (exact backward string matching for 9-character pattern s_54)
  - slice_from_s (replaces matched pattern with 3-character form s_55)
  - r_fix_ending (applies morphological corrections after replacement)
- Called from (representative examples):
  - tamil_UTF_8_stem (main Tamil stemming function)

## Notes and Other Information
- Handles a very specific Tamil morphological pattern ("um" suffix variations)
- More targeted than other suffix removal functions, focusing on one 9-character pattern
- Includes post-processing correction through r_fix_ending, indicating this transformation may affect word structure
- The pattern replacement (9 chars → 3 chars) suggests significant morphological simplification
- Part of the Tamil stemming pipeline that handles various morphological endings and particles
- The specific pattern (s_54/s_55) likely represents a common Tamil grammatical construction that needs normalization for search/indexing purposes