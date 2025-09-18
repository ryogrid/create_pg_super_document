# r_i_plural

## Location
src/backend/snowball/libstemmer/stem_UTF_8_finnish.c: 510 - 526

## Overview
The r_i_plural function identifies and removes Finnish plural endings that involve the letters i or j in the stemming process.

## Definition
static int r_i_plural(struct SN_env * z)

## Detailed Description
This function handles the removal of Finnish plural markers that contain the characters i (ASCII 105) or j (ASCII 106). It operates within the boundaries defined by z->I[1] (likely the R2 region) and uses a small pattern array (a_8) containing only 2 patterns.

The function follows this process:
1. Sets up processing boundaries using z->I[1] as the left limit
2. Checks if the character immediately before the cursor is either i (105) or j (106)
3. If the character check passes, uses find_among_b to match against 2 predefined patterns (a_8 array)
4. If a match is found, removes the matched ending using slice_del

The function implements a two-stage validation:
- First stage: Character-level check for i or j
- Second stage: Pattern matching against specific plural ending patterns

This approach ensures that only specific i/j-containing plural endings are removed, not just any occurrence of these characters.

## Parameters / Member Variables
- z: Pointer to SN_env structure containing string buffer, cursor positions, and processing state

## Dependencies
- Functions called/Symbols referenced:
  - [find_among_b](../f/find_among_b.md) (pattern matching for backward processing)
  - [slice_del](../s/slice_del.md) (removes matched substring)
- Called from (representative examples):
  - [finnish_ISO_8859_1_stem](../f/finnish_ISO_8859_1_stem.md) (main stemming function)
  - [finnish_UTF_8_stem](../f/finnish_UTF_8_stem.md) (UTF-8 variant of stemming function)

## Notes and Other Information
- Part of the Finnish Snowball stemmer algorithm for plural ending removal
- Uses a_8 array containing only 2 predefined plural patterns
- Operates within morphological boundaries (R2 region) to prevent over-stemming
- Character codes 105 and 106 correspond to ASCII i and j respectively
- The function is highly specific, targeting only i/j-based plural forms
- Returns 1 on successful processing, 0 on failure
- Located in stem_ISO_8859_1_finnish.c indicating ISO 8859-1 character encoding support
- This function handles a specific subset of Finnish plural morphology involving i and j characters