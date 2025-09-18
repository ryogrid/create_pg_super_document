# r_t_plural

## Location
src/backend/snowball/libstemmer/stem_UTF_8_finnish.c: 527 - 572

## Overview
The r_t_plural function identifies and removes Finnish plural endings that involve the letter t followed by vowel-containing patterns in the stemming process.

## Definition
static int r_t_plural(struct SN_env * z)

## Detailed Description
This function handles the removal of Finnish plural markers that contain the character t followed by specific vowel patterns. It operates in two distinct phases with different morphological boundaries and implements a complex validation process.

The function follows this two-phase process:

**Phase 1: t-ending removal (R2 region)**
1. Sets up processing boundaries using z->I[1] (likely the R2 region) as the left limit
2. Checks if the character immediately before the cursor is t
3. Moves the cursor back past the t
4. Validates that the character before t is a vowel (using g_V1 group, ASCII 97-246)
5. If validation passes, removes the t using slice_del

**Phase 2: Vowel-ending pattern matching (R1 region)**
1. Sets up new processing boundaries using z->I[0] (likely the R1 region) as the left limit
2. Checks if the character immediately before the cursor is a (ASCII 97)
3. Uses find_among_b to match against 2 predefined patterns (a_9 array)
4. For case 1, performs an exclusion check using eq_s_b with s_4 (a 2-character string)
5. If the exclusion check matches, returns without removal
6. Otherwise, removes the matched ending using slice_del

This two-phase approach ensures proper handling of complex t-plural forms that require both consonant and vowel validation.

## Parameters / Member Variables
- z: Pointer to SN_env structure containing string buffer, cursor positions, and processing state

## Dependencies
- Functions called/Symbols referenced:
  - in_grouping_b (character group membership check for backward processing)
  - find_among_b (pattern matching for backward processing)
  - eq_s_b (string equality check for backward processing)
  - slice_del (removes matched substring)
- Called from (representative examples):
  - finnish_ISO_8859_1_stem (main stemming function)
  - finnish_UTF_8_stem (UTF-8 variant of stemming function)

## Notes and Other Information
- Part of the Finnish Snowball stemmer algorithm for t-based plural ending removal
- Uses dual-phase processing with different morphological boundary regions (R1 and R2)
- Phase 1 operates in R2 region for t removal with vowel validation
- Phase 2 operates in R1 region for a-ending pattern matching using a_9 array (2 patterns)
- Character code 97 corresponds to ASCII a, and 116 would be ASCII t
- Includes exclusion logic in case 1 to prevent removal of certain patterns (s_4)
- Returns 1 on successful processing, 0 on failure or exclusion
- Located in stem_ISO_8859_1_finnish.c indicating ISO 8859-1 character encoding support
- This function handles complex Finnish plural morphology involving consonant-vowel sequences