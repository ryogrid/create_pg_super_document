# r_VI

## Location
src/backend/snowball/libstemmer/stem_UTF_8_finnish.c: 403 - 409

## Overview
The r_VI function is a predicate function in the Finnish stemmer that checks for vowel-i patterns by detecting if the current position has an i preceded by a vowel from the V2 vowel group.

## Definition
static int r_VI(struct SN_env * z)

## Detailed Description
This function implements a backward matching rule (indicated by the _b suffix in the called function) for Finnish morphological analysis. It checks whether the current cursor position in the string buffer points to an i character that is preceded by a vowel from the V2 vowel group. The function moves the cursor backward during the check and returns 1 if the pattern matches, 0 otherwise.

The function first verifies that:
1. The cursor is not at or before the left boundary (z->lb)
2. The character at the current position minus one is i

If these conditions are met, it decrements the cursor and checks if the previous character is NOT in the V2 vowel grouping (characters 97-246, corresponding to a through various accented characters). The logic uses negation - it returns 0 (failure) if the character IS in the grouping, and 1 (success) if it is NOT in the grouping.

## Parameters / Member Variables
- z: Pointer to SN_env structure containing the string buffer, cursor position (c), and left boundary (lb)

## Dependencies
- Functions called/Symbols referenced:
  - in_grouping_b (utility function for backward character group matching)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- This is part of the Finnish language stemming algorithm implementation in the Snowball stemmer
- The function follows the Snowball algorithm convention for backward pattern matching
- The V2 vowel group (97-246) covers the ASCII range from a through extended Latin characters with diacritics
- The function modifies the cursor position (z->c) as a side effect when the pattern matches
- Located in stem_ISO_8859_1_finnish.c, indicating it is part of the ISO 8859-1 (Latin-1) character encoding variant of the Finnish stemmer