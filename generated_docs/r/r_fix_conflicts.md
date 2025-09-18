# r_fix_conflicts

## Location
src/backend/snowball/libstemmer/stem_UTF_8_lithuanian.c: 676 - 727

## Overview
The r_fix_conflicts function resolves morphological conflicts in Lithuanian word stemming by applying specific character sequence corrections to handle phonetic variations and orthographic inconsistencies.

## Definition
```c
static int r_fix_conflicts(struct SN_env * z)
```

## Detailed Description
This function addresses morphological conflicts that arise during Lithuanian stemming by identifying and correcting specific problematic character sequences. It first performs an optimized character check using bit manipulation (checking if the last character has specific properties via a bitmask operation on 2621472) before attempting pattern matching. The function uses backward pattern matching against 11 predefined conflict patterns (a_2 array) and replaces matched sequences with their corrected forms. Each case handles a different type of morphological conflict with replacement strings of varying lengths (4, 5, 6, or 7 characters).

## Parameters / Member Variables
- `z`: Pointer to the stemming environment structure (SN_env) containing the Lithuanian word being processed, cursor positions, and transformation state

## Dependencies
- Functions called/Symbols referenced:
  - [find_among_b](../f/find_among_b.md): Backward pattern matching function for conflict pattern detection
  - [slice_from_s](../s/slice_from_s.md): String replacement function for applying corrections
  - a_2: Array of 11 Lithuanian conflict patterns
  - s_0 through s_7: Replacement string constants for conflict resolution
- Called from (representative examples):
  - [lithuanian_UTF_8_stem](../l/lithuanian_UTF_8_stem.md): Main Lithuanian stemming function

## Notes and Other Information
- Returns 1 on successful transformation, 0 if no conflicts found or optimization checks fail
- Uses bit manipulation optimization to quickly eliminate non-matching cases before expensive pattern matching
- Handles 8 distinct conflict resolution cases with different replacement strategies
- The bit operation (2621472 >> (z->p[z->c - 1] & 0x1f)) & 1 provides fast character class checking
- Requires minimum 3 characters from left boundary for processing
- Located in src/backend/snowball/libstemmer/stem_UTF_8_lithuanian.c:676-727
- Static function scope indicates internal use within the Lithuanian stemmer module
- Essential for handling Lithuanian-specific orthographic and phonetic variations that occur after suffix removal