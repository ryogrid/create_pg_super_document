# r_fix_chdz

## Location
src/backend/snowball/libstemmer/stem_UTF_8_lithuanian.c: 728 - 749

## Overview
The r_fix_chdz function handles specific Lithuanian character sequence corrections for 'ch' and 'dz' phonetic combinations that require normalization during stemming.

## Definition
```c
static int r_fix_chdz(struct SN_env * z)
```

## Detailed Description
This function addresses specific Lithuanian orthographic corrections for two important phonetic combinations: 'ch' and 'dz'. It performs an optimized character check by examining if the last character matches specific byte values (141 or 190 in UTF-8 encoding, likely representing Lithuanian-specific characters). When a match is found, it uses backward pattern matching against 2 predefined patterns (a_3 array) and replaces them with single-character corrections. This function is essential for proper Lithuanian word normalization, particularly for handling digraphs that need to be converted to their canonical forms.

## Parameters / Member Variables
- `z`: Pointer to the stemming environment structure (SN_env) containing the Lithuanian word being processed, cursor positions, and transformation state

## Dependencies
- Functions called/Symbols referenced:
  - [find_among_b](../f/find_among_b.md): Backward pattern matching function for 'ch'/'dz' pattern detection
  - [slice_from_s](../s/slice_from_s.md): String replacement function for applying single-character corrections
  - a_3: Array of 2 Lithuanian 'ch'/'dz' patterns
  - s_8, s_9: Single-character replacement constants for the corrections
- Called from (representative examples):
  - [lithuanian_UTF_8_stem](../l/lithuanian_UTF_8_stem.md): Main Lithuanian stemming function (called twice at different stages)

## Notes and Other Information
- Returns 1 on successful transformation, 0 if no matching patterns found or optimization checks fail
- Uses direct byte value comparison (141, 190) for fast character identification in UTF-8 encoding
- Handles exactly 2 specific phonetic correction cases, each replacing with single characters
- Requires minimum 1 character from left boundary for processing
- Called multiple times during Lithuanian stemming process (lines 807 and 819)
- Located in src/backend/snowball/libstemmer/stem_UTF_8_lithuanian.c:728-749
- Static function scope indicates internal use within the Lithuanian stemmer module
- Essential for Lithuanian phonetic normalization, particularly for digraph-to-monograph conversions