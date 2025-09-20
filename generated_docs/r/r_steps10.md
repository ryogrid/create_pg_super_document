# r_steps10

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_greek.c:2862-2879](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_greek.c#L2862-L2879)

## Overview
A static function that implements step 10 of the Greek language stemming algorithm, performing a two-stage suffix removal and replacement process.

## Definition
static int r_steps10(struct SN_env * z)

## Detailed Description
The r_steps10 function performs morphological transformations for step 10 in Greek stemming through a streamlined two-stage process:

1. Sets the cursor position (ket) to current position
2. Uses find_among_b to search backward through predefined suffix patterns (a_23 array with 4 entries)
3. If a match is found, deletes the matched suffix using slice_del
4. Resets counter I[0] = 0 
5. Performs a second pattern matching operation using a_22 array (7 entries)
6. Validates that the cursor is not beyond the left boundary
7. Replaces the matched pattern with a 6-character string (s_64)

This function follows a simpler structure compared to other steps, with mandatory two-stage processing rather than optional fallback operations.

## Parameters / Member Variables
- `z`: Pointer to SN_env structure containing the stemming environment, including:

## Dependencies
- Functions called/Symbols referenced:
  - [find_among_b](../f/find_among_b.md) (backward string pattern matching)
  - [slice_del](../s/slice_del.md) (suffix deletion)
  - [slice_from_s](../s/slice_from_s.md) (string replacement)
- Called from (representative examples):
  - [greek_UTF_8_stem](../g/greek_UTF_8_stem.md)

## Notes and Other Information
- Returns 1 on successful execution, 0 on no match, or negative values on errors
- Part of automatically generated Snowball stemmer code for Greek language
- Uses predefined arrays (a_22, a_23) and string constant (s_64)
- Implements a mandatory two-stage transformation without optional fallback paths
- Final step in the sequence, performing cleanup operations on remaining morphological patterns