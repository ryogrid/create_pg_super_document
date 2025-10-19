# r_v_ending

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_hungarian.c:517-541](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_hungarian.c#L517-L541)

## Overview
The r_v_ending function handles specific vowel endings in Hungarian words, removing or transforming certain accented vowel suffixes as part of the Hungarian stemming algorithm.

## Definition
static int r_v_ending(struct SN_env * z)

## Detailed Description
This function processes Hungarian vowel endings by identifying and transforming specific accented characters (á and é, represented as characters 225 and 233 respectively in ISO-8859-2 encoding). It operates backwards from the current cursor position to find matching patterns from the a_1 suffix array containing 2 elements.

The function performs the following operations:
1. Sets the ket (end marker) to the current cursor position
2. Checks if the character before the cursor is either 'á' (225) or 'é' (233)
3. Uses find_among_b to match against predefined suffixes in array a_1
4. Verifies that the operation is within the R1 region using r_R1
5. Replaces the matched suffix with appropriate substitute strings (s_0 or s_1)

This is specifically designed for Hungarian morphology where certain vowel endings need to be normalized during stemming.

## Parameters / Member Variables
- : Pointer to SN_env structure containing:
  - : Current cursor position in the word
  - : End position marker for pattern matching
  - : Start position marker for pattern matching  
  - : Character array containing the word being processed
  - : Lower boundary limit

## Dependencies
- Functions called/Symbols referenced:
  - [r_R1](r_R1.md): Tests if current position is within R1 region
  - [find_among_b](../f/find_among_b.md): Backward suffix matching function
  - [slice_from_s](../s/slice_from_s.md): String replacement function
  - a_1: Suffix array containing 2 vowel ending patterns
  - s_0, s_1: Replacement strings for the matched patterns
- Called from (representative examples):
  - [r_case](r_case.md) (Hungarian case suffix processing)

## Notes and Other Information
- This function is specific to Hungarian stemming and handles accented vowel transformations
- The function checks for ISO-8859-2 encoded characters 225 (á) and 233 (é)
- Uses backward matching (find_among_b) to process suffixes from right to left
- Requires R1 region validation to prevent over-stemming
- Returns 1 on successful transformation, 0 if no match found
- Part of the Hungarian morphological analysis system in PostgreSQL's full-text search

## Simplified Source

```c
static int r_v_ending(struct SN_env * z) {
    // Set end marker and check for Hungarian accented vowels (á=225, é=233)
    z->ket = z->c;
    if (z->c <= z->lb || (z->p[z->c - 1] != 225 && z->p[z->c - 1] != 233))
        return 0;

    // Find matching vowel ending pattern from predefined array
    int among_var = find_among_b(z, a_1, 2);
    if (!among_var) return 0;

    // Set start marker and verify we're in R1 region
    z->bra = z->c;
    if (r_R1(z) <= 0) return 0;

    // Replace with appropriate substitute based on pattern match
    switch (among_var) {
        case 1: slice_from_s(z, 1, s_0); break;
        case 2: slice_from_s(z, 1, s_1); break;
    }

    return 1;
}
```