# r_plur_owner

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_hungarian.c:768-797](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_hungarian.c#L768-L797)

## Overview
The r_plur_owner function handles plural possessor suffix removal for the Hungarian stemming algorithm in the Snowball stemming library.

## Definition
```c
static int r_plur_owner(struct SN_env * z)
```

## Detailed Description
The r_plur_owner function is responsible for detecting and removing Hungarian plural possessor suffixes during the stemming process. It operates by:

1. Setting the ket position to the current cursor position
2. Performing a complex character validation using bitwise operations to check if the character before the cursor matches specific patterns (the expression `z->p[z->c - 1] >> 5 != 3 || !((10768 >> (z->p[z->c - 1] & 0x1f)) & 1)` creates a character set filter)
3. Using find_among_b to match against an extensive set of 42 plural possessor suffix patterns (a_11 array)
4. Ensuring the match occurs within the R1 region
5. Performing appropriate transformations based on the matched pattern:
   - Case 1: Deletes the matched suffix
   - Case 2: Replaces with string s_12
   - Case 3: Replaces with string s_13

The sophisticated character checking mechanism uses bitwise operations to efficiently test for multiple possible ending characters that are characteristic of Hungarian plural possessor forms, making it more efficient than multiple individual character comparisons.

## Parameters / Member Variables
- `z`: Pointer to the Snowball environment structure containing the word being stemmed, cursor positions, and other stemming state

## Dependencies
- Functions called/Symbols referenced:
  - [r_R1](r_R1.md) (region boundary test function)
  - [find_among_b](../f/find_among_b.md) (backward pattern matching function)
  - [slice_from_s](../s/slice_from_s.md) (string replacement function)
  - [slice_del](../s/slice_del.md) (deletion function)
- Called from (representative examples):
  - [hungarian_ISO_8859_2_stem](../h/hungarian_ISO_8859_2_stem.md)
  - [hungarian_UTF_8_stem](../h/hungarian_UTF_8_stem.md)

## Notes and Other Information
- This function is part of the Hungarian stemming algorithm implementation
- It handles plural possessor forms which are the most complex category of Hungarian possessive morphology
- The function returns 1 on successful application, 0 if no match is found, and negative values on error
- The pattern matching uses the a_11 array which contains 42 different plural possessor suffix patterns - the largest set among all possessive functions
- The bitwise character validation (using constant 10768) efficiently filters for valid ending characters
- Region checking ensures morphologically appropriate suffix removal
- The extensive pattern set and complex character checking reflect the rich morphological complexity of Hungarian plural possessive forms

## Simplified Source

```c
static int r_plur_owner(struct SN_env * z) {
    // Set end position and validate character using bitwise check
    z->ket = z->c;
    if (z->c <= z->lb || z->p[z->c - 1] >> 5 != 3 ||
        !((10768 >> (z->p[z->c - 1] & 0x1f)) & 1)) return 0;

    // Find matching plural possessor suffix pattern from 42 patterns
    int among_var = find_among_b(z, a_11, 42);
    if (!among_var) return 0;

    // Set start position and verify in R1 region
    z->bra = z->c;
    if (r_R1(z) <= 0) return 0;

    // Apply transformation based on pattern type
    switch (among_var) {
        case 1: slice_del(z); break;              // Delete suffix
        case 2: slice_from_s(z, 1, s_12); break; // Replace with s_12
        case 3: slice_from_s(z, 1, s_13); break; // Replace with s_13
    }

    return 1;
}
```