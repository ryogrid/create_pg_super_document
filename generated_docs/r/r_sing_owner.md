# r_sing_owner

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_hungarian.c:739-767](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_hungarian.c#L739-L767)

## Overview
The r_sing_owner function handles singular possessor suffix removal for the Hungarian stemming algorithm in the Snowball stemming library.

## Definition
```c
static int r_sing_owner(struct SN_env * z)
```

## Detailed Description
The r_sing_owner function is responsible for detecting and removing Hungarian singular possessor suffixes during the stemming process. It operates by:

1. Setting the ket position to the current cursor position
2. Using find_among_b to match against a comprehensive set of 31 singular possessor suffix patterns (a_10 array)
3. Ensuring the match occurs within the R1 region
4. Performing appropriate transformations based on the matched pattern:
   - Case 1: Deletes the matched suffix
   - Case 2: Replaces with string s_10
   - Case 3: Replaces with string s_11

Unlike r_owned, this function does not perform preliminary character checking, instead relying on the extensive pattern matching in the a_10 array to identify valid singular possessor forms. The large number of patterns (31) reflects the complexity of Hungarian singular possessive morphology.

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
- It handles singular possessor forms which are distinct from general possessive forms handled by r_owned
- The function returns 1 on successful application, 0 if no match is found, and negative values on error
- The pattern matching uses the a_10 array which contains 31 different singular possessor suffix patterns
- Region checking ensures morphologically appropriate suffix removal
- The extensive pattern set reflects the rich morphological variation in Hungarian singular possessive forms

## Simplified Source

```c
static int r_sing_owner(struct SN_env * z) {
    // Set end position
    z->ket = z->c;

    // Find matching singular possessor suffix pattern from 31 patterns
    int among_var = find_among_b(z, a_10, 31);
    if (!among_var) return 0;

    // Set start position and verify in R1 region
    z->bra = z->c;
    if (r_R1(z) <= 0) return 0;

    // Apply transformation based on pattern type
    switch (among_var) {
        case 1: slice_del(z); break;              // Delete suffix
        case 2: slice_from_s(z, 1, s_10); break; // Replace with s_10
        case 3: slice_from_s(z, 1, s_11); break; // Replace with s_11
    }

    return 1;
}
```