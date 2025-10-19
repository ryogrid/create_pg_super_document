# r_plural

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_hungarian.c:679-708](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_hungarian.c#L679-L708)

## Overview
The r_plural function handles plural suffix removal for the Hungarian stemming algorithm in the Snowball stemming library.

## Definition

```c
}

static int r_plural(struct SN_env * z)
```
## Detailed Description
The r_plural function is responsible for detecting and removing Hungarian plural suffixes during the stemming process. It operates by:

1. Setting the ket position to the current cursor position
2. Checking if the character before the cursor is 'k' (ASCII 107)
3. Using find_among_b to match against a set of 7 plural suffix patterns (a_8 array)
4. Ensuring the match occurs within the R1 region
5. Performing appropriate transformations based on the matched pattern:
   - Case 1: Replaces with string s_6
   - Case 2: Replaces with string s_7  
   - Case 3: Deletes the matched suffix

The function ensures that plural suffix removal only occurs in appropriate morphological contexts by requiring matches to be within the R1 region, which represents the main stem portion of the word.

## Parameters / Member Variables
- `*z`: Pointer to the Snowball environment structure containing the word being stemmed, cursor positions, and other stemming state
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
- It specifically targets plural forms by looking for the 'k' character pattern typical in Hungarian plurals
- The function returns 1 on successful application, 0 if no match is found, and negative values on error
- The pattern matching uses the a_8 array which contains 7 different plural suffix patterns
- Region checking ensures morphologically appropriate suffix removal

## Simplified Source

```c
static int r_plural(struct SN_env * z) {
    // Set end position and check for 'k' character
    z->ket = z->c;
    if (z->c <= z->lb || z->p[z->c - 1] != 'k') return 0;

    // Find matching plural suffix pattern
    int among_var = find_among_b(z, a_8, 7);
    if (!among_var) return 0;

    // Set start position and verify in R1 region
    z->bra = z->c;
    if (r_R1(z) <= 0) return 0;

    // Apply transformation based on pattern type
    switch (among_var) {
        case 1: slice_from_s(z, 1, s_6); break;  // Replace with s_6
        case 2: slice_from_s(z, 1, s_7); break;  // Replace with s_7
        case 3: slice_del(z); break;             // Delete suffix
    }

    return 1;
}
```