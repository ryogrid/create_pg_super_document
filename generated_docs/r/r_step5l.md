# r_step5l

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_greek.c:3391-3409](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_greek.c#L3391-L3409)

## Overview
A static function in the Greek stemmer that performs step 5l of the Greek language stemming algorithm, handling specific suffix transformations for Greek words ending in certain patterns.

## Definition
```c
static int r_step5l(struct SN_env * z)
```

## Detailed Description
This function is part of the Snowball Greek stemming algorithm implementation. It performs step 5l of the stemming process, which involves:

1. Setting the cursor position and checking for specific suffix patterns
2. Looking for suffixes from the a_61 array (Greek suffixes like ουνε, ησουνε, ηθουνε)  
3. Deleting the matched suffix if found
4. Resetting the step counter (I[0] = 0)
5. Finding patterns from the a_62 array and replacing them with the Greek suffix "ουν" (s_104)

The function uses backward searching to find suffixes and performs deletion and replacement operations on the word being stemmed.

## Parameters / Member Variables
- `z`: Pointer to the SN_env structure containing:
  - `ket`: End position marker for substring operations
  - `bra`: Start position marker for substring operations  
  - `c`: Current cursor position
  - `lb`: Left boundary limit
  - `p`: Pointer to the string being processed
  - `I[0]`: Step counter/flag

## Dependencies
- Functions called/Symbols referenced:
  - [find_among_b](../f/find_among_b.md) (backward suffix matching)
  - [slice_del](../s/slice_del.md) (delete substring)
  - [slice_from_s](../s/slice_from_s.md) (insert substring)
- Arrays used:
  - a_61 (3 Greek suffixes: ουνε, ησουνε, ηθουνε)
  - a_62 (6 Greek word patterns)
  - s_104 (Greek suffix "ουν" replacement)
- Called from:
  - [greek_UTF_8_stem](../g/greek_UTF_8_stem.md) at src/backend/snowball/libstemmer/stem_UTF_8_greek.c:3643

## Notes and Other Information
- Returns 1 on successful processing, 0 if no transformation was applied
- Part of a multi-step Greek stemming algorithm
- Handles UTF-8 encoded Greek text with specific byte patterns
- The function checks for a minimum word length (7 characters) and specific ending patterns before processing
- Byte value 181 corresponds to part of a Greek UTF-8 character sequence

## Simplified Source

```c
static int r_step5l(struct SN_env * z) {
    // Step 1: Preliminary validation - length and character checks
    z->ket = z->c;

    // Ensure minimum length (7 chars) and specific character (code 181)
    if (z->c - 7 <= z->lb || z->p[z->c - 1] != 181) return 0;

    // Step 2: Find and remove Greek suffix from a_61 (3 entries)
    if (!find_among_b(z, a_61, 3)) return 0;
    z->bra = z->c;
    slice_del(z);  // Remove found suffix

    // Reset stemmer state
    z->I[0] = 0;

    // Step 3: Pattern matching and replacement with Greek suffix "ουν"
    z->ket = z->c;
    z->bra = z->c;
    if (!find_among_b(z, a_62, 6)) return 0;
    if (z->c > z->lb) return 0;  // Boundary check

    slice_from_s(z, 6, s_104);   // Replace with 6-char Greek suffix s_104
    return 1;
}
```