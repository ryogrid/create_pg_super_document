# r_post_process_last_consonants

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_turkish.c:1864-1894](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_turkish.c#L1864-L1894)

## Overview
Performs post-processing of last consonants in Turkish words by applying consonant transformations based on phonetic rules specific to Turkish language stemming.

## Definition

```c
}

static int r_post_process_last_consonants(struct SN_env * z)
```
## Detailed Description
This function is part of the Turkish Snowball stemmer that handles post-processing of final consonants in Turkish words. It uses backward matching to find specific consonant patterns at the end of words and replaces them according to Turkish phonological rules. The function works by:

1. Setting the cursor position (ket) to the current position
2. Using find_among_b to search for patterns in array a_23 (containing 'b', 'c', 'd', 'ğ')
3. Applying appropriate consonant transformations based on the matched pattern:
   - 'b' → 'p' (devoicing)
   - 'c' → 'ç' (devoicing)
   - 'd' → 't' (devoicing) 
   - 'ğ' → 'k' (devoicing)

These transformations follow Turkish consonant devoicing rules where voiced consonants become voiceless at word boundaries.

## Parameters / Member Variables
- `*z`: Pointer to the Snowball environment structure containing the string being processed and cursor positions
## Dependencies
- Functions called/Symbols referenced:
  - [find_among_b](../f/find_among_b.md) (Snowball library function for backward pattern matching)
  - [slice_from_s](../s/slice_from_s.md) (Snowball library function for string replacement)
- Called from:
  - [r_postlude](r_postlude.md) (at src/backend/snowball/libstemmer/stem_UTF_8_turkish.c:2058)

## Notes and Other Information
- This is a static function within the Turkish stemmer implementation
- Returns 1 on success, 0 if no pattern matches, or negative value on error
- Part of the final cleanup phase in Turkish word stemming
- Generated automatically by Snowball 2.2.0 stemmer generator

## Simplified Source

```c
static int r_post_process_last_consonants(struct SN_env * z) {
    z->ket = z->c;

    // Find Turkish consonants that need devoicing at word boundaries
    int among_var = find_among_b(z, a_23, 4);
    if (!among_var) return 0;

    z->bra = z->c;

    // Apply Turkish consonant devoicing rules
    switch (among_var) {
        case 1:  // 'b' → 'p' (devoicing)
            slice_from_s(z, 1, s_5);
            break;
        case 2:  // 'c' → 'ç' (devoicing)
            slice_from_s(z, 2, s_6);
            break;
        case 3:  // 'd' → 't' (devoicing)
            slice_from_s(z, 1, s_7);
            break;
        case 4:  // 'ğ' → 'k' (devoicing)
            slice_from_s(z, 1, s_8);
            break;
    }

    return 1;  // Successfully applied consonant transformation
}
```