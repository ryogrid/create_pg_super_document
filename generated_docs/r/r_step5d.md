# r_step5d

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_greek.c:3145-3175](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_greek.c#L3145-L3175)

## Overview
A static function in the Greek stemmer that performs step 5d of the Greek stemming algorithm, handling specific 6-character pattern substitutions in Greek words.

## Definition
```c
static int r_step5d(struct SN_env * z)
```

## Detailed Description
The r_step5d function is part of the Greek language stemming algorithm implementation in PostgreSQL's snowball stemmer library. This function performs step 5d of the Greek stemming process, which involves:

1. **Initial Pattern Matching**: Checks for specific Greek patterns ending with Unicode character 131, using the a_43 lookup table (2 entries)
2. **Pattern Deletion**: Removes the matched pattern from the word
3. **Conditional Replacement**: Performs one of two possible 6-character pattern substitutions:
   - First attempts to match pattern s_84 and replace with s_85
   - If the first pattern doesn't match, tries to match pattern s_86 and replace with s_87
   - If neither pattern matches, the function returns 0 (failure)

The function is more restrictive than previous steps, requiring exact pattern matches and performing direct substitutions rather than complex conditional logic.

## Parameters / Member Variables
- `z`: Pointer to SN_env structure containing:
  - `c`: Current cursor position in the string
  - `l`: Length of the string being processed  
  - `lb`: Left boundary for processing
  - `p`: Pointer to the string buffer
  - `ket`: End position marker for substring operations
  - `bra`: Start position marker for substring operations
  - `I[0]`: Integer array for storing intermediate results

## Dependencies
- Functions called/Symbols referenced:
  - [find_among_b](../f/find_among_b.md): Backward pattern matching function
  - [slice_del](../s/slice_del.md): Function to delete a substring slice  
  - [slice_from_s](../s/slice_from_s.md): Function to replace slice with specific string
  - [eq_s_b](../e/eq_s_b.md): Backward string equality check function (called twice)
- Called from (representative examples):
  - [greek_UTF_8_stem](../g/greek_UTF_8_stem.md): Main Greek stemming function

## Notes and Other Information
- This function is specific to Greek language morphology and handles UTF-8 encoded Greek text
- Uses a small lookup table (a_43 with only 2 entries), indicating it handles very specific Greek morphological patterns
- Implements a simpler two-branch conditional logic compared to previous steps
- Returns 1 on successful pattern matching and substitution, 0 if required patterns don't match, or negative values on error
- Part of the sequential Greek stemming pipeline, typically executed after steps 5a, 5b, and 5c
- The function performs direct 6-character to 6-character substitutions, maintaining word length in the replacement phase

## Simplified Source

```c
static int r_step5d(struct SN_env * z) {
    // Initial pattern matching with character validation
    z->ket = z->c;

    // Check minimum length and specific character (131)
    if (z->c - 9 <= z->lb || z->p[z->c - 1] != 131) return 0;

    // Find pattern from a_43 (2 patterns)
    if (!find_among_b(z, a_43, 2)) return 0;

    // Remove the matched pattern
    z->bra = z->c;
    slice_del(z);
    z->I[0] = 0;  // Reset state

    // Conditional replacement with backtracking
    int saved_pos = z->l - z->c;
    z->ket = z->c;
    z->bra = z->c;

    // Try first replacement pattern
    if (eq_s_b(z, 6, s_84) && z->c <= z->lb) {
        // Replace s_84 with s_85 (both 6 characters)
        slice_from_s(z, 6, s_85);
    } else {
        // Try alternative replacement pattern
        z->c = z->l - saved_pos;
        z->ket = z->c;
        z->bra = z->c;

        if (!eq_s_b(z, 6, s_86)) return 0;  // Must find s_86

        // Replace s_86 with s_87 (both 6 characters)
        slice_from_s(z, 6, s_87);
    }

    return 1;  // Success
}
```