# r_steps9

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_greek.c:2830-2861](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_greek.c#L2830-L2861)

## Overview
A static function that implements step 9 of the Greek language stemming algorithm, performing suffix pattern matching and replacement with specific character validation checks.

## Definition
static int r_steps9(struct SN_env * z)

## Detailed Description
The r_steps9 function performs morphological transformations for step 9 in Greek stemming with enhanced validation:

1. Sets the cursor position (ket) to current position
2. Performs bounds checking (minimum 7 characters from left boundary)
3. Validates the character at position c-1 using bit operations to ensure it meets specific criteria
4. Uses find_among_b to search backward through predefined suffix patterns (a_21 array with 3 entries)
5. If a match is found, deletes the matched suffix
6. Resets counter I[0] = 0
7. Attempts pattern matching with a_19 array (4 entries) and performs string replacement
8. As fallback, validates specific character values (181 or 189) and uses a_20 array (2 entries) for alternative replacement

The function includes sophisticated character validation using bitwise operations, indicating language-specific character set requirements for Greek text processing.

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
- Returns 1 on successful execution, 0 on no match or validation failure, negative on errors
- Part of automatically generated Snowball stemmer code for Greek language
- Uses bitwise validation (-1610481664 >> (z->p[z->c - 1] & 0x1f)) & 1 for character checking
- Implements Greek-specific character validation for UTF-8 encoded text
- Uses predefined arrays (a_19, a_20, a_21) and string constants (s_62, s_63)

## Simplified Source

```c
static int r_steps9(struct SN_env * z) {
    // Initial validation and pattern matching
    z->ket = z->c;

    // Check bounds (need at least 7 characters) and character properties
    if (z->c - 7 <= z->lb) return 0;
    if (!char_matches_greek_filter(z->p[z->c - 1])) return 0;

    // Find and delete suffix patterns from array a_21
    if (!(find_among_b(z, a_21, 3))) return 0;
    z->bra = z->c;
    slice_del(z);  // Delete matched suffix
    z->I[0] = 0;   // Reset counter

    // Save position for potential backtracking
    int saved_pos = z->l - z->c;

    // Try first replacement strategy with array a_19
    z->ket = z->c;
    z->bra = z->c;
    if (find_among_b(z, a_19, 4) && z->c <= z->lb) {
        slice_from_s(z, 4, s_62);  // Replace with s_62
        return 1;  // Success with first strategy
    }

    // Fallback: check for specific characters µ (181) or ½ (189)
    z->c = z->l - saved_pos;  // Restore position
    z->ket = z->c;
    z->bra = z->c;
    if (z->c - 1 <= z->lb) return 0;

    char last_char = z->p[z->c - 1];
    if (last_char != 181 && last_char != 189) return 0;

    if (!(find_among_b(z, a_20, 2))) return 0;
    slice_from_s(z, 4, s_63);  // Replace with s_63

    return 1;  // Success
}
```