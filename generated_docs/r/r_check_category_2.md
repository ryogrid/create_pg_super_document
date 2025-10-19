# r_check_category_2

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_nepali.c:319-326](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_nepali.c#L319-L326)

## Overview
Checks for the presence of category 2 suffix patterns in Nepali words during the stemming process.

## Definition
```c
static int r_check_category_2(struct SN_env * z)
```

## Detailed Description
This function performs a conditional check for category 2 suffixes in the Nepali stemming algorithm. It first validates that there are sufficient characters available for processing by checking the cursor position against the left boundary. The function then performs a bit-mask operation on the character at position c-1 to verify it matches expected UTF-8 encoding patterns for Nepali characters.

The validation includes:
1. Boundary check: ensures c-2 > lb (at least 2 characters from left boundary)
2. UTF-8 encoding check: verifies the character's high bits match expected patterns
3. Bitmask check: uses value 262 with bit operations to validate character properties
4. Pattern matching: searches for matches in the a_1 suffix array containing 3 patterns

If all conditions are met, it sets the boundary markers (ket and bra) and returns success.

## Parameters / Member Variables
- `z`: Pointer to the SN_env structure containing the stemming environment with string data, cursor positions, and boundary markers

## Dependencies
- Functions called/Symbols referenced:
  - [find_among_b](../f/find_among_b.md) (for pattern matching in suffix array a_1)
- Called from:
  - [nepali_UTF_8_stem](../n/nepali_UTF_8_stem.md) (main stemming function)

## Notes and Other Information
- This is a static function, only accessible within the Nepali stemmer module  
- Returns 1 if category 2 patterns are found and conditions met, 0 otherwise
- The bit manipulation (262 >> (z->p[z->c - 1] & 0x1f)) & 1) is used for efficient character classification in UTF-8 Nepali text
- Used as a precondition check before attempting category 2 suffix removal operations
- Part of the automatically generated code from Snowball stemming rules

## Simplified Source

```c
static int r_check_category_2(struct SN_env * z) {
    // Set boundary for pattern matching
    z->ket = z->c;

    // Perform multiple validation checks before pattern matching:

    // 1. Boundary check: ensure at least 2 characters from left boundary
    if (z->c - 2 <= z->lb) {
        return 0;
    }

    // 2. UTF-8 encoding check: verify character at c-1 has expected high bits (4)
    if (z->p[z->c - 1] >> 5 != 4) {
        return 0;
    }

    // 3. Bitmask check: validate character properties using bit manipulation
    if (!((262 >> (z->p[z->c - 1] & 0x1f)) & 1)) {
        return 0;
    }

    // 4. Pattern matching: look for category 2 patterns (3 patterns in a_1 array)
    if (!find_among_b(z, a_1, 3)) {
        return 0;
    }

    // All checks passed - set deletion boundary and return success
    z->bra = z->c;
    return 1;
}
```