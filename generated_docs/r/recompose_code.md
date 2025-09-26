# recompose_code

## Location
src/common/unicode_norm.c: 218 - 320

## Overview
Attempts to recompose two Unicode codepoints into a single composite character, handling both Hangul characters algorithmically and other characters through decomposition table lookup.

## Definition
```c
static bool recompose_code(uint32 start, uint32 code, uint32 *result)
```

## Detailed Description
This function implements Unicode recomposition by taking two codepoints and attempting to combine them into a single composite character. It handles two distinct cases:

1. **Hangul Characters**: Uses algorithmic recomposition following the Unicode specification to combine:
   - L (Leading consonant) + V (Vowel) → LV syllable
   - LV syllable + T (Trailing consonant) → LVT syllable

2. **Other Characters**: Performs inverse lookup in decomposition tables to find matching compositions. The backend uses a perfect hash function for efficient lookups, while the frontend uses linear search through the decomposition table.

The function ensures that only valid Unicode recomposition rules are applied and respects composition exclusions.

## Parameters / Member Variables
- `start`: The first Unicode codepoint (base character)
- `code`: The second Unicode codepoint to be combined with the first
- `result`: Pointer to store the resulting composite codepoint if recomposition succeeds

## Dependencies
- Functions called/Symbols referenced:
  - LBASE, LCOUNT, VBASE, VCOUNT, SBASE, SCOUNT, TBASE, TCOUNT (Hangul constants)
  - pg_unicode_decomposition (decomposition table structure)
  - pg_unicode_recompinfo (recomposition info structure, backend only)
  - pg_hton64 (byte order conversion, backend only)
  - UnicodeDecompMain (main decomposition table)
  - UnicodeDecomp_codepoints (codepoint array)
  - DECOMPOSITION_SIZE, DECOMPOSITION_NO_COMPOSE (macros for decomposition properties)
- Called from (representative examples):
  - unicode_normalize

## Notes and Other Information
- Returns `true` if successful recomposition occurs, `false` otherwise
- Backend implementation uses perfect hash function for O(1) lookup performance
- Frontend implementation uses linear search as fallback
- Follows Unicode Standard Annex #15 (Unicode Normalization Forms)
- Critical for implementing NFC (Normalization Form Composed) normalization
- Hangul syllable composition follows Unicode algorithmic rules for efficiency