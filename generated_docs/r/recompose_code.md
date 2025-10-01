# recompose_code

## Location
[src/common/unicode_norm.c:218-320](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/unicode_norm.c#L218-L320)

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
  - [pg_unicode_decomposition](../p/pg_unicode_decomposition.md) (decomposition table structure)
  - pg_unicode_recompinfo (recomposition info structure, backend only)
  - pg_hton64 (byte order conversion, backend only)
  - UnicodeDecompMain (main decomposition table)
  - UnicodeDecomp_codepoints (codepoint array)
  - DECOMPOSITION_SIZE, DECOMPOSITION_NO_COMPOSE (macros for decomposition properties)
- Called from (representative examples):
  - [unicode_normalize](../u/unicode_normalize.md)

## Notes and Other Information
- Returns `true` if successful recomposition occurs, `false` otherwise
- [Backend](../B/Backend.md) implementation uses perfect hash function for O(1) lookup performance
- Frontend implementation uses linear search as fallback
- Follows Unicode Standard Annex #15 (Unicode Normalization Forms)
- Critical for implementing NFC (Normalization Form Composed) normalization
- Hangul syllable composition follows Unicode algorithmic rules for efficiency

## Simplified Source

```c
static bool
recompose_code(uint32 start, uint32 code, uint32 *result)
{
    // Hangul L + V → LV syllable
    if (start >= LBASE && start < LBASE + LCOUNT &&
        code >= VBASE && code < VBASE + VCOUNT) {
        uint32 lindex = start - LBASE;
        uint32 vindex = code - VBASE;
        *result = SBASE + (lindex * VCOUNT + vindex) * TCOUNT;
        return true;
    }

    // Hangul LV + T → LVT syllable
    if (start >= SBASE && start < (SBASE + SCOUNT) &&
        ((start - SBASE) % TCOUNT) == 0 &&
        code >= TBASE && code < (TBASE + TCOUNT)) {
        *result = start + (code - TBASE);
        return true;
    }

    // For other characters, lookup in decomposition table
#ifndef FRONTEND
    // Backend: Use perfect hash for efficient lookup
    uint64 hashkey = pg_hton64(((uint64) start << 32) | (uint64) code);
    pg_unicode_recompinfo recompinfo = UnicodeRecompInfo;
    int h = recompinfo.hash(&hashkey);

    if (h >= 0 && h < recompinfo.num_recomps) {
        const pg_unicode_decomposition *entry =
            &UnicodeDecompMain[recompinfo.inverse_lookup[h]];

        if (start == UnicodeDecomp_codepoints[entry->dec_index] &&
            code == UnicodeDecomp_codepoints[entry->dec_index + 1]) {
            *result = entry->codepoint;
            return true;
        }
    }
#else
    // Frontend: Linear search through decomposition table
    for (int i = 0; i < lengthof(UnicodeDecompMain); i++) {
        const pg_unicode_decomposition *entry = &UnicodeDecompMain[i];

        if (DECOMPOSITION_SIZE(entry) != 2 || DECOMPOSITION_NO_COMPOSE(entry))
            continue;

        if (start == UnicodeDecomp_codepoints[entry->dec_index] &&
            code == UnicodeDecomp_codepoints[entry->dec_index + 1]) {
            *result = entry->codepoint;
            return true;
        }
    }
#endif

    return false;  // No recomposition possible
}
```