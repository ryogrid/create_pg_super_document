# decompose_code

## Location
[src/common/unicode_norm.c:321-401](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/unicode_norm.c#L321-L401)

## Overview
Recursively decomposes a Unicode codepoint into its constituent base characters, handling both Hangul characters algorithmically and other characters through decomposition table lookup.

## Definition
```c
static void decompose_code(pg_wchar code, bool compat, pg_wchar **result, int *current)
```

## Detailed Description
This function performs Unicode decomposition by breaking down a composite character into its component parts. It operates recursively to handle multi-level decompositions and supports two types of decomposition:

1. **Hangul Characters**: Uses algorithmic decomposition following Unicode specification to break down:
   - LVT syllables → L (leading) + V (vowel) + T (trailing) components
   - LV syllables → L (leading) + V (vowel) components

2. **Other Characters**: Looks up decomposition sequences in Unicode tables and recursively decomposes each component until base characters are reached.

The function supports both canonical and compatibility decomposition modes, where compatibility mode includes additional decompositions for formatting characters.

## Parameters / Member Variables
- `code`: The Unicode codepoint to be decomposed
- `compat`: Boolean flag indicating whether to perform compatibility decomposition (true) or canonical only (false)
- `result`: Pointer to the output array where decomposed characters will be stored
- `current`: Pointer to the current position in the result array, updated as characters are added

## Dependencies
- Functions called/Symbols referenced:
  - SBASE, SCOUNT, LBASE, VBASE, TBASE, VCOUNT, TCOUNT (Hangul constants)
  - [pg_unicode_decomposition](../p/pg_unicode_decomposition.md) (decomposition table structure)
  - [get_code_entry](../g/get_code_entry.md) (retrieves decomposition entry for a codepoint)
  - [get_code_decomposition](../g/get_code_decomposition.md) (retrieves decomposition sequence)
  - DECOMPOSITION_SIZE, DECOMPOSITION_IS_COMPAT (macros for decomposition properties)
  - [decompose_code](decompose_code.md) (recursive self-call)
- Called from (representative examples):
  - [unicode_normalize](../u/unicode_normalize.md)
  - [decompose_code](decompose_code.md) (recursive calls)

## Notes and Other Information
- Performs recursive decomposition until base characters are reached
- Updates the `current` position pointer to track array filling progress
- Hangul decomposition is algorithmic for memory efficiency
- Returns early for characters with no decomposition or when compatibility is disabled for compat-only decompositions
- Critical for implementing NFD (Normalization Form Decomposed) and NFKD (Normalization Form Compatibility Decomposed)
- Follows Unicode Standard Annex #15 decomposition rules

## Simplified Source

```c
static void
decompose_code(pg_wchar code, bool compat, pg_wchar **result, int *current)
{
    // Fast path: Hangul characters (algorithmic decomposition)
    if (code >= SBASE && code < SBASE + SCOUNT) {
        pg_wchar *res = *result;
        uint32 sindex = code - SBASE;

        // Calculate L, V, T components
        uint32 l = LBASE + sindex / (VCOUNT * TCOUNT);
        uint32 v = VBASE + (sindex % (VCOUNT * TCOUNT)) / TCOUNT;
        uint32 tindex = sindex % TCOUNT;

        // Store L and V components
        res[(*current)++] = l;
        res[(*current)++] = v;

        // Store T component if present
        if (tindex != 0)
            res[(*current)++] = TBASE + tindex;
        return;
    }

    // Get decomposition table entry
    const pg_unicode_decomposition *entry = get_code_entry(code);

    // No decomposition available - store original character
    if (entry == NULL || DECOMPOSITION_SIZE(entry) == 0 ||
        (!compat && DECOMPOSITION_IS_COMPAT(entry))) {
        (*result)[(*current)++] = code;
        return;
    }

    // Recursively decompose each component
    int dec_size;
    const uint32 *decomp = get_code_decomposition(entry, &dec_size);

    for (int i = 0; i < dec_size; i++) {
        decompose_code((pg_wchar) decomp[i], compat, result, current);
    }
}
```