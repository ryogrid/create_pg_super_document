# get_decomposed_size

## Location
[src/common/unicode_norm.c:159-217](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/unicode_norm.c#L159-L217)

## Overview
Calculates the total number of characters that a given Unicode character will expand to after complete recursive decomposition.

## Definition
```c
static int get_decomposed_size(pg_wchar code, bool compat)
```

## Detailed Description
`get_decomposed_size` computes the final character count that results from fully decomposing a Unicode character. This function is essential for memory allocation during Unicode normalization, as it determines how much space is needed to store the normalized string.

The function handles several cases:

1. **Hangul characters**: Uses algorithmic decomposition for efficiency. Hangul syllables decompose into 2 or 3 characters depending on whether they have a trailing consonant (tindex != 0).

2. **Regular characters**: Looks up the character in the decomposition table and recursively calculates the size of each decomposed component, since decomposed characters may themselves be decomposable.

3. **Compatibility vs. canonical**: The `compat` parameter determines whether to include compatibility decompositions (NFKC/NFKD forms) or only canonical decompositions (NFC/NFD forms).

The function returns 1 for characters that cannot be decomposed further or have no valid decomposition for the specified form.

## Parameters / Member Variables
- `code`: The Unicode codepoint (pg_wchar) to calculate decomposition size for
- `compat`: Boolean flag indicating whether to include compatibility decompositions (true) or only canonical ones (false)

## Dependencies
- Functions called/Symbols referenced:
  - [get_code_entry](get_code_entry.md)
  - [get_code_decomposition](get_code_decomposition.md)
  - [get_decomposed_size](get_decomposed_size.md) (recursive call)
  - SBASE, SCOUNT, TCOUNT (Hangul constants)
  - DECOMPOSITION_SIZE (macro)
  - DECOMPOSITION_IS_COMPAT (macro)
  - [pg_unicode_decomposition](../p/pg_unicode_decomposition.md) (structure type)
- Called from (representative examples):
  - [get_decomposed_size](get_decomposed_size.md) (recursive calls)
  - [unicode_normalize](../u/unicode_normalize.md)

## Notes and Other Information
- This is a static function, accessible only within unicode_norm.c
- Uses recursion to handle multi-level decompositions (characters that decompose into decomposable characters)
- Optimized fast path for Hangul characters to avoid table lookup
- Hangul syllables (U+AC00-U+D7A3) decompose algorithmically into 2-3 Jamo characters
- Returns 1 for characters with no decomposition or when compatibility decompositions are excluded
- Essential for pre-calculating buffer sizes during Unicode normalization to avoid memory allocation errors

## Simplified Source

```c
static int
get_decomposed_size(pg_wchar code, bool compat)
{
    // Fast path: Hangul characters (algorithmic calculation)
    if (code >= SBASE && code < SBASE + SCOUNT) {
        uint32 sindex = code - SBASE;
        uint32 tindex = sindex % TCOUNT;

        // Return 3 if has trailing consonant, otherwise 2
        return (tindex != 0) ? 3 : 2;
    }

    // Look up character in decomposition table
    const pg_unicode_decomposition *entry = get_code_entry(code);

    // No decomposition available - character counts as 1
    if (entry == NULL || DECOMPOSITION_SIZE(entry) == 0 ||
        (!compat && DECOMPOSITION_IS_COMPAT(entry)))
        return 1;

    // Recursively calculate size of all decomposed components
    int size = 0;
    int dec_size;
    const uint32 *decomp = get_code_decomposition(entry, &dec_size);

    for (int i = 0; i < dec_size; i++) {
        size += get_decomposed_size(decomp[i], compat);
    }

    return size;
}
```