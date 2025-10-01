# get_code_decomposition

## Location
[src/common/unicode_norm.c:134-158](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/unicode_norm.c#L134-L158)

## Overview
Retrieves the actual decomposed characters for a Unicode codepoint from a decomposition entry, handling both inline and external decomposition storage.

## Definition
```c
static const pg_wchar * get_code_decomposition(const pg_unicode_decomposition *entry, int *dec_size)
```

## Detailed Description
`get_code_decomposition` extracts the actual decomposed character sequence from a decomposition table entry. The function handles two different storage formats for efficiency:

1. **Inline decomposition**: For single-character decompositions, the character value is stored directly in the `dec_index` field of the entry. This optimization avoids the need for external storage for the most common case.

2. **External decomposition**: For multi-character decompositions, the `dec_index` field contains an index into the `UnicodeDecomp_codepoints` array where the actual decomposition sequence is stored.

The function returns a pointer to the decomposition sequence and sets the `dec_size` parameter to indicate the number of characters in the decomposition.

## Parameters / Member Variables
- `entry`: Pointer to the pg_unicode_decomposition entry containing decomposition information
- `dec_size`: Output parameter set to the number of characters in the decomposition sequence

## Dependencies
- Functions called/Symbols referenced:
  - DECOMPOSITION_IS_INLINE (macro)
  - DECOMPOSITION_SIZE (macro)  
  - UnicodeDecomp_codepoints (global array)
  - [pg_unicode_decomposition](../p/pg_unicode_decomposition.md) (structure type)
- Called from (representative examples):
  - [get_decomposed_size](get_decomposed_size.md)
  - [decompose_code](../d/decompose_code.md)

## Notes and Other Information
- This is a static function, accessible only within unicode_norm.c
- Uses a static variable `x` for inline decompositions - the returned pointer is only valid until the next call
- The DECOMPOSITION_IS_INLINE macro determines whether to use inline or external storage
- For inline decompositions, the function asserts that the size is exactly 1 character
- Returns a pointer to either the static variable `x` or the external decomposition array
- Critical for Unicode normalization as it provides the actual replacement characters for composed forms

## Simplified Source

```c
static const pg_wchar *
get_code_decomposition(const pg_unicode_decomposition *entry, int *dec_size)
{
    static pg_wchar x;

    if (DECOMPOSITION_IS_INLINE(entry))
    {
        // Single character stored inline
        x = (pg_wchar) entry->dec_index;
        *dec_size = 1;
        return &x;
    }
    else
    {
        // Multi-character stored externally
        *dec_size = DECOMPOSITION_SIZE(entry);
        return &UnicodeDecomp_codepoints[entry->dec_index];
    }
}
```