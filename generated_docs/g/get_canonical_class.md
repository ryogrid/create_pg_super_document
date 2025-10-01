# get_canonical_class

## Location
[src/common/unicode_norm.c:112-133](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/unicode_norm.c#L112-L133)

## Overview
Retrieves the Unicode combining class value for a given codepoint, which is used to determine the canonical ordering of characters during normalization.

## Definition
```c
static uint8 get_canonical_class(pg_wchar code)
```

## Detailed Description
`get_canonical_class` extracts the combining class property of a Unicode character, which is essential for Unicode normalization algorithms. The combining class is a numeric value that determines how combining marks should be ordered when multiple marks are applied to the same base character.

The function first attempts to locate the character in the decomposition table using `get_code_entry()`. If an entry is found, it returns the combining class stored in the entry. If no entry exists, the function returns 0, indicating that the character either has no combining class (such as Hangul characters) or is a base character.

Combining classes range from 0 to 255, with 0 indicating a base character and higher values indicating different types of combining marks that must be ordered canonically.

## Parameters / Member Variables
- `code`: The Unicode codepoint (pg_wchar) for which to retrieve the combining class

## Dependencies
- Functions called/Symbols referenced:
  - [get_code_entry](get_code_entry.md)
  - [pg_unicode_decomposition](../p/pg_unicode_decomposition.md) (structure type)
- Called from (representative examples):
  - [unicode_normalize](../u/unicode_normalize.md)
  - [unicode_is_normalized_quickcheck](../u/unicode_is_normalized_quickcheck.md)

## Notes and Other Information
- This is a static function, accessible only within unicode_norm.c
- Returns 0 for characters without decomposition entries (including Hangul characters)
- The combining class is stored as uint8, supporting values 0-255
- Essential for canonical ordering during Unicode normalization
- Characters with combining class 0 are starter characters that can begin a new combining sequence
- Non-zero combining classes indicate the relative ordering priority of combining marks

## Simplified Source

```c
static uint8
get_canonical_class(pg_wchar code)
{
    // Look up character in decomposition table
    const pg_unicode_decomposition *entry = get_code_entry(code);

    // Return combining class, or 0 if no entry found
    return entry ? entry->comb_class : 0;
}
```