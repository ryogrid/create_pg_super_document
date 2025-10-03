# find_case_map

## Location
[src/common/unicode_case.c:203-234](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/unicode_case.c#L203-L234)

## Overview
Locates the Unicode case mapping entry for a given Unicode codepoint using optimized lookup for ASCII characters and binary search for higher codepoints.

## Definition

```c
static const pg_case_map *
find_case_map(pg_wchar ucs)
```
## Detailed Description
The  function is responsible for efficiently locating case mapping information for Unicode characters in PostgreSQL's case conversion system. It implements a two-tier lookup strategy optimized for performance:

1. **Fast ASCII lookup**: Characters with codepoints less than 0x80 (128) are stored in a dense array that allows direct indexing for O(1) lookup time.

2. **Binary search for higher codepoints**: Characters with codepoints 0x80 and above are stored sparsely and require a binary search through the sorted case mapping table.

The function returns a pointer to the  structure containing the case conversion mappings for the requested character, or NULL if no mapping exists for that codepoint. The case mapping table contains conversion information for lowercase, uppercase, and titlecase variants of each character.

## Parameters / Member Variables
- : The Unicode codepoint (pg_wchar) to find case mapping for

## Dependencies
- Functions called/Symbols referenced:
  - lengthof (macro to get array length)
  - pg_case_map (case mapping structure type)
  - case_map (global case mapping table array)
- Called from (representative examples):
  - [unicode_lowercase_simple](../u/unicode_lowercase_simple.md)
  - [unicode_titlecase_simple](../u/unicode_titlecase_simple.md)
  - [unicode_uppercase_simple](../u/unicode_uppercase_simple.md)
  - [convert_case](../c/convert_case.md)

## Notes and Other Information
- This is a static function, only accessible within the unicode_case.c file
- The function uses an assertion to verify that the case_map array contains at least 0x80 entries for the fast ASCII lookup
- The dense storage for ASCII characters (codepoints < 0x80) provides optimal performance for the most commonly used characters
- Binary search is used for non-ASCII characters, providing O(log n) lookup time for the sparse portion of the table
- Returns NULL for characters that don't have case conversion mappings, allowing calling code to handle such characters appropriately
- The case mapping table is pre-sorted by codepoint to enable binary search functionality
- The function assumes the case_map array is properly initialized and sorted, which is handled by the Unicode data generation process

## Simplified Source

```c
static const pg_case_map *
find_case_map(pg_wchar ucs)
{
    // Fast lookup for ASCII characters (0-127)
    if (ucs < 0x80) {
        return &case_map[ucs];
    }

    // Binary search for non-ASCII characters
    int min = 0x80;
    int max = lengthof(case_map) - 1;

    while (max >= min) {
        int mid = (min + max) / 2;

        if (ucs > case_map[mid].codepoint)
            min = mid + 1;
        else if (ucs < case_map[mid].codepoint)
            max = mid - 1;
        else
            return &case_map[mid];  // Found exact match
    }

    return NULL;  // No case mapping found
}
```