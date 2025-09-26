# find_case_map

## Location
src/common/unicode_case.c: 203 - 234

## Overview
Locates the Unicode case mapping entry for a given Unicode codepoint using optimized lookup for ASCII characters and binary search for higher codepoints.

## Definition


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
  - unicode_lowercase_simple
  - unicode_titlecase_simple
  - unicode_uppercase_simple
  - convert_case

## Notes and Other Information
- This is a static function, only accessible within the unicode_case.c file
- The function uses an assertion to verify that the case_map array contains at least 0x80 entries for the fast ASCII lookup
- The dense storage for ASCII characters (codepoints < 0x80) provides optimal performance for the most commonly used characters
- Binary search is used for non-ASCII characters, providing O(log n) lookup time for the sparse portion of the table
- Returns NULL for characters that don't have case conversion mappings, allowing calling code to handle such characters appropriately
- The case mapping table is pre-sorted by codepoint to enable binary search functionality
- The function assumes the case_map array is properly initialized and sorted, which is handled by the Unicode data generation process