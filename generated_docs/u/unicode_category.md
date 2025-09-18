# unicode_category

## Location
src/common/unicode_category.c: 85 - 110

## Overview
The unicode_category function returns the Unicode general category for a given Unicode codepoint, providing essential character classification for text processing operations.

## Definition


## Detailed Description
This function determines the Unicode general category of a character by performing a binary search through the unicode_categories lookup table. For ASCII characters (code < 0x80), it uses an optimized lookup table (unicode_opt_ascii) for faster access. For non-ASCII characters, it performs a binary search on the main unicode_categories array, which contains ranges of Unicode codepoints and their corresponding categories.

The function validates that the input codepoint is within the valid Unicode range (≤ 0x10ffff) and returns PG_U_UNASSIGNED for any codepoints not found in the lookup tables.

## Parameters / Member Variables
- `code`: The Unicode codepoint (pg_wchar) for which to determine the general category

## Dependencies
- Functions called/Symbols referenced:
  - lengthof (macro for array length)
  - PG_U_UNASSIGNED (constant for unassigned category)
- Called from (representative examples):
  - [unicode_assigned](unicode_assigned.md)
  - [pg_u_prop_cased](../p/pg_u_prop_cased.md)
  - [pg_u_isdigit](../p/pg_u_isdigit.md)
  - [pg_u_isword](../p/pg_u_isword.md)
  - [pg_u_isblank](../p/pg_u_isblank.md)
  - [pg_u_iscntrl](../p/pg_u_iscntrl.md)
  - [pg_u_isgraph](../p/pg_u_isgraph.md)
  - [pg_u_isprint](../p/pg_u_isprint.md)
  - [pg_u_ispunct](../p/pg_u_ispunct.md)
  - [pg_u_isxdigit](../p/pg_u_isxdigit.md)

## Notes and Other Information
- Uses binary search algorithm for efficient O(log n) lookup in the unicode_categories table
- Optimized for ASCII characters with direct array access
- Validates input range with Assert for debugging builds
- Central function used by many Unicode property checking functions in PostgreSQL
- Located in src/common/unicode_category.c:85-110