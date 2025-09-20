# pg_u_prop_white_space

## Location
[src/common/unicode_category.c:170-180](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/unicode_category.c#L170-L180)

## Overview
Determines whether a Unicode code point has the White_Space property, identifying characters that are considered whitespace according to the Unicode Standard.

## Definition

```c
bool
pg_u_prop_white_space(pg_wchar code)
```
## Detailed Description
This function checks if a given Unicode code point has the White_Space property according to the Unicode Standard. The White_Space property identifies characters that are used for spacing, line breaks, and paragraph separation in text. This includes traditional ASCII whitespace characters as well as various Unicode spacing characters.

The function implements the same efficient two-tier approach:
1. For ASCII characters (code < 0x80), it performs a direct lookup in the  table using a bitmask check
2. For non-ASCII characters, it performs a binary search in the  range table

## Parameters / Member Variables
- : The Unicode code point (pg_wchar) to test for the White_Space property

## Dependencies
- Functions called/Symbols referenced:
  - PG_U_PROP_WHITE_SPACE (constant/macro)
  - [range_search](../r/range_search.md) (function for binary search in Unicode ranges)
  - lengthof (macro to get array length)
- Called from (representative examples):
  - [icu_test](../i/icu_test.md) (in test code)
  - [pg_u_isspace](pg_u_isspace.md) (higher-level space checking function)
  - Referenced in pg_unicode_category header

## Notes and Other Information
- Core component of PostgreSQL's Unicode whitespace handling
- Used by pg_u_isspace for comprehensive whitespace detection
- Covers both ASCII whitespace (space, tab, newline, etc.) and Unicode whitespace characters
- Essential for proper text parsing, tokenization, and formatting operations
- Follows Unicode Standard definition of White_Space property rather than just ASCII space characters