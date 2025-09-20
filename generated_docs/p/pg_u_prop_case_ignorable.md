# pg_u_prop_case_ignorable

## Location
[src/common/unicode_category.c:159-169](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/unicode_category.c#L159-L169)

## Overview
Determines whether a Unicode code point has the Case_Ignorable property, which identifies characters that should be ignored when performing case-sensitive operations.

## Definition

```c
bool
pg_u_prop_case_ignorable(pg_wchar code)
```
## Detailed Description
This function checks if a given Unicode code point has the Case_Ignorable property according to the Unicode Standard. Characters with this property are typically modifiers, combining marks, or format characters that don't affect case operations and should be ignored during case folding, case mapping, or case-sensitive comparisons.

The function uses a two-tier approach for efficiency:
1. For ASCII characters (code < 0x80), it performs a direct lookup in the  table using a bitmask check
2. For non-ASCII characters, it performs a binary search in the  range table

## Parameters / Member Variables
- : The Unicode code point (pg_wchar) to test for the Case_Ignorable property

## Dependencies
- Functions called/Symbols referenced:
  - PG_U_PROP_CASE_IGNORABLE (constant/macro)
  - [range_search](../r/range_search.md) (function for binary search in Unicode ranges)
  - lengthof (macro to get array length)
- Called from (representative examples):
  - [icu_test](../i/icu_test.md) (in test code)
  - Referenced in pg_unicode_category header

## Notes and Other Information
- Part of PostgreSQL's Unicode property implementation
- Uses optimized ASCII lookup table for performance on common ASCII characters
- Falls back to binary search for full Unicode range coverage
- Case_Ignorable characters include combining marks, certain modifiers, and format characters
- Essential for proper Unicode case handling in text processing operations