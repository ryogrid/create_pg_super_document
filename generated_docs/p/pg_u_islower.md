# pg_u_islower

## Location
src/common/unicode_category.c: 249 - 254

## Overview
Tests whether a Unicode character has the lowercase property according to Unicode character properties.

## Definition
```c
bool pg_u_islower(pg_wchar code)
```

## Detailed Description
The `pg_u_islower` function determines if a given Unicode character has the lowercase property as defined by the Unicode standard. This function is a wrapper around `pg_u_prop_lowercase`, which performs the actual Unicode property lookup. The function handles both ASCII characters (optimized lookup) and non-ASCII Unicode characters (range search through Unicode property tables).

For ASCII characters (code < 0x80), the function uses a pre-computed lookup table for optimal performance. For Unicode characters beyond ASCII, it performs a binary search through the Unicode lowercase property ranges. This approach correctly identifies lowercase characters across all Unicode scripts and languages.

## Parameters / Member Variables
- `code`: The Unicode character code point to test (pg_wchar type)

## Dependencies
- Functions called/Symbols referenced:
  - [pg_u_prop_lowercase](pg_u_prop_lowercase.md)
- Called from (representative examples):
  - [pg_wc_islower](pg_wc_islower.md) (regex locale support)
  - [icu_test](../i/icu_test.md) (Unicode category testing)

## Notes and Other Information
- Returns true if the character has the lowercase property, false otherwise
- Part of PostgreSQL's Unicode character classification system
- Used internally by regex engine and text processing functions
- Follows Unicode standard for lowercase character classification, not just ASCII a-z
- Performance optimized with ASCII fast-path and binary search for Unicode ranges
- Correctly handles lowercase characters from various scripts like Greek, Cyrillic, etc.