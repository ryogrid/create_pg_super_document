# pg_u_prop_join_control

## Location
[src/common/unicode_category.c:192-210](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/unicode_category.c#L192-L210)

## Overview
Determines whether a Unicode code point has the Join_Control property, identifying characters that control cursive joining behavior in scripts like Arabic.

## Definition

```c
bool
pg_u_prop_join_control(pg_wchar code)
```
## Detailed Description
This function checks if a given Unicode code point has the Join_Control property according to the Unicode Standard. Characters with this property are format characters that control whether adjacent characters should join when rendered in cursive scripts. The most common examples are the Zero Width Joiner (ZWJ, U+200D) and Zero Width Non-Joiner (ZWNJ, U+200C), which are used to explicitly control letter joining in scripts like Arabic, Persian, and Devanagari.

The function implements the standard two-tier lookup strategy:
1. For ASCII characters (code < 0x80), it performs a direct lookup in the  table using a bitmask check
2. For non-ASCII characters, it performs a binary search in the  range table

## Parameters / Member Variables
- : The Unicode code point (pg_wchar) to test for the Join_Control property

## Dependencies
- Functions called/Symbols referenced:
  - PG_U_PROP_JOIN_CONTROL (constant/macro)
  - [range_search](../r/range_search.md) (function for binary search in Unicode ranges)
  - lengthof (macro to get array length)
- Called from (representative examples):
  - [icu_test](../i/icu_test.md) (in test code)
  - [pg_u_isword](pg_u_isword.md) (used in word boundary detection)
  - Referenced in pg_unicode_category header

## Notes and Other Information
- Critical for proper handling of cursive scripts and complex text layout
- Used by pg_u_isword function for word boundary analysis in complex scripts
- [Join](../J/Join.md) control characters are invisible but affect text rendering and shaping
- Essential for correct text processing in Arabic, Persian, Urdu, and similar scripts
- These characters have special significance in text segmentation and word identification
- No ASCII characters have the Join_Control property (all are in the higher Unicode ranges)