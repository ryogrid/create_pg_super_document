# pg_u_isalpha

## Location
[src/common/unicode_category.c:220-225](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/unicode_category.c#L220-L225)

## Overview
Tests whether a Unicode character is an alphabetic character according to Unicode character properties.

## Definition

```c
bool
pg_u_isalpha(pg_wchar code)
```
## Detailed Description
The  function determines if a given Unicode character has the alphabetic property as defined by the Unicode standard. This function is a wrapper around , which performs the actual Unicode property lookup. The function handles both ASCII characters (optimized lookup) and non-ASCII Unicode characters (range search through Unicode property tables).

For ASCII characters (code < 0x80), the function uses a pre-computed lookup table for optimal performance. For Unicode characters beyond ASCII, it performs a binary search through the Unicode alphabetic property ranges.

## Parameters / Member Variables
- `code`: The Unicode character code point to test (pg_wchar type)
## Dependencies
- Functions called/Symbols referenced:
  - [pg_u_prop_alphabetic](pg_u_prop_alphabetic.md)
- Called from (representative examples):
  - [pg_wc_isalpha](pg_wc_isalpha.md) (regex locale support)
  - [pg_u_isalnum](pg_u_isalnum.md) (alphanumeric character test)
  - [pg_u_isword](pg_u_isword.md) (word character test)
  - [pg_u_ispunct](pg_u_ispunct.md) (punctuation character test)
  - [icu_test](../i/icu_test.md) (Unicode category testing)

## Notes and Other Information
- Returns true if the character is alphabetic, false otherwise
- Part of PostgreSQL's Unicode character classification system
- Used internally by regex engine and text processing functions
- Follows Unicode standard for alphabetic character classification
- Performance optimized with ASCII fast-path and binary search for Unicode ranges

## Simplified Source

```c
bool
pg_u_isalpha(pg_wchar code) {
    // Check if character has Unicode alphabetic property
    return pg_u_prop_alphabetic(code);
}
```