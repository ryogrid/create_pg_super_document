# pg_u_prop_lowercase

## Location
src/common/unicode_category.c: 122 - 132

## Overview
The pg_u_prop_lowercase function determines whether a given Unicode codepoint has the Lowercase property according to Unicode character classification standards.

## Definition
```c
bool pg_u_prop_lowercase(pg_wchar code)
```

## Detailed Description
This function checks if a Unicode character has the Lowercase property, which identifies characters that are lowercase letters or letter-like symbols. For ASCII characters (code < 0x80), it performs an optimized lookup using a bitmask operation on the unicode_opt_ascii table. For non-ASCII characters, it uses the range_search function to search through the unicode_lowercase array, which contains ranges of Unicode codepoints that have the Lowercase property.

The Lowercase property is distinct from the general category "Lowercase Letter" and includes additional characters beyond just traditional lowercase letters, encompassing various scripts and writing systems.

## Parameters / Member Variables
- `code`: The Unicode codepoint (pg_wchar) to test for the Lowercase property

## Dependencies
- Functions called/Symbols referenced:
  - PG_U_PROP_LOWERCASE (constant bitmask for the Lowercase property)
  - range_search (function for searching ranges)
  - lengthof (macro for array length)
- Called from (representative examples):
  - pg_u_prop_cased
  - pg_u_islower
  - icu_test (testing function)

## Notes and Other Information
- Optimized for ASCII characters with direct bitmask lookup
- Essential for case-sensitive text operations and collation
- Used in implementing proper lowercase detection across all Unicode scripts
- Part of the casing system that supports PostgreSQL's text processing and pattern matching
- Located in src/common/unicode_category.c:122-132