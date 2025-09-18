# pg_u_isalnum

## Location
src/common/unicode_category.c: 226 - 231

## Overview
Tests whether a Unicode character is alphanumeric (either alphabetic or digit) with support for both POSIX-compatible and Unicode standard digit classification.

## Definition
```c
bool pg_u_isalnum(pg_wchar code, bool posix)
```

## Detailed Description
The `pg_u_isalnum` function determines if a given Unicode character is alphanumeric by combining alphabetic and digit character tests. It uses `pg_u_isalpha` to check for alphabetic characters and `pg_u_isdigit` to check for digit characters. The function supports two modes of digit classification controlled by the `posix` parameter: POSIX-compatible mode (ASCII digits 0-9 only) and Unicode standard mode (all Unicode decimal number characters).

This function is essential for text processing operations that need to identify alphanumeric characters across different Unicode scripts and locales.

## Parameters / Member Variables
- `code`: The Unicode character code point to test (pg_wchar type)
- `posix`: Boolean flag controlling digit classification mode. If true, only ASCII digits (0-9) are considered digits; if false, all Unicode decimal number characters are considered digits.

## Dependencies
- Functions called/Symbols referenced:
  - pg_u_isalpha
  - pg_u_isdigit
- Called from (representative examples):
  - pg_wc_isalnum (regex locale support)
  - initcap_wbnext (word boundary detection in formatting)
  - icu_test (Unicode category testing)

## Notes and Other Information
- Returns true if the character is either alphabetic or a digit, false otherwise
- Part of PostgreSQL's Unicode character classification system
- The posix parameter allows compatibility with traditional POSIX character classes
- Used in text formatting, regex processing, and word boundary detection
- Combines Unicode alphabetic property with configurable digit classification