# pg_u_isdigit

## Location
[src/common/unicode_category.c:211-219](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/unicode_category.c#L211-L219)

## Overview
Determines whether a Unicode code point is a digit character, with support for both POSIX-compatible and Unicode standard variants.

## Definition


## Detailed Description
This function checks if a given Unicode code point represents a digit character. It implements the Unicode Compatibility Properties as described in Unicode Technical Report #18. The function provides two different behaviors based on the  parameter:

1. **POSIX Compatible mode** (): Returns true only for ASCII digits 0-9, matching traditional POSIX behavior for maximum compatibility with legacy applications
2. **Standard Unicode mode** (): Returns true for any Unicode character in the Decimal_Number general category, including digits from various scripts like Arabic-Indic digits, Devanagari digits, Thai digits, etc.

This dual-mode approach allows applications to choose between strict ASCII compatibility and full Unicode digit support based on their requirements.

## Parameters / Member Variables
- : The Unicode code point (pg_wchar) to test for digit property
- : Boolean flag controlling behavior - true for POSIX-compatible ASCII-only mode, false for full Unicode support

## Dependencies
- Functions called/Symbols referenced:
  - [unicode_category](../u/unicode_category.md) (function to get Unicode general category)
  - PG_U_DECIMAL_NUMBER (constant for Decimal_Number category)
- Called from (representative examples):
  - [pg_wc_isdigit](pg_wc_isdigit.md) (regex locale compatibility function)
  - [icu_test](../i/icu_test.md) (in test code)
  - [pg_u_isalnum](pg_u_isalnum.md) (alphanumeric character detection)
  - Referenced in pg_unicode_category header

## Notes and Other Information
- Part of PostgreSQL's Unicode Compatibility Properties implementation
- Provides flexibility between ASCII-only and full Unicode digit recognition
- Used by regex engine for character class matching
- Essential component of pg_u_isalnum for alphanumeric character detection
- POSIX mode ensures compatibility with traditional C library isdigit() behavior
- Standard mode enables proper internationalization support for numeric input from various writing systems