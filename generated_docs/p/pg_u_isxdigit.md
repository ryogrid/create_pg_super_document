# pg_u_isxdigit

## Location
[src/common/unicode_category.c:317-331](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/unicode_category.c#L317-L331)

## Overview
Tests whether a Unicode code point represents a hexadecimal digit character, with support for both POSIX-compliant and Unicode-standard behavior.

## Definition

```c
bool
pg_u_isxdigit(pg_wchar code, bool posix)
```
## Detailed Description
This function determines if a given Unicode code point represents a valid hexadecimal digit. It provides two modes of operation: POSIX mode which strictly checks for ASCII hex digits (0-9, A-F, a-f), and Unicode mode which additionally considers Unicode decimal numbers and characters with the Hex_Digit property. The dual-mode design allows PostgreSQL to maintain compatibility with POSIX standards while supporting full Unicode hexadecimal digit recognition when needed.

## Parameters / Member Variables
- `code`: The Unicode code point (pg_wchar) to test for hexadecimal digit property
- `posix`: Boolean flag determining the checking mode - true for POSIX-compliant ASCII-only checking, false for full Unicode support
## Dependencies
- Functions called/Symbols referenced:
  - [unicode_category](../u/unicode_category.md)
  - PG_U_DECIMAL_NUMBER
  - [pg_u_prop_hex_digit](pg_u_prop_hex_digit.md)
- Called from (representative examples):
  - [icu_test](../i/icu_test.md) (in Unicode category tests)

## Notes and Other Information
- In POSIX mode, only ASCII characters 0-9, A-F, and a-f are considered hexadecimal digits
- In Unicode mode, the function accepts both decimal numbers (Unicode category) and characters with the Unicode Hex_Digit property
- This dual-mode approach provides flexibility for different internationalization requirements
- The function is used in text processing and parsing operations where hexadecimal digit recognition is needed
- Returns a boolean value: true if the character is a hexadecimal digit according to the specified mode, false otherwise