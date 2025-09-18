# pg_u_prop_hex_digit

## Location
src/common/unicode_category.c: 181 - 191

## Overview
Determines whether a Unicode code point has the Hex_Digit property, identifying characters that can be used as hexadecimal digits.

## Definition


## Detailed Description
This function checks if a given Unicode code point has the Hex_Digit property according to the Unicode Standard. Characters with this property are those that can legitimately be used as hexadecimal digits in numeric literals. This includes the standard ASCII digits 0-9 and letters A-F (both uppercase and lowercase), as well as any fullwidth variants or other Unicode characters designated as hex digits.

The function follows the same efficient implementation pattern as other Unicode property functions:
1. For ASCII characters (code < 0x80), it performs a direct lookup in the  table using a bitmask check
2. For non-ASCII characters, it performs a binary search in the  range table

## Parameters / Member Variables
- : The Unicode code point (pg_wchar) to test for the Hex_Digit property

## Dependencies
- Functions called/Symbols referenced:
  - PG_U_PROP_HEX_DIGIT (constant/macro)
  - range_search (function for binary search in Unicode ranges)
  - lengthof (macro to get array length)
- Called from (representative examples):
  - icu_test (in test code)
  - pg_u_isxdigit (higher-level hexadecimal digit checking function)
  - Referenced in pg_unicode_category header

## Notes and Other Information
- Essential component for hexadecimal number parsing and validation
- Used by pg_u_isxdigit to provide comprehensive hex digit recognition
- Covers standard ASCII hex digits (0-9, A-F, a-f) and Unicode variants
- Important for parsing hexadecimal literals in SQL and other contexts
- Ensures consistent hex digit recognition across different Unicode encodings and character sets