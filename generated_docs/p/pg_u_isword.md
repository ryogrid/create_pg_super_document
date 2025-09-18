# pg_u_isword

## Location
src/common/unicode_category.c: 232 - 242

## Overview
Tests whether a Unicode character is a word character according to Unicode TR18 specification, including alphabetic characters, marks, decimal numbers, connector punctuation, and join control characters.

## Definition
```c
bool pg_u_isword(pg_wchar code)
```

## Detailed Description
The `pg_u_isword` function implements Unicode TR18 word character classification by testing if a character belongs to any of the categories that constitute word characters. Unlike simple alphanumeric tests, this function includes a comprehensive set of Unicode categories:

- Mark characters (combining marks, spacing marks, enclosing marks) - PG_U_M_MASK
- Decimal number characters - PG_U_ND_MASK  
- Connector punctuation (like underscore) - PG_U_PC_MASK
- Alphabetic characters (via pg_u_isalpha)
- Join control characters (via pg_u_prop_join_control)

This comprehensive approach ensures proper word boundary detection across all Unicode scripts and languages, making it suitable for internationalized text processing.

## Parameters / Member Variables
- `code`: The Unicode character code point to test (pg_wchar type)

## Dependencies
- Functions called/Symbols referenced:
  - unicode_category
  - PG_U_CATEGORY_MASK
  - PG_U_M_MASK (mark characters bitmask)
  - PG_U_ND_MASK (decimal number bitmask) 
  - PG_U_PC_MASK (connector punctuation bitmask)
  - pg_u_isalpha
  - pg_u_prop_join_control
- Called from (representative examples):
  - Referenced in unicode_category.h header

## Notes and Other Information
- Returns true if the character is a word character, false otherwise
- Implements Unicode TR18 word character specification
- More comprehensive than traditional alphanumeric tests
- Includes Unicode mark characters, which is important for languages with combining characters
- Includes connector punctuation like underscore (_) which is traditionally considered part of word characters
- Join control characters are included for proper handling of complex scripts
- Used for regex word boundary detection and text processing operations