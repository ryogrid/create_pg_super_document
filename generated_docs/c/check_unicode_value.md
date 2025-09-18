# check_unicode_value

## Location
src/backend/parser/parser.c: 342 - 351

## Overview
A validation function that verifies whether a given Unicode code point is acceptable and raises an error if it's invalid.

## Definition
```c
static void check_unicode_value(pg_wchar c)
```

## Detailed Description
The check_unicode_value function serves as a validation gate for Unicode code points during Unicode escape sequence processing in PostgreSQL's parser. It takes a wide character (pg_wchar) representing a Unicode code point and validates it using PostgreSQL's internal Unicode validation logic.

When an invalid Unicode code point is encountered, the function reports a syntax error with the specific error code ERRCODE_SYNTAX_ERROR and a descriptive message "invalid Unicode escape value". This helps users identify problems with Unicode escape sequences in their SQL strings.

The function is static, meaning it's only accessible within the parser.c compilation unit, and is designed specifically for internal use during string literal parsing.

## Parameters / Member Variables
- `c`: A pg_wchar (wide character) representing the Unicode code point to validate

## Dependencies
- Functions called/Symbols referenced:
  - is_valid_unicode_codepoint (for validation logic)
  - ereport (for error reporting)
  - errcode (for error code specification)
  - errmsg (for error message formatting)
- Called from (representative examples):
  - str_udeescape (called twice during Unicode escape processing)

## Notes and Other Information
- Part of PostgreSQL's Unicode string literal processing pipeline
- Helps ensure that only valid Unicode code points are accepted in escape sequences
- The function does not return a value - it either succeeds silently or raises an ERROR
- Works in conjunction with the Unicode escape processing in str_udeescape function
- Validates both 4-digit (\uXXXX) and 6-digit (\UXXXXXX) Unicode escape sequences