# unistr

## Location
src/backend/utils/adt/varlena.c: 6502 - 6667

## Overview
A PostgreSQL built-in function that processes Unicode escape sequences in text strings and converts them to their corresponding Unicode characters in the server encoding.

## Definition
```c
Datum unistr(PG_FUNCTION_ARGS)
```

## Detailed Description
The `unistr` function implements PostgreSQL's Unicode string processing functionality, similar to Oracle's UNISTR function. It scans through an input text string and replaces Unicode escape sequences with their corresponding Unicode characters. The function supports multiple Unicode escape sequence formats:

1. `\\XXXX` - 4-digit hexadecimal Unicode code point
2. `\\uXXXX` - 4-digit hexadecimal Unicode code point (with 'u' prefix)
3. `\\+XXXXXX` - 6-digit hexadecimal Unicode code point (with '+' prefix)
4. `\\UXXXXXXXX` - 8-digit hexadecimal Unicode code point (with 'U' prefix)

The function properly handles UTF-16 surrogate pairs for characters beyond the Basic Multilingual Plane (BMP), validates Unicode code points, and converts the results to the server's character encoding. It also handles escaped backslashes (`\\\\`) by converting them to single backslashes.

## Parameters / Member Variables
- Function accepts a single text argument via `PG_GETARG_TEXT_PP(0)`
- Returns a text value via `PG_RETURN_TEXT_P(result)`

## Dependencies
- Functions called/Symbols referenced:
  - `hexval_n`: Converts hexadecimal digit sequences to numeric values
  - `isxdigits_n`: Validates that a sequence contains valid hexadecimal digits
  - `is_valid_unicode_codepoint`: Validates Unicode code point values
  - `is_utf16_surrogate_first/second`: Handles UTF-16 surrogate pair validation
  - `surrogate_pair_to_codepoint`: Combines UTF-16 surrogate pairs into code points
  - `pg_unicode_to_server`: Converts Unicode to server encoding
  - `cstring_to_text_with_len`: Creates PostgreSQL text result
  - `MAX_UNICODE_EQUIVALENT_STRING`: Buffer size constant
- Called from (representative examples):
  - No direct callers found (likely called through PostgreSQL's function call mechanism as a SQL function)

## Notes and Other Information
- This is a PostgreSQL built-in function available to SQL users
- Supports proper UTF-16 surrogate pair handling for characters beyond the BMP (U+10000 and above)
- Validates all Unicode escape sequences and throws errors for invalid formats or code points
- Handles incomplete surrogate pairs by reporting syntax errors
- The function maintains state to track surrogate pair processing across multiple escape sequences
- All Unicode characters are converted to the server's character encoding
- Located at src/backend/utils/adt/varlena.c:6502-6667
- Error messages provide helpful hints about valid Unicode escape sequence formats