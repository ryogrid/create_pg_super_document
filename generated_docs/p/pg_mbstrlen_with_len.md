# pg_mbstrlen_with_len

## Location
src/backend/utils/mb/mbutils.c: 1057 - 1082

## Overview
Returns the character length of a multibyte string with a byte limit boundary, handling strings that may not be null-terminated.

## Definition
```c
int pg_mbstrlen_with_len(const char *mbstr, int limit)
```

## Detailed Description
`pg_mbstrlen_with_len` calculates the character length of a multibyte string within a specified byte limit. Unlike `pg_mbstrlen()`, this function does not require the input string to be null-terminated and will stop counting when either the byte limit is reached or a null terminator is encountered, whichever comes first.

This function is essential when working with string data that has a known byte length but may not be null-terminated, such as data from database columns or network packets. The function includes the same single-byte encoding optimization as `pg_mbstrlen()`, returning the limit value directly for single-byte encodings.

The function carefully tracks both the character count and remaining byte limit, advancing through the string using `pg_mblen()` to ensure proper multibyte character boundaries are respected.

## Parameters / Member Variables
- `mbstr`: Pointer to the start of a multibyte string (not necessarily null-terminated).
- `limit`: Maximum number of bytes to examine in the string.

## Dependencies
- Functions called/Symbols referenced:
  - `pg_database_encoding_max_length` (checks if encoding is single-byte)
  - `pg_mblen` (gets byte length of individual characters)
- Called from (representative examples):
  - `text_length` (calculating text column lengths)
  - `text_substring` (substring operations with bounds)
  - `bpchar` and `bpcharlen` (fixed-length character type operations)
  - `executor_errposition` and `parser_errposition` (error reporting with position)
  - Various text manipulation functions like `lpad`, `rpad`, `text_left`, `text_right`

## Notes and Other Information
- Returns the number of complete characters within the byte limit
- Handles both null-terminated and non-null-terminated strings safely
- Includes performance optimization for single-byte encodings
- Ensures multibyte character boundaries are not broken when the limit is reached
- Critical for safe string processing in PostgreSQL's text handling functions
- Used extensively in SQL text functions and error reporting mechanisms