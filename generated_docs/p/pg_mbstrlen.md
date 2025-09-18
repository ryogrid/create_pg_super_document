# pg_mbstrlen

## Location
src/backend/utils/mb/mbutils.c: 1037 - 1056

## Overview
Returns the length of a multibyte string counted in characters (not bytes).

## Definition
```c
int pg_mbstrlen(const char *mbstr)
```

## Detailed Description
`pg_mbstrlen` calculates the character length of a null-terminated multibyte string. Unlike `strlen()` which counts bytes, this function counts actual characters, making it essential for proper text processing in multibyte environments. For example, a UTF-8 string containing Asian characters might have a byte length of 12 but a character length of only 4.

The function includes an important optimization: if the current database encoding is single-byte (maximum character length is 1), it simply calls the standard `strlen()` function. For multibyte encodings, it iterates through the string using `pg_mblen()` to advance by complete characters rather than individual bytes.

## Parameters / Member Variables
- `mbstr`: Null-terminated multibyte string whose character length is to be calculated.

## Dependencies
- Functions called/Symbols referenced:
  - [pg_database_encoding_max_length](pg_database_encoding_max_length.md) (checks if encoding is single-byte)
  - [pg_mblen](pg_mblen.md) (gets byte length of individual characters)
  - `strlen` (standard C library function for single-byte optimization)
- Called from (representative examples):
  - [NUM_processor](../N/NUM_processor.md) (numeric formatting operations)
  - [get_iso_localename](../g/get_iso_localename.md) (locale name processing)
  - [text_format_append_string](../t/text_format_append_string.md) (text formatting operations)

## Notes and Other Information
- Returns the number of characters (not bytes) in the string
- Includes performance optimization for single-byte encodings
- Essential for accurate character counting in multibyte environments
- Commonly used in text formatting, validation, and processing functions
- Assumes input string is properly null-terminated and contains valid multibyte sequences