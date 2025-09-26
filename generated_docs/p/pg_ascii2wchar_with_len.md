# pg_ascii2wchar_with_len

## Location
src/common/wchar.c: 70 - 84

## Overview
Converts ASCII-encoded string to wide character representation with length constraint, providing a safe conversion mechanism that respects buffer boundaries.

## Definition
```c
static int pg_ascii2wchar_with_len(const unsigned char *from, pg_wchar *to, int len)
```

## Detailed Description
This function performs character-by-character conversion from ASCII bytes to PostgreSQL's internal wide character format (pg_wchar). It operates within specified length constraints to prevent buffer overflows. The conversion continues until either the input length is exhausted, a null terminator is encountered, or the end of the input string is reached. The function ensures the output is null-terminated and returns the count of characters converted.

As part of PostgreSQL's multi-byte encoding support framework, this function specifically handles the SQL/ASCII encoding, which is the simplest case where each byte directly maps to a wide character value since ASCII characters have the same numeric values in both representations.

## Parameters / Member Variables
- `from`: Source buffer containing ASCII-encoded bytes to convert
- `to`: Destination buffer for storing converted wide characters (pg_wchar)  
- `len`: Maximum number of input bytes to process, providing buffer overflow protection

## Dependencies
- Functions called/Symbols referenced:
  - (None - uses only basic pointer operations and assignments)
- Called from (representative examples):
  - pg_encoding_set_invalid

## Notes and Other Information
- This is a static function internal to the wchar.c module
- Part of PostgreSQL's encoding conversion infrastructure supporting multiple character encodings
- The function assumes input is validly formed ASCII, as noted in the file comments
- Returns the count of characters actually converted, which may be less than the input length
- Always null-terminates the output buffer for safety
- Used specifically for SQL/ASCII encoding handling within PostgreSQL's broader multi-byte character support system