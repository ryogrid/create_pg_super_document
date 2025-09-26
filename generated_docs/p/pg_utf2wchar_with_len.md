# pg_utf2wchar_with_len

## Location
src/common/wchar.c: 441 - 506

## Overview
Converts a UTF-8 encoded byte string to an array of pg_wchar (UCS-4) wide characters with a specified input length limit.

## Definition
```c
static int pg_utf2wchar_with_len(const unsigned char *from, pg_wchar *to, int len)
```

## Detailed Description
This function performs UTF-8 to UCS-4 (Universal Character Set, 32-bit) conversion for a bounded input string. It processes UTF-8 encoded bytes and converts them to PostgreSQL's internal wide character representation (pg_wchar). The function handles all valid UTF-8 sequences from 1-byte ASCII characters to 4-byte Unicode characters.

The conversion process follows UTF-8 decoding rules:
- 1-byte sequences (0xxxxxxx): ASCII characters (0-127)
- 2-byte sequences (110xxxxx 10xxxxxx): Characters U+0080 to U+07FF
- 3-byte sequences (1110xxxx 10xxxxxx 10xxxxxx): Characters U+0800 to U+FFFF
- 4-byte sequences (11110xxx 10xxxxxx 10xxxxxx 10xxxxxx): Characters U+10000 to U+10FFFF

The function gracefully handles incomplete sequences at the end of input by dropping them, and treats invalid byte sequences as single-byte characters to avoid raising errors.

## Parameters / Member Variables
- `from`: Pointer to the source UTF-8 encoded byte string (not necessarily null-terminated)
- `to`: Pointer to the destination buffer for pg_wchar characters (caller must allocate sufficient space including trailing zero)
- `len`: Maximum number of input bytes to process from the source string

## Dependencies
- Functions called/Symbols referenced:
  - (None - uses only basic bit operations and assignments)
- Called from (representative examples):
  - pg_encoding_set_invalid

## Notes and Other Information
- This is a static function with internal linkage, only accessible within wchar.c
- The function null-terminates the output array automatically
- Returns the count of wide characters written to the output buffer (excluding the null terminator)
- Designed for bounded input processing where the source string length is explicitly specified
- Does not perform UTF-8 validation beyond basic structural checks - malformed sequences are handled by treating invalid bytes as single characters
- The caller is responsible for ensuring the output buffer is large enough to hold the converted characters plus a null terminator
- Part of PostgreSQL's character encoding conversion infrastructure