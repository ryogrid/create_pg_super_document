# utf8_to_unicode

## Location
[src/fe_utils/mbprint.c:53-81](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/fe_utils/mbprint.c#L53-L81)

## Overview
A static utility function that converts a UTF-8 encoded character sequence to its corresponding Unicode code point.

## Definition
```c
static pg_wchar utf8_to_unicode(const unsigned char *c)
```

## Detailed Description
This function performs direct conversion of UTF-8 byte sequences to Unicode code points by examining the bit patterns in the first byte to determine the character length and then extracting and combining the relevant bits from each byte. It handles all valid UTF-8 encodings:

- 1-byte sequences (ASCII): 0xxxxxxx
- 2-byte sequences: 110xxxxx 10xxxxxx  
- 3-byte sequences: 1110xxxx 10xxxxxx 10xxxxxx
- 4-byte sequences: 11110xxx 10xxxxxx 10xxxxxx 10xxxxxx

The function is optimized for performance and assumes the input is valid UTF-8 - it performs no error checking. Invalid sequences return 0xffffffff as an error indicator.

## Parameters / Member Variables
- `c`: Pointer to the first byte of a UTF-8 character sequence (must point to a sufficiently long buffer)

## Dependencies
- Functions called/Symbols referenced:
  - None (uses only bitwise operations)
- Called from (representative examples):
  - [initcap_wbnext](../i/initcap_wbnext.md)
  - [unicode_assigned](unicode_assigned.md)
  - [unicode_normalize_func](unicode_normalize_func.md)
  - [unicode_is_normalized](unicode_is_normalized.md)
  - [pg_saslprep](../p/pg_saslprep.md)
  - convert_case
  - pg_utf_dsplen
  - [pg_wcsformat](../p/pg_wcsformat.md)

## Notes and Other Information
- This is a one-character version of pg_utf2wchar_with_len optimized for single character conversion
- No bounds checking is performed - caller must ensure the buffer is long enough for the complete UTF-8 sequence
- Returns 0xffffffff for invalid UTF-8 lead bytes (intentional error code)
- Used extensively throughout PostgreSQL for Unicode text processing, normalization, and formatting
- The function assumes little-endian byte order and uses standard UTF-8 encoding rules