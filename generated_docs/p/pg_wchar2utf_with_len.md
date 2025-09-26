# pg_wchar2utf_with_len

## Location
[src/common/wchar.c:507-537](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/wchar.c#L507-L537)

## Overview
Converts an array of pg_wchar (UCS-4) wide characters to UTF-8 encoded byte string with a specified input character count limit.

## Definition
```c
static int pg_wchar2utf_with_len(const pg_wchar *from, unsigned char *to, int len)
```

## Detailed Description
This function performs the reverse conversion of pg_utf2wchar_with_len, converting PostgreSQL's internal wide character representation (pg_wchar/UCS-4) back to UTF-8 encoded bytes. It processes a bounded number of wide characters and generates the corresponding UTF-8 byte sequence.

The function iterates through the input wide character array, converting each pg_wchar to its UTF-8 representation using the unicode_to_utf8 helper function. It then determines the byte length of each converted UTF-8 character using pg_utf_mblen and advances the output pointer accordingly.

The conversion handles the full Unicode range that can be represented in UTF-8, from basic ASCII characters to 4-byte UTF-8 sequences for characters beyond the Basic Multilingual Plane.

## Parameters / Member Variables
- `from`: Pointer to the source array of pg_wchar wide characters (not necessarily null-terminated)
- `to`: Pointer to the destination buffer for UTF-8 encoded bytes (caller must allocate sufficient space)
- `len`: Maximum number of wide characters to process from the source array

## Dependencies
- Functions called/Symbols referenced:
  - unicode_to_utf8
  - pg_utf_mblen
- Called from (representative examples):
  - pg_encoding_set_invalid

## Notes and Other Information
- This is a static function with internal linkage, only accessible within wchar.c
- The function null-terminates the output byte string automatically
- Returns the total number of bytes written to the output buffer (excluding the null terminator)
- The caller is responsible for ensuring the output buffer is large enough to hold all converted UTF-8 bytes plus a null terminator
- Designed for bounded input processing where the number of source characters is explicitly specified
- Part of PostgreSQL's character encoding conversion infrastructure, working as the inverse of pg_utf2wchar_with_len
- The comment describes the conversion as "trivial" because it leverages existing helper functions for the actual Unicode-to-UTF8 transformation