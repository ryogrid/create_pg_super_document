# unicode_to_utf8

## Location
[src/include/mb/pg_wchar.h:591-622](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/mb/pg_wchar.h#L591-L622)

## Overview
Converts a Unicode code point to its UTF-8 byte sequence representation in a provided buffer.

## Definition
static inline unsigned char *unicode_to_utf8(pg_wchar c, unsigned char *utf8string)

## Detailed Description
This inline function encodes a Unicode code point into its corresponding UTF-8 byte sequence. UTF-8 is a variable-length encoding where characters can be represented using 1-4 bytes depending on their code point value. The function implements the standard UTF-8 encoding algorithm, handling four different encoding ranges:

- 1 byte: U+0000 to U+007F (ASCII characters)
- 2 bytes: U+0080 to U+07FF  
- 3 bytes: U+0800 to U+FFFF (Basic Multilingual Plane)
- 4 bytes: U+10000 to U+10FFFF (Supplementary Planes)

The function directly writes the UTF-8 bytes to the provided buffer and assumes the caller has allocated sufficient space (which can be determined using unicode_utf8len()).

## Parameters / Member Variables
- `c`: The Unicode code point to convert to UTF-8
- `utf8string`: Pointer to the buffer where UTF-8 bytes will be written (must have sufficient space)

## Dependencies
- Functions called/Symbols referenced: None (bitwise operations and arithmetic)
- Called from (representative examples):
  - [unicode_normalize_func](unicode_normalize_func.md) (src/backend/utils/adt/varlena.c:6379, 6389)
  - [pg_unicode_to_server](../p/pg_unicode_to_server.md) (src/backend/utils/mb/mbutils.c:891, 905)
  - [pg_unicode_to_server_noerror](../p/pg_unicode_to_server_noerror.md) (src/backend/utils/mb/mbutils.c:949, 959)
  - [pg_saslprep](../p/pg_saslprep.md) (src/common/saslprep.c:1210, 1225)
  - [convert_case](../c/convert_case.md) (src/common/unicode_case.c:179)
  - [pg_wchar2utf_with_len](../p/pg_wchar2utf_with_len.md) (src/common/wchar.c:515)

## Notes and Other Information
- Defined as a static inline function for performance optimization
- Located in src/include/mb/pg_wchar.h with other Unicode utility functions
- The caller must ensure the destination buffer has adequate space before calling
- Returns a pointer to the utf8string buffer for convenience
- Uses standard UTF-8 encoding bit patterns: 0xC0, 0xE0, 0xF0 for multi-byte sequences
- Continuation bytes always start with 0x80 bit pattern
- Does not perform input validation - assumes valid Unicode code point
- Critical component in PostgreSQL's character encoding conversion system
- Used extensively in string processing, normalization, and database encoding operations

## Simplified Source

```c
static inline unsigned char *unicode_to_utf8(pg_wchar c, unsigned char *utf8string) {
    // 1-byte encoding for ASCII (0x00-0x7F)
    if (c <= 0x7F) {
        utf8string[0] = c;
    }
    // 2-byte encoding (0x80-0x7FF)
    else if (c <= 0x7FF) {
        utf8string[0] = 0xC0 | ((c >> 6) & 0x1F);   // First byte: 110xxxxx
        utf8string[1] = 0x80 | (c & 0x3F);          // Second byte: 10xxxxxx
    }
    // 3-byte encoding (0x800-0xFFFF) - Basic Multilingual Plane
    else if (c <= 0xFFFF) {
        utf8string[0] = 0xE0 | ((c >> 12) & 0x0F);  // First byte: 1110xxxx
        utf8string[1] = 0x80 | ((c >> 6) & 0x3F);   // Second byte: 10xxxxxx
        utf8string[2] = 0x80 | (c & 0x3F);          // Third byte: 10xxxxxx
    }
    // 4-byte encoding (0x10000-0x10FFFF) - Supplementary Planes
    else {
        utf8string[0] = 0xF0 | ((c >> 18) & 0x07);  // First byte: 11110xxx
        utf8string[1] = 0x80 | ((c >> 12) & 0x3F);  // Second byte: 10xxxxxx
        utf8string[2] = 0x80 | ((c >> 6) & 0x3F);   // Third byte: 10xxxxxx
        utf8string[3] = 0x80 | (c & 0x3F);          // Fourth byte: 10xxxxxx
    }

    return utf8string;
}
```