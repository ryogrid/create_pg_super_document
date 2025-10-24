# get_b_utf8

## Location
[src/backend/snowball/libstemmer/utilities.c:93-116](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/utilities.c#L93-L116)

## Overview
Decodes a UTF-8 character from a symbol buffer in reverse direction (backward), extracting the Unicode code point and returning the number of bytes consumed.

## Definition
static int get_b_utf8(const symbol * p, int c, int lb, int * slot)

## Detailed Description
This function is a backward UTF-8 decoder that reads UTF-8 encoded characters from a symbol buffer starting from a given position and moving backward. It handles UTF-8 multibyte sequences of 1-4 bytes and stores the decoded Unicode code point in the provided slot. The function is designed to work with the Snowball stemming algorithm's text processing needs, where backward traversal of UTF-8 strings is required.

The function implements the standard UTF-8 decoding algorithm but in reverse order:
- For 1-byte sequences (ASCII): directly extracts the value
- For 2-byte sequences: combines the continuation byte with the leading byte
- For 3-byte sequences: combines two continuation bytes with the leading byte  
- For 4-byte sequences: combines three continuation bytes with the leading byte

## Parameters / Member Variables
- : Pointer to the symbol buffer containing UTF-8 encoded text
- : Current position in the buffer (starting point for backward decoding)
- : Lower bound index - the minimum position we can read from
- : Output parameter where the decoded Unicode code point will be stored

## Dependencies
- Functions called/Symbols referenced:
  - symbol (type used for buffer parameter)
- Called from (representative examples):
  - [in_grouping_b_U](../i/in_grouping_b_U.md)
  - [out_grouping_b_U](../o/out_grouping_b_U.md)

## Notes and Other Information
- This is a static function internal to the Snowball stemmer utilities
- Returns the number of bytes consumed (1-4) or 0 if decoding fails
- Handles UTF-8 boundary detection using bit patterns (0x80, 0xC0, 0xE0)
- Uses bitwise operations to extract and combine UTF-8 byte sequences
- The function assumes well-formed UTF-8 input and may not handle all edge cases of malformed sequences
- Part of PostgreSQL's text search functionality for stemming non-ASCII text

## Simplified Source

```c
static int get_b_utf8(const symbol * p, int c, int lb, int * slot) {
    int a, b;

    if (c <= lb) return 0;  // Check bounds

    // Read first byte (moving backward)
    b = p[--c];

    // 1-byte UTF-8 character (ASCII)
    if (b < 0x80 || c == lb) {
        *slot = b;
        return 1;
    }

    // 2-byte UTF-8 character
    a = b & 0x3F;  // Extract lower 6 bits
    b = p[--c];
    if (b >= 0xC0 || c == lb) {
        *slot = (b & 0x1F) << 6 | a;  // Combine bytes
        return 2;
    }

    // 3-byte UTF-8 character
    a |= (b & 0x3F) << 6;
    b = p[--c];
    if (b >= 0xE0 || c == lb) {
        *slot = (b & 0xF) << 12 | a;
        return 3;
    }

    // 4-byte UTF-8 character
    *slot = (p[--c] & 0x7) << 18 | (b & 0x3F) << 12 | a;
    return 4;
}
```