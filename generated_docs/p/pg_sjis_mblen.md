# pg_sjis_mblen

## Location
[src/port/path.c:201-234](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/path.c#L201-L234)

## Overview
Determines the byte length of a multi-byte character in Shift-JIS (SJIS) encoding, returning the number of bytes that comprise the character starting at the given position.

## Definition
```c
static int pg_sjis_mblen(const unsigned char *s)
```

## Detailed Description
This function analyzes the first byte of a character sequence in Shift-JIS encoding to determine how many bytes comprise the complete character. Shift-JIS is a variable-length encoding where characters can be either 1 or 2 bytes long. The function uses byte value ranges to distinguish between:
- Single-byte katakana characters (0xa1-0xdf)
- Double-byte kanji characters (bytes with high bit set, excluding the katakana range)
- Single-byte ASCII characters (bytes without high bit set)

This function is essential for proper traversal of Shift-JIS encoded strings without splitting multi-byte characters.

## Parameters / Member Variables
- `s`: Pointer to the first byte of a character in a Shift-JIS encoded string

## Dependencies
- Functions called/Symbols referenced:
  - IS_HIGHBIT_SET
- Called from (representative examples):
  - [pg_sjis_verifychar](pg_sjis_verifychar.md)
  - [pg_encoding_set_invalid](pg_encoding_set_invalid.md)
  - [debackslash_path](../d/debackslash_path.md)

## Notes and Other Information
- This is a static function, only accessible within src/common/wchar.c
- Returns 1 for single-byte characters (ASCII and half-width katakana)
- Returns 2 for double-byte characters (kanji and full-width characters)
- The function assumes the input points to a valid Shift-JIS character sequence
- Half-width katakana characters (0xa1-0xdf) are treated as single-byte despite having the high bit set
- This function is part of PostgreSQL's character encoding support infrastructure

## Simplified Source

```c
// Simplified version of pg_sjis_mblen
static int pg_sjis_mblen(const unsigned char *s) {
    // Check for single-byte katakana characters (half-width katakana)
    if (*s >= 0xa1 && *s <= 0xdf) {
        return 1;
    }

    // Check for double-byte characters (kanji, full-width characters)
    if (IS_HIGHBIT_SET(*s)) {
        return 2;
    }

    // Default case: ASCII characters
    return 1;
}
```

Key simplifications made:
- Added descriptive comments for each character type check
- Clarified the logic flow with explicit return statements
- Explained the purpose of each byte range check
- The original code was already quite simple, so minimal changes were needed