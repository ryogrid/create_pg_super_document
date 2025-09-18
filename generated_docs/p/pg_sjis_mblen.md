# pg_sjis_mblen

## Location
src/port/path.c: 201 - 234

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
  - pg_sjis_verifychar
  - pg_encoding_set_invalid
  - debackslash_path

## Notes and Other Information
- This is a static function, only accessible within src/common/wchar.c
- Returns 1 for single-byte characters (ASCII and half-width katakana)
- Returns 2 for double-byte characters (kanji and full-width characters)
- The function assumes the input points to a valid Shift-JIS character sequence
- Half-width katakana characters (0xa1-0xdf) are treated as single-byte despite having the high bit set
- This function is part of PostgreSQL's character encoding support infrastructure