# pg_big5_mblen

## Location
[src/common/wchar.c:922-933](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/wchar.c#L922-L933)

## Overview
Determines the byte length of a single character in the Big5 multibyte encoding.

## Definition
static int pg_big5_mblen(const unsigned char *s)

## Detailed Description
This function analyzes the first byte of a character sequence to determine how many bytes comprise a complete character in the Big5 encoding. Big5 is a variable-width encoding used primarily for Traditional Chinese text that can represent characters using either 1 or 2 bytes.

The function implements simple Big5 character length detection logic:
- Characters with the high bit set (0x80-0xFF) are assumed to be double-byte Chinese characters
- Characters without the high bit set (0x00-0x7F) are single-byte ASCII characters

## Parameters / Member Variables
- s: Pointer to the first byte of the character sequence to analyze

## Dependencies
- Functions called/Symbols referenced:
  - IS_HIGHBIT_SET (macro to check if high bit is set)
- Called from (representative examples):
  - [pg_big5_verifychar](pg_big5_verifychar.md)
  - [pg_encoding_set_invalid](pg_encoding_set_invalid.md)

## Notes and Other Information
- This is a static function used internally within the multibyte character handling subsystem
- The function only examines the first byte to make the length determination
- Return value is always 1 or 2, representing the character length in bytes
- Used as part of the character encoding function table for Big5 support
- The logic is simpler than SJIS as Big5 doesn't have single-byte extended characters