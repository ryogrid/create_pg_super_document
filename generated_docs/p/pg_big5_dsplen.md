# pg_big5_dsplen

## Location
[src/common/wchar.c:934-948](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/wchar.c#L934-L948)

## Overview
Determines the display width of a single character in the Big5 multibyte encoding.

## Definition
static int pg_big5_dsplen(const unsigned char *s)

## Detailed Description
This function calculates how many display columns a character occupies when rendered in a terminal or text display. Unlike pg_big5_mblen which returns byte length, this function returns the visual width of the character.

The function implements Big5 display width logic:
- Characters with the high bit set (double-byte Chinese characters) occupy 2 display columns
- ASCII characters are handled by pg_ascii_dsplen which returns 1 for printable characters or -1 for control characters

## Parameters / Member Variables
- s: Pointer to the first byte of the character sequence to analyze

## Dependencies
- Functions called/Symbols referenced:
  - IS_HIGHBIT_SET (macro to check if high bit is set)
  - pg_ascii_dsplen (handles ASCII character display width)
- Called from (representative examples):
  - pg_encoding_set_invalid

## Notes and Other Information
- This is a static function used internally within the multibyte character handling subsystem
- The function only examines the first byte to determine display width
- Return value can be 1 or 2 for valid characters, or -1 for control characters (via pg_ascii_dsplen)
- Used as part of the character encoding function table for Big5 support
- Essential for proper text formatting and cursor positioning when displaying Traditional Chinese text