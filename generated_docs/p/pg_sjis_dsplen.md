# pg_sjis_dsplen

## Location
src/common/wchar.c: 905 - 921

## Overview
Determines the display width of a single character in the Shift JIS (SJIS) multibyte encoding.

## Definition
static int pg_sjis_dsplen(const unsigned char *s)

## Detailed Description
This function calculates how many display columns a character occupies when rendered in a terminal or text display. Unlike pg_sjis_mblen which returns byte length, this function returns the visual width of the character.

The function implements SJIS display width logic:
- Half-width katakana characters (0xA1-0xDF) occupy 1 display column
- Full-width characters (kanji, hiragana, full-width katakana) occupy 2 display columns
- ASCII characters are handled by pg_ascii_dsplen which returns 1 for printable characters

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
- Used as part of the character encoding function table for SJIS support
- Essential for proper text formatting and cursor positioning in terminals