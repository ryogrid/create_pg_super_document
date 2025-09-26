# pg_utf_dsplen

## Location
[src/common/wchar.c:662-673](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/wchar.c#L662-L673)

## Overview
Calculates the display column width of a UTF-8 encoded character by converting it to Unicode and determining its visual width.

## Definition
static int pg_utf_dsplen(const unsigned char *s)

## Detailed Description
The  function serves as a UTF-8 specific wrapper around the Unicode width calculation functionality. It takes a UTF-8 encoded character sequence, converts it to a Unicode code point, and then determines how many display columns that character occupies when rendered.

This function is part of PostgreSQL's character encoding infrastructure, providing display width information needed for proper formatting and alignment of UTF-8 text in terminal outputs, query results, and logging.

## Parameters
- : Pointer to the start of a UTF-8 encoded character sequence

## Dependencies
- Functions called/Symbols referenced:
  - [ucs_wcwidth](../u/ucs_wcwidth.md) (to determine Unicode character display width)
  - [utf8_to_unicode](../u/utf8_to_unicode.md) (to convert UTF-8 bytes to Unicode code point)
- Called from:
  - [pg_encoding_set_invalid](pg_encoding_set_invalid.md) (as part of encoding function table setup)

## Notes and Other Information
- This is a static function used internally within PostgreSQL's character encoding system
- Returns the same width values as ucs_wcwidth: -1 for control characters, 0 for non-spacing, 1 for normal width, 2 for wide characters
- Part of the encoding-specific function dispatch mechanism in PostgreSQL
- The input pointer should point to a valid UTF-8 character sequence
- Used when PostgreSQL needs to calculate display widths for UTF-8 encoded text