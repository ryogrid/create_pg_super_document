# ucs_wcwidth

## Location
[src/common/wchar.c:628-661](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/wchar.c#L628-L661)

## Overview
Determines the display column width of a Unicode character (UCS code point) for proper terminal/console formatting, handling control characters, combining characters, and wide East Asian characters.

## Definition
static int ucs_wcwidth(pg_wchar ucs)

## Detailed Description
The  function calculates the column width of an ISO 10646 (Unicode) character following specific rules:

- **Null character (U+0000)**: Returns 0 (no width)
- **Control characters and DEL**: C0/C1 control characters and DEL return -1 (unprintable)
- **Non-spacing/combining characters**: Characters with general category Mn, Me, or Cf return 0 (no additional width)
- **Wide East Asian characters**: Characters in East Asian Wide (W) or FullWidth (F) categories return 2 (double-width)
- **All other printable characters**: Return 1 (single-width)

The function uses binary search through precompiled Unicode tables to efficiently categorize characters. It prioritizes non-spacing properties over wide character properties when a character has both attributes.

## Parameters
- : The Unicode code point (pg_wchar) to measure for display width

## Dependencies
- Functions called/Symbols referenced:
  - mbbisearch (for binary search in Unicode tables)
  - mbinterval (structure type for Unicode ranges)
- Includes Unicode tables:
  - common/unicode_nonspacing_table.h
  - common/unicode_east_asian_fw_table.h
- Called from:
  - pg_utf_dsplen

## Notes and Other Information
- Assumes wchar_t characters are encoded in ISO 10646
- Returns -1 for characters outside valid Unicode range (> 0x0010ffff)
- Non-spacing property takes precedence over wide character property
- Based on Unicode Technical Report #11 for East Asian width determination
- Used internally for calculating proper string display lengths in PostgreSQL's multi-byte character handling