# pg_euctw_dsplen

## Location
[src/common/wchar.c:355-376](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/wchar.c#L355-L376)

## Overview
Determines the display width (number of column positions) that a single EUC-TW encoded character occupies when displayed.

## Definition
```c
static int pg_euctw_dsplen(const unsigned char *s)
```

## Detailed Description
This function calculates the display width of an EUC-TW character, which is important for proper text formatting, alignment, and terminal display. In EUC-TW encoding, different character types have different display characteristics:

- **SS2 sequences**: Display width of 2 columns (full-width characters)
- **SS3 sequences**: Display width of 2 columns (full-width characters)
- **High-bit set characters**: Display width of 2 columns (full-width characters)
- **ASCII characters**: Display width determined by `pg_ascii_dsplen` (typically 1 column, but may be 0 for control characters)

The function is essential for text layout operations such as column alignment, text wrapping, and cursor positioning in terminal applications.

## Parameters / Member Variables
- `s`: Pointer to the first byte of an EUC-TW encoded character sequence

## Dependencies
- Functions called/Symbols referenced:
  - SS2 (Single Shift 2 control character constant)
  - SS3 (Single Shift 3 control character constant)
  - IS_HIGHBIT_SET (macro to check if high bit is set in byte)
  - pg_ascii_dsplen (function to determine ASCII character display width)
- Called from (representative examples):
  - pg_encoding_set_invalid (indirectly through encoding function tables)

## Notes and Other Information
- Returns an integer representing the number of display columns occupied (typically 1 or 2)
- Display width differs from byte length - multibyte characters typically occupy 2 display columns
- Used for proper text formatting and alignment in database output
- ASCII control characters may have display width of 0 (handled by pg_ascii_dsplen)
- Essential for consistent text rendering across different character sets in PostgreSQL's output formatting system