# PQdsplen

## Location
[src/interfaces/libpq/fe-misc.c:1252-1260](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-misc.c#L1252-L1260)

## Overview
PQdsplen calculates the display length (visual width) of a character at the beginning of a string, accounting for multibyte character encoding and display characteristics.

## Definition
```c
int PQdsplen(const char *s, int encoding)
```

## Detailed Description
This function provides a wrapper around pg_encoding_dsplen() to determine how many character positions a multibyte character will occupy when displayed. Unlike byte length or character count, display length accounts for the visual width of characters, which is particularly important for East Asian characters that may occupy two display columns, control characters that may not be visible, or combining characters that don't add width. This function ensures consistent display width calculations using libpq's dynamic encoding stance.

## Parameters / Member Variables
- `s`: Pointer to the string containing the character whose display length to calculate
- `encoding`: The character encoding identifier to use for display length calculation

## Dependencies
- Functions called/Symbols referenced:
  - [pg_encoding_dsplen](../p/pg_encoding_dsplen.md)
- Called from (representative examples):
  - MAX_PROMPT_SIZE (src/bin/psql/prompt.c:370)
  - [pg_wcswidth](../p/pg_wcswidth.md) (src/fe_utils/mbprint.c:190)
  - [pg_wcssize](../p/pg_wcssize.md) (src/fe_utils/mbprint.c:226)
  - [pg_wcsformat](../p/pg_wcsformat.md) (src/fe_utils/mbprint.c:307)
  - [strlen_max_width](../s/strlen_max_width.md) (src/fe_utils/print.c:3754)

## Notes and Other Information
- Returns the display width in character cells, not bytes or logical characters
- Critical for proper text alignment and formatting in terminal applications
- Handles wide characters (typically East Asian) that occupy two display columns
- Used extensively in psql and frontend utilities for proper text layout
- Part of libpq's comprehensive multibyte character support system
- Essential for calculating column widths and text positioning in formatted output

## Simplified Source

```c
int PQdsplen(const char *s, int encoding) {
    // Calculate display width of character using specified encoding
    return pg_encoding_dsplen(encoding, s);
}
```