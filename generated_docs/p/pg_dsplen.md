# pg_dsplen

## Location
src/backend/utils/mb/mbutils.c: 1030 - 1036

## Overview
Returns the display length (width) of a multibyte character in terms of screen columns.

## Definition
```c
int pg_dsplen(const char *mbstr)
```

## Detailed Description
`pg_dsplen` determines the display width of the first multibyte character in a given string. Unlike `pg_mblen` which returns the byte length, this function returns how many screen columns the character occupies when displayed. This is particularly important for East Asian characters and other wide characters that may occupy two or more display columns.

The function delegates to the encoding-specific display length function through `pg_wchar_table[DatabaseEncoding->encoding].dsplen`. Different character encodings have different rules for character display width - for example, in UTF-8, most ASCII characters have a display width of 1, while many CJK (Chinese, Japanese, Korean) characters have a display width of 2.

## Parameters / Member Variables
- `mbstr`: Pointer to the start of a multibyte string. The function examines the character at this position to determine its display width.

## Dependencies
- Functions called/Symbols referenced:
  - `pg_wchar_table` (global encoding function table)
  - `DatabaseEncoding` (current database encoding information)
- Called from (representative examples):
  - `[p_isspecial](p_isspecial.md)` (text search parser special character detection)

## Notes and Other Information
- Returns the number of screen columns (typically 1 or 2) occupied by the first character
- Essential for proper text formatting and alignment in multibyte environments
- Particularly important for Asian character sets where characters may be "wide" (occupy 2 columns)
- Used less frequently than `pg_mblen` but critical for display-related operations
- The return value is typically 1 for most Latin characters and 2 for full-width Asian characters