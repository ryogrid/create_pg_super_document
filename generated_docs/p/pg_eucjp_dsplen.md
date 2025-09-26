# pg_eucjp_dsplen

## Location
[src/common/wchar.c:191-209](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/wchar.c#L191-L209)

## Overview
Calculates the display length (number of screen columns) for EUC-JP (Extended Unix Code for Japanese) encoded characters.

## Definition
```c
static int pg_eucjp_dsplen(const unsigned char *s)
```

## Detailed Description
This function determines how many screen columns an EUC-JP encoded character will occupy when displayed. EUC-JP has specific display characteristics that differ from generic EUC encodings:

- Single Shift 2 (SS2) characters (half-width katakana): 1 display column
- Single Shift 3 (SS3) characters (JIS X 0212 supplementary kanji): 2 display columns
- High-bit set characters (JIS X 0208 characters like hiragana, katakana, kanji): 2 display columns
- ASCII characters: Uses pg_ascii_dsplen for proper handling (typically 1 column)

This function is crucial for proper text layout and formatting when working with Japanese text, as it accounts for the visual width differences between half-width and full-width characters.

## Parameters / Member Variables
- `s`: Pointer to the first byte of the EUC-JP encoded character to measure

## Dependencies
- Functions called/Symbols referenced:
  - SS2 (Single Shift 2 constant for half-width katakana)
  - SS3 (Single Shift 3 constant for supplementary characters)
  - IS_HIGHBIT_SET (macro to check if high bit is set)
  - pg_ascii_dsplen (function for ASCII display length calculation)
- Called from (representative examples):
  - pg_encoding_set_invalid (character encoding setup function)

## Notes and Other Information
- This is a static function, limiting its scope to the wchar.c compilation unit
- Specifically handles EUC-JP's unique characteristic where SS2 characters (half-width katakana) occupy only 1 column
- Different from generic EUC display length calculation which treats SS2 as 2 columns
- Essential for proper Japanese text display formatting in terminal and GUI applications
- Returns 1 for half-width characters, 2 for full-width characters, enabling proper text alignment