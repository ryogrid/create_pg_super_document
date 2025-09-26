# pg_eucjp_mblen

## Location
src/common/wchar.c: 185 - 190

## Overview
Determines the byte length of a single EUC-JP (Extended Unix Code for Japanese) encoded character.

## Definition
```c
static int pg_eucjp_mblen(const unsigned char *s)
```

## Detailed Description
This function calculates how many bytes comprise a single character in EUC-JP encoding. It serves as a wrapper around the generic EUC multibyte length function `pg_euc_mblen`. EUC-JP is a variable-width character encoding where:

- ASCII characters occupy 1 byte
- Half-width katakana characters (with Single Shift 2) occupy 2 bytes
- Full-width characters (hiragana, katakana, kanji) occupy 2-3 bytes

The function is essential for properly parsing EUC-JP text streams by identifying character boundaries, which is crucial for text processing operations like searching, indexing, and display formatting.

## Parameters / Member Variables
- `s`: Pointer to the first byte of the EUC-JP encoded character to measure

## Dependencies
- Functions called/Symbols referenced:
  - pg_euc_mblen (generic EUC multibyte length calculation function)
- Called from (representative examples):
  - pg_encoding_set_invalid (character encoding setup function)

## Notes and Other Information
- This is a static function, limiting its scope to the wchar.c compilation unit
- Acts as a thin wrapper around the generic EUC multibyte length function
- Essential for text processing operations that need to iterate through EUC-JP character sequences
- Returns the number of bytes in the character, or potentially an error indication for invalid sequences
- Part of PostgreSQL's character encoding infrastructure that supports multiple Asian character sets