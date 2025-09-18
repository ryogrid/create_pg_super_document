# pg_eucjp_increment

## Location
src/backend/utils/mb/mbutils.c: 1437 - 1522

## Overview
An EUC-JP character incrementer function that handles the complex multi-byte encoding rules of Extended Unix Code for Japanese to generate the next valid character in lexicographic order.

## Definition
```c
static bool pg_eucjp_increment(unsigned char *charptr, int length)
```

## Detailed Description
This function implements EUC-JP specific logic for incrementing character values, supporting the various character sets used in Japanese encoding. EUC-JP is a variable-length encoding that can represent:
- ASCII characters (single byte, 0x00-0x7F)
- JIS X 0201 characters (two bytes, starting with SS2/0x8E)
- JIS X 0208 characters (two bytes, both bytes 0xA1-0xFE)  
- JIS X 0212 characters (three bytes, starting with SS3/0x8F)

The function processes each encoding type differently:
- SS2 sequences: Increments the second byte within valid range (0xA1-0xDF)
- SS3 sequences: Increments rightmost byte possible within 0xA1-0xFE range
- High-bit set sequences: Treats as JIS X 0208, increments within 0xA1-0xFE range
- ASCII: Simple increment up to 0x7F

Like other encoding incrementers, it's designed for range operations where exhaustive search isn't practical.

## Parameters / Member Variables
- `charptr`: Pointer to the EUC-JP character bytes to be incremented
- `length`: Length of the EUC-JP character sequence in bytes

## Dependencies
- Functions called/Symbols referenced:
  - SS2 (single shift 2 constant, 0x8E)
  - SS3 (single shift 3 constant, 0x8F)  
  - IS_HIGHBIT_SET (macro to check high bit)
- Called from (representative examples):
  - [pg_database_encoding_character_incrementer](pg_database_encoding_character_incrementer.md)

## Notes and Other Information
- Handles three different JIS character sets within EUC-JP encoding
- SS2 sequences represent half-width katakana and special characters
- SS3 sequences represent supplementary kanji characters  
- JIS X 0208 sequences represent the main kanji and hiragana/katakana sets
- Returns false when no valid increment is possible within encoding constraints
- Essential for proper Japanese text processing in range operations and indexing