# pg_euccn_mblen

## Location
[src/common/wchar.c:271-282](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/wchar.c#L271-L282)

## Overview
A static function that determines the byte length of a multi-byte character sequence in EUC-CN (Extended Unix Code for Chinese) encoding.

## Definition
```c
static int pg_euccn_mblen(const unsigned char *s)
```

## Detailed Description
The pg_euccn_mblen function calculates how many bytes are needed to represent a single character in EUC-CN encoding. Unlike the generic EUC implementation, this function uses a simplified approach specifically for EUC-CN: characters with the high bit set (non-ASCII) are always 2 bytes, while ASCII characters (high bit clear) are 1 byte. This simplified logic works for EUC-CN because it primarily uses 1-byte ASCII and 2-byte Chinese characters, without commonly using the 3-byte SS2/SS3 sequences.

## Parameters / Member Variables
- `s`: Pointer to the first byte of a character sequence in EUC-CN encoding

## Dependencies
- Functions called/Symbols referenced:
  - IS_HIGHBIT_SET (macro to test if the high bit of a byte is set)
- Called from (representative examples):
  - pg_encoding_set_invalid (via function pointer assignment)

## Notes and Other Information
- This is a static function with internal linkage, only visible within the wchar.c compilation unit
- Uses a simplified approach compared to the generic pg_euc_mblen function
- Returns 1 for ASCII characters (0x00-0x7F) and 2 for Chinese characters (0x80-0xFF)
- The simplified logic reflects that EUC-CN primarily uses 1-2 byte sequences, unlike other EUC variants that commonly use 3-byte sequences
- This function is more efficient than the generic EUC handler since it avoids checking for SS2/SS3 prefixes that are rarely used in EUC-CN