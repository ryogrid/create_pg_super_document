# pg_euctw_mblen

## Location
[src/common/wchar.c:339-354](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/wchar.c#L339-L354)

## Overview
Determines the byte length of a single multibyte character in EUC-TW (Extended Unix Code for Taiwan) encoding.

## Definition
```c
static int pg_euctw_mblen(const unsigned char *s)
```

## Detailed Description
This function analyzes the first byte of an EUC-TW encoded character sequence and returns the total number of bytes that comprise that character. EUC-TW uses different code sets with varying byte lengths:

- **SS2 sequences**: 4 bytes total (SS2 + 3 data bytes)
- **SS3 sequences**: 3 bytes total (SS3 + 2 data bytes) 
- **High-bit set bytes**: 2 bytes total (double-byte characters)
- **ASCII characters**: 1 byte (single-byte characters)

The function serves as a utility for parsing EUC-TW streams by providing the character boundary information needed for proper character-by-character processing.

## Parameters / Member Variables
- `s`: Pointer to the first byte of an EUC-TW encoded character sequence

## Dependencies
- Functions called/Symbols referenced:
  - SS2 (Single Shift 2 control character constant)
  - SS3 (Single Shift 3 control character constant)
  - IS_HIGHBIT_SET (macro to check if high bit is set in byte)
- Called from (representative examples):
  - pg_encoding_set_invalid (indirectly through encoding function tables)

## Notes and Other Information
- Returns an integer representing the number of bytes in the character (1, 2, 3, or 4)
- Does not validate that the subsequent bytes form a valid character sequence
- Essential for character boundary detection in EUC-TW text processing
- Used in conjunction with other EUC-TW conversion functions for complete encoding support
- Part of PostgreSQL's multibyte character length determination system