# pg_euckr_mblen

## Location
[src/common/wchar.c:216-221](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/wchar.c#L216-L221)

## Overview
A static function that determines the byte length of a multi-byte character sequence in EUC-KR (Extended Unix Code for Korean) encoding.

## Definition

```c
static int
pg_euckr_mblen(const unsigned char *s)
```
## Detailed Description
The pg_euckr_mblen function is a wrapper around the generic EUC (Extended Unix Code) multi-byte length function. It determines how many bytes are needed to represent a single character in EUC-KR encoding by delegating to the pg_euc_mblen function, which implements the standard EUC byte-length logic. EUC-KR uses 1-3 bytes per character, where single-byte characters represent ASCII, and multi-byte sequences represent Korean characters.

## Parameters / Member Variables
- : Pointer to the first byte of a character sequence in EUC-KR encoding

## Dependencies
- Functions called/Symbols referenced:
  - pg_euc_mblen
- Called from (representative examples):
  - pg_encoding_set_invalid (via function pointer assignment)

## Notes and Other Information
- This is a static function, meaning it has internal linkage and is only visible within the wchar.c compilation unit
- EUC-KR is a Korean character encoding that extends ASCII with Korean characters
- The function relies on the generic EUC implementation since EUC-KR follows the same byte-length determination rules as other EUC variants
- Returns 1 for ASCII characters, 2 for characters with SS2 prefix or high-bit set characters, and 3 for characters with SS3 prefix