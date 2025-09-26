# pg_euckr_verifystr

## Location
[src/common/wchar.c:1195-1223](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/wchar.c#L1195-L1223)

## Overview
Validates the byte sequence of an EUC-KR (Extended Unix Code for Korean) encoded string and returns the number of valid bytes processed.

## Definition

```c
static int
pg_euckr_verifystr(const unsigned char *s, int len)
```
## Detailed Description
This function verifies the validity of an EUC-KR encoded string by processing each character until it encounters an invalid character, a null terminator, or reaches the end of the specified length. EUC-KR is a variable-width character encoding where ASCII characters (0x00-0x7F) are single-byte and Korean characters are represented as two-byte sequences.

The function employs a fast-path optimization for ASCII characters and delegates multi-byte character validation to . It stops processing upon encountering the first invalid character or null terminator and returns the number of bytes successfully validated.

## Parameters / Member Variables
- : Pointer to the unsigned char array containing the EUC-KR encoded string to validate
- : Maximum number of bytes to process from the input string

## Dependencies
- Functions called/Symbols referenced:
  - IS_HIGHBIT_SET (macro to check if the high bit is set)
  - pg_euckr_verifychar (validates individual EUC-KR characters)
- Called from (representative examples):
  - pg_euccn_verifystr
  - pg_encoding_set_invalid

## Notes and Other Information
- Returns the number of valid bytes processed, which may be less than  if an invalid character or null terminator is encountered
- Uses a fast-path optimization for ASCII characters (bytes with high bit clear)
- Part of PostgreSQL's character encoding validation system for ensuring data integrity
- The function is static, indicating it's used internally within the wchar.c module
- EUC-KR encoding uses the range 0xA1-0xFE for the first byte and 0xA1-0xFE for the second byte of Korean characters