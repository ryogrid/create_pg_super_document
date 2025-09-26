# pg_euctw_verifystr

## Location
[src/common/wchar.c:1278-1306](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/wchar.c#L1278-L1306)

## Overview
Validates the byte sequence of an EUC-TW (Extended Unix Code for Traditional Chinese) encoded string and returns the number of valid bytes processed.

## Definition
```c
static int pg_euctw_verifystr(const unsigned char *s, int len)
```

## Detailed Description
This function verifies the validity of an EUC-TW encoded string by processing each character sequentially until it encounters an invalid character, a null terminator, or reaches the end of the specified length. EUC-TW is a variable-width character encoding used for Traditional Chinese text, where ASCII characters are single-byte and Chinese characters can be 2-byte (CNS 11643 Plane 1) or 4-byte (CNS 11643 Planes 1-7 with SS2 prefix) sequences.

The function employs a fast-path optimization for ASCII characters (bytes without the high bit set) and delegates multi-byte character validation to `pg_euctw_verifychar()`. Processing stops upon encountering the first invalid character or null terminator.

## Parameters / Member Variables
- `s`: Pointer to the unsigned char array containing the EUC-TW encoded string to validate
- `len`: Maximum number of bytes to process from the input string

## Dependencies
- Functions called/Symbols referenced:
  - IS_HIGHBIT_SET (macro to check if the high bit is set)
  - [pg_euctw_verifychar](pg_euctw_verifychar.md) (validates individual EUC-TW characters)
- Called from (representative examples):
  - [pg_encoding_set_invalid](pg_encoding_set_invalid.md)

## Notes and Other Information
- Returns the number of valid bytes processed, which may be less than `len` if an invalid character or null terminator is encountered
- Uses a fast-path optimization for ASCII characters (bytes with high bit clear)
- Part of PostgreSQL's character encoding validation system for ensuring data integrity in Traditional Chinese text processing
- The function is static, indicating it's used internally within the wchar.c module
- Handles the complexity of EUC-TW's variable-width encoding through delegation to the character-level validation function