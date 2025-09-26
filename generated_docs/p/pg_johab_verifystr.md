# pg_johab_verifystr

## Location
[src/common/wchar.c:1331-1359](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/wchar.c#L1331-L1359)

## Overview
Validates the byte sequence of a JOHAB (Korean Industrial Standard) encoded string and returns the number of valid bytes processed.

## Definition
```c
static int pg_johab_verifystr(const unsigned char *s, int len)
```

## Detailed Description
This function verifies the validity of a JOHAB encoded string by processing each character sequentially until it encounters an invalid character, a null terminator, or reaches the end of the specified length. JOHAB is a variable-width character encoding used for Korean text, where ASCII characters are single-byte and Korean characters can be 2-byte or 3-byte sequences depending on the specific character set plane being used.

The function employs a fast-path optimization for ASCII characters (bytes without the high bit set) and delegates multi-byte character validation to `pg_johab_verifychar()`. Processing stops upon encountering the first invalid character or null terminator.

## Parameters / Member Variables
- `s`: Pointer to the unsigned char array containing the JOHAB encoded string to validate  
- `len`: Maximum number of bytes to process from the input string

## Dependencies
- Functions called/Symbols referenced:
  - IS_HIGHBIT_SET (macro to check if the high bit is set)
  - pg_johab_verifychar (validates individual JOHAB characters)
- Called from (representative examples):
  - pg_encoding_set_invalid

## Notes and Other Information
- Returns the number of valid bytes processed, which may be less than `len` if an invalid character or null terminator is encountered
- Uses a fast-path optimization for ASCII characters (bytes with high bit clear)
- Part of PostgreSQL's character encoding validation system for ensuring data integrity in Korean text processing
- The function is static, indicating it's used internally within the wchar.c module
- JOHAB encoding uses the same multi-byte length determination as EUC encodings but with different character validation rules
- Handles the complexity of JOHAB's variable-width encoding through delegation to the character-level validation function