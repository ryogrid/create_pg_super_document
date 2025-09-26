# pg_johab_verifychar

## Location
[src/common/wchar.c:1307-1330](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/wchar.c#L1307-L1330)

## Overview
Validates a single character in JOHAB (Korean Industrial Standard) encoding and returns the number of bytes consumed if valid.

## Definition
```c
static int pg_johab_verifychar(const unsigned char *s, int len)
```

## Detailed Description
This function validates individual characters in JOHAB encoding, which is used for Korean text. JOHAB uses the same multi-byte length determination as EUC encodings but with different character validation rules. The function first determines the expected character length using `pg_johab_mblen()`, then validates that all bytes beyond the first conform to EUC range requirements.

For ASCII characters (high bit not set), the function accepts them immediately. For multi-byte characters, it validates that each subsequent byte falls within the valid EUC range (0xA1-0xFE).

## Parameters / Member Variables  
- `s`: Pointer to the unsigned char array containing the character to validate
- `len`: Maximum number of bytes available in the input buffer

## Dependencies
- Functions called/Symbols referenced:
  - [pg_johab_mblen](pg_johab_mblen.md) (determines multi-byte character length)
  - IS_HIGHBIT_SET (macro to check if high bit is set)
  - IS_EUC_RANGE_VALID (macro to validate EUC byte range 0xA1-0xFE)
- Called from (representative examples):
  - [pg_johab_verifystr](pg_johab_verifystr.md)
  - [pg_encoding_set_invalid](pg_encoding_set_invalid.md)

## Notes and Other Information
- Returns the number of bytes consumed for valid characters, -1 for invalid sequences
- Uses `pg_johab_mblen()` which delegates to `pg_euc_mblen()` for character length determination
- JOHAB characters can be 1 byte (ASCII), 2 bytes (single shift sequences or high-bit characters), or 3 bytes (SS3 sequences)
- Validates that all non-first bytes in multi-byte sequences conform to EUC range requirements
- Part of PostgreSQL's character encoding validation system ensuring data integrity for Korean text
- The function is static, indicating it's used internally within the wchar.c module