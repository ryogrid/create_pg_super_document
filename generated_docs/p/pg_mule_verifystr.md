# pg_mule_verifystr

## Location
[src/common/wchar.c:1381-1409](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/wchar.c#L1381-L1409)

## Overview
Verifies the validity of a MULE-encoded string by iterating through characters and validating each multi-byte character sequence until a null terminator or invalid character is encountered.

## Definition
```c
static int pg_mule_verifystr(const unsigned char *s, int len)
```

## Detailed Description
This function validates an entire MULE-encoded string by processing it character by character. It implements an optimized approach with a fast path for ASCII characters (those without the high bit set) and delegates multi-byte character validation to `pg_mule_verifychar()`. The function continues validation until it encounters:
1. A null terminator ('\0')
2. An invalid multi-byte character sequence
3. The end of the provided buffer

The function returns the number of bytes successfully validated, which may be less than the input length if invalid characters are found.

## Parameters / Member Variables
- `s`: Pointer to the beginning of the string to verify
- `len`: Maximum number of bytes to examine in the input buffer

## Dependencies
- Functions called/Symbols referenced:
  - IS_HIGHBIT_SET
  - pg_mule_verifychar
- Called from (representative examples):
  - pg_encoding_set_invalid

## Notes and Other Information
- Returns the number of bytes successfully validated from the beginning of the string
- Implements a performance optimization by handling ASCII characters directly without calling the character verification function
- Part of PostgreSQL's character encoding validation infrastructure for MULE encoding
- The function is static, indicating it's only used within the wchar.c compilation unit
- Stops validation at the first null byte encountered, making it suitable for null-terminated strings