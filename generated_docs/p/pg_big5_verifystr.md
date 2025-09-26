# pg_big5_verifystr

## Location
src/common/wchar.c: 1504 - 1532

## Overview
Verifies the validity of a Big5 encoded string by iterating through each character and ensuring the entire string conforms to Big5 encoding rules.

## Definition
```c
static int pg_big5_verifystr(const unsigned char *s, int len)
```

## Detailed Description
This function validates a complete Big5 encoded string by processing each character sequentially. Big5 is a character encoding scheme primarily used for Traditional Chinese characters. The function employs a dual-strategy approach for optimal performance:

1. **Fast path for ASCII-compatible characters**: Single-byte characters without the high bit set are validated quickly as they are ASCII-compatible and inherently valid in Big5
2. **Full verification for multi-byte characters**: Characters with the high bit set undergo complete validation using `pg_big5_verifychar`

The function processes the string character by character, advancing by the appropriate byte length for each validated character. The verification stops when:
- An invalid character is encountered (when `pg_big5_verifychar` returns -1)
- A null terminator is found
- The end of the specified length is reached

This function is essential for ensuring data integrity when handling Traditional Chinese text in PostgreSQL databases.

## Parameters / Member Variables
- `s`: Pointer to the unsigned char array containing the Big5 encoded string to verify
- `len`: Maximum number of bytes to verify in the string

## Dependencies
- Functions called/Symbols referenced:
  - IS_HIGHBIT_SET (macro to check if the high bit is set in a byte)
  - pg_big5_verifychar (function to verify individual Big5 characters)
- Called from (representative examples):
  - pg_encoding_set_invalid (in encoding validation routines)

## Notes and Other Information
- Returns the number of valid bytes processed before encountering an invalid character, null terminator, or reaching the end
- Uses an optimized approach by handling ASCII-compatible characters separately from multi-byte Big5 characters
- Part of PostgreSQL's comprehensive character encoding validation framework for Asian languages
- The function is static, meaning it's only accessible within the same compilation unit (wchar.c)
- Handles both null-terminated strings and fixed-length buffers effectively
- Big5 characters can be either 1 byte (ASCII-compatible) or 2 bytes (Traditional Chinese characters)