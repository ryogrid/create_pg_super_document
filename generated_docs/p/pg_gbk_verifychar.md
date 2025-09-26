# pg_gbk_verifychar

## Location
src/common/wchar.c: 1533 - 1557

## Overview
Verifies the validity of a single GBK encoded character by checking its byte sequence and ensuring it conforms to GBK encoding rules.

## Definition
```c
static int pg_gbk_verifychar(const unsigned char *s, int len)
```

## Detailed Description
This function validates a single character in GBK encoding, which is a character encoding scheme used for Simplified Chinese characters. GBK is an extension of the GB2312 character set and is widely used in mainland China. The verification process includes several critical checks:

1. **Length verification**: Uses `pg_gbk_mblen` to determine the expected byte length of the character and ensures sufficient bytes are available in the buffer
2. **Invalid sequence detection**: Specifically rejects the invalid byte sequence (NONUTF8_INVALID_BYTE0, NONUTF8_INVALID_BYTE1) which represents a problematic character marker that could cause encoding conflicts
3. **Null terminator validation**: Ensures that no null bytes appear within the multi-byte character sequence, as this would indicate improper string termination or corrupted data

The function returns the number of bytes consumed by the valid character, or -1 if the character is malformed, invalid, or incomplete.

## Parameters / Member Variables
- `s`: Pointer to the unsigned char array starting at the character to verify
- `len`: Number of bytes available in the buffer for verification

## Dependencies
- Functions called/Symbols referenced:
  - pg_gbk_mblen (function to determine byte length of GBK character)
  - NONUTF8_INVALID_BYTE0 (constant for invalid byte sequence detection)
  - NONUTF8_INVALID_BYTE1 (constant for invalid byte sequence detection)
- Called from (representative examples):
  - pg_gbk_verifystr (for string-level verification)
  - pg_encoding_set_invalid (in encoding validation routines)

## Notes and Other Information
- Returns the number of bytes in the valid character (typically 1 or 2 for GBK), or -1 for invalid characters
- Part of PostgreSQL's character encoding validation system for Simplified Chinese text
- The function is static, limiting its scope to the wchar.c compilation unit
- GBK is a double-byte character set where characters can be either 1 byte (ASCII-compatible) or 2 bytes (Chinese characters)
- The invalid byte sequence check prevents specific problematic combinations that could interfere with other encoding systems
- Essential for maintaining data integrity when handling Chinese text in PostgreSQL databases