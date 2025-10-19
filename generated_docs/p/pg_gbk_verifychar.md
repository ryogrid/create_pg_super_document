# pg_gbk_verifychar

## Location
[src/common/wchar.c:1533-1557](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/wchar.c#L1533-L1557)

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
  - [pg_gbk_mblen](pg_gbk_mblen.md) (function to determine byte length of GBK character)
  - NONUTF8_INVALID_BYTE0 (constant for invalid byte sequence detection)
  - NONUTF8_INVALID_BYTE1 (constant for invalid byte sequence detection)
- Called from (representative examples):
  - [pg_gbk_verifystr](pg_gbk_verifystr.md) (for string-level verification)
  - [pg_encoding_set_invalid](pg_encoding_set_invalid.md) (in encoding validation routines)

## Notes and Other Information
- Returns the number of bytes in the valid character (typically 1 or 2 for GBK), or -1 for invalid characters
- Part of PostgreSQL's character encoding validation system for Simplified Chinese text
- The function is static, limiting its scope to the wchar.c compilation unit
- GBK is a double-byte character set where characters can be either 1 byte (ASCII-compatible) or 2 bytes (Chinese characters)
- The invalid byte sequence check prevents specific problematic combinations that could interfere with other encoding systems
- Essential for maintaining data integrity when handling Chinese text in PostgreSQL databases

## Simplified Source
```c
static int pg_gbk_verifychar(const unsigned char *s, int len) {
    // Get expected character length from GBK encoding rules
    int expected_len = pg_gbk_mblen(s);

    // Check if buffer has enough bytes
    if (len < expected_len)
        return -1;

    // Reject specific invalid byte sequence
    if (expected_len == 2 &&
        s[0] == NONUTF8_INVALID_BYTE0 &&
        s[1] == NONUTF8_INVALID_BYTE1)
        return -1;

    // Ensure no null bytes within multi-byte character
    for (int i = 1; i < expected_len; i++) {
        if (s[i] == '\0')
            return -1;
    }

    return expected_len;
}
```