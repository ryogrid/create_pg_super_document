# pg_gbk_verifystr

## Location
[src/common/wchar.c:1558-1586](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/wchar.c#L1558-L1586)

## Overview
Verifies the validity of a GBK encoded string by iterating through each character and ensuring the entire string conforms to GBK encoding rules.

## Definition
```c
static int pg_gbk_verifystr(const unsigned char *s, int len)
```

## Detailed Description
This function validates a complete GBK encoded string by processing each character sequentially. GBK (GuoBiao Kuozhan) is a character encoding scheme used primarily for Simplified Chinese characters and is an extension of the GB2312 standard widely used in mainland China. The function implements an optimized dual-strategy verification approach:

1. **Fast path for ASCII-compatible characters**: Single-byte characters without the high bit set are validated quickly as they are ASCII-compatible and inherently valid within the GBK encoding space
2. **Full verification for multi-byte characters**: Characters with the high bit set undergo comprehensive validation using `pg_gbk_verifychar` to ensure proper GBK compliance

The function processes the string character by character, advancing by the appropriate byte length for each successfully validated character. The verification process terminates when:
- An invalid or malformed character is encountered (when `pg_gbk_verifychar` returns -1)
- A null terminator is found within the string
- The end of the specified buffer length is reached

This function is crucial for maintaining data integrity when handling Simplified Chinese text in PostgreSQL database systems.

## Parameters / Member Variables
- `s`: Pointer to the unsigned char array containing the GBK encoded string to verify
- `len`: Maximum number of bytes to verify in the string buffer

## Dependencies
- Functions called/Symbols referenced:
  - IS_HIGHBIT_SET (macro to check if the high bit is set in a byte)
  - [pg_gbk_verifychar](pg_gbk_verifychar.md) (function to verify individual GBK characters)
- Called from (representative examples):
  - [pg_encoding_set_invalid](pg_encoding_set_invalid.md) (in encoding validation routines)

## Notes and Other Information
- Returns the number of valid bytes processed before encountering an invalid character, null terminator, or reaching the buffer end
- Uses an optimized approach by handling ASCII-compatible characters separately from multi-byte GBK characters
- Part of PostgreSQL's comprehensive character encoding validation framework for Chinese language support
- The function is static, meaning it's only accessible within the same compilation unit (wchar.c)
- Efficiently handles both null-terminated strings and fixed-length buffers
- GBK characters can be either 1 byte (ASCII-compatible) or 2 bytes (Simplified Chinese characters)
- Essential for preventing encoding-related data corruption in Chinese text processing

## Simplified Source
```c
static int pg_gbk_verifystr(const unsigned char *s, int len) {
    const unsigned char *start = s;

    while (len > 0) {
        int char_len;

        // Fast path: ASCII characters (high bit not set)
        if (!IS_HIGHBIT_SET(*s)) {
            if (*s == '\0')
                break;  // Stop at null terminator
            char_len = 1;
        } else {
            // Verify multi-byte GBK character
            char_len = pg_gbk_verifychar(s, len);
            if (char_len == -1)
                break;  // Invalid character found
        }

        s += char_len;
        len -= char_len;
    }

    return s - start;  // Return bytes processed
}
```