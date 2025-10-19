# pg_big5_verifychar

## Location
[src/common/wchar.c:1479-1503](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/wchar.c#L1479-L1503)

## Overview
Verifies the validity of a single Big5 encoded character by checking its byte sequence and ensuring it conforms to Big5 encoding rules.

## Definition
```c
static int pg_big5_verifychar(const unsigned char *s, int len)
```

## Detailed Description
This function validates a single character in Big5 encoding, which is a character encoding scheme used primarily for Traditional Chinese characters. The verification process involves multiple checks:

1. **Length verification**: Uses `pg_big5_mblen` to determine the expected byte length of the character and ensures the available buffer has sufficient bytes
2. **Invalid sequence detection**: Specifically checks for and rejects the invalid byte sequence (NONUTF8_INVALID_BYTE0, NONUTF8_INVALID_BYTE1) which represents an invalid character marker
3. **Null terminator check**: Ensures that no null bytes appear within the multi-byte character sequence, as this would indicate premature string termination

The function returns the number of bytes consumed by the valid character, or -1 if the character is invalid or malformed.

## Parameters / Member Variables
- `s`: Pointer to the unsigned char array starting at the character to verify
- `len`: Number of bytes available in the buffer for verification

## Dependencies
- Functions called/Symbols referenced:
  - [pg_big5_mblen](pg_big5_mblen.md) (function to determine byte length of Big5 character)
  - NONUTF8_INVALID_BYTE0 (constant for invalid byte sequence detection)
  - NONUTF8_INVALID_BYTE1 (constant for invalid byte sequence detection)
- Called from (representative examples):
  - [pg_big5_verifystr](pg_big5_verifystr.md) (for string-level verification)
  - [pg_encoding_set_invalid](pg_encoding_set_invalid.md) (in encoding validation routines)

## Notes and Other Information
- Returns the number of bytes in the valid character (typically 1 or 2 for Big5), or -1 for invalid characters
- Part of PostgreSQL's character encoding validation system for Traditional Chinese text
- The function is static, limiting its scope to the wchar.c compilation unit
- Big5 is a double-byte character set where characters can be either 1 or 2 bytes long
- The invalid byte sequence check prevents specific problematic byte combinations that could cause encoding conflicts

## Simplified Source
```c
static int pg_big5_verifychar(const unsigned char *s, int len) {
    // Get expected character length from Big5 encoding rules
    int expected_len = pg_big5_mblen(s);

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