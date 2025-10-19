# pg_gb18030_verifystr

## Location
[src/common/wchar.c:1672-1700](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/wchar.c#L1672-L1700)

## Overview
Validates a string of characters in GB18030 encoding format, checking each character in the string for proper formatting and compliance with the Chinese character encoding standard.

## Definition
static int pg_gb18030_verifystr(const unsigned char *s, int len)

## Detailed Description
This function performs string validation for GB18030 encoding by iterating through each character in the provided buffer. It employs an optimized approach where ASCII characters are processed through a fast path, while multibyte characters (2-byte and 4-byte sequences) are validated using pg_gb18030_verifychar(). The function continues validation until it encounters a null terminator, an invalid character sequence, or reaches the end of the buffer. This design efficiently handles mixed ASCII/Chinese content, which is common in Chinese text processing applications.

## Parameters / Member Variables
- `s`: Pointer to the byte sequence (string) to validate
- `len`: Maximum length of the buffer to process in bytes

## Dependencies
- Functions called/Symbols referenced:
  - IS_HIGHBIT_SET
  - [pg_gb18030_verifychar](pg_gb18030_verifychar.md)
- Called from (representative examples):
  - [pg_encoding_set_invalid](pg_encoding_set_invalid.md)

## Notes and Other Information
- Returns the number of bytes successfully validated before encountering an error, null terminator, or end of buffer
- Uses a fast path optimization for ASCII characters, avoiding the overhead of full multibyte validation for single-byte characters
- Stops validation at the first null byte encountered, treating it as a string terminator
- Handles the complete GB18030 character set including 1-byte ASCII, 2-byte characters, and 4-byte Unicode-compatible sequences
- Part of PostgreSQL's character encoding validation infrastructure for supporting Chinese text in database applications
- Critical for ensuring data integrity when storing Chinese text in PostgreSQL databases

## Simplified Source
```c
static int pg_gb18030_verifystr(const unsigned char *s, int len) {
    const unsigned char *start = s;

    while (len > 0) {
        int char_len;

        // Fast path: ASCII characters (high bit not set)
        if (!IS_HIGHBIT_SET(*s)) {
            if (*s == '\0')
                break;  // Stop at null terminator
            char_len = 1;
        } else {
            // Verify multi-byte GB18030 character (2 or 4 bytes)
            char_len = pg_gb18030_verifychar(s, len);
            if (char_len == -1)
                break;  // Invalid character found
        }

        s += char_len;
        len -= char_len;
    }

    return s - start;  // Return bytes processed
}
```