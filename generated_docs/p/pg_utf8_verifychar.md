# pg_utf8_verifychar

## Location
[src/common/wchar.c:1701-1778](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/wchar.c#L1701-L1778)

## Overview
Validates a single character in UTF-8 encoding format, determining the character length and verifying that the byte sequence conforms to UTF-8 encoding rules.

## Definition
static int pg_utf8_verifychar(const unsigned char *s, int len)

## Detailed Description
This function performs character validation for UTF-8 encoding by first determining the expected character length based on the leading byte's bit pattern, then validating the complete sequence using pg_utf8_islegal(). UTF-8 uses variable-length encoding where characters can be 1-4 bytes long. The function identifies the character length by examining the high-order bits of the first byte: single-byte ASCII characters (0xxxxxxx), 2-byte sequences (110xxxxx), 3-byte sequences (1110xxxx), and 4-byte sequences (11110xxx). It explicitly rejects null bytes in single-byte contexts as invalid, ensuring proper string handling in C contexts.

## Parameters / Member Variables
- `s`: Pointer to the byte sequence to validate
- `len`: Length of the available buffer in bytes

## Dependencies
- Functions called/Symbols referenced:
  - [pg_utf8_islegal](pg_utf8_islegal.md)
- Called from (representative examples):
  - STRIDE_LENGTH
  - [pg_encoding_set_invalid](pg_encoding_set_invalid.md)

## Notes and Other Information
- Returns the length of the validated character in bytes (1-4) on success, or -1 on failure
- Handles all valid UTF-8 character lengths: 1 byte (ASCII), 2 bytes, 3 bytes, and 4 bytes
- Explicitly rejects null bytes (0x00) when encountered as single-byte characters
- Uses pg_utf8_islegal() for comprehensive RFC3629 compliance checking, including overlong sequence detection
- Part of PostgreSQL's core UTF-8 validation infrastructure, critical for data integrity in Unicode text processing
- The validation ensures no security vulnerabilities from overlong encodings that could bypass ASCII-based security checks

## Simplified Source

```c
static int pg_utf8_verifychar(const unsigned char *s, int len) {
    int char_length;

    // Handle ASCII characters (0xxxxxxx)
    if ((*s & 0x80) == 0) {
        if (*s == '\0')
            return -1;  // Reject null bytes
        return 1;
    }

    // Determine character length based on leading byte pattern
    if ((*s & 0xe0) == 0xc0)      // 110xxxxx = 2-byte char
        char_length = 2;
    else if ((*s & 0xf0) == 0xe0) // 1110xxxx = 3-byte char
        char_length = 3;
    else if ((*s & 0xf8) == 0xf0) // 11110xxx = 4-byte char
        char_length = 4;
    else
        char_length = 1;          // Invalid leading byte

    // Check if we have enough bytes in buffer
    if (char_length > len)
        return -1;

    // Validate the complete UTF-8 sequence
    if (!pg_utf8_islegal(s, char_length))
        return -1;

    return char_length;
}
```