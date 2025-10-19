# pg_latin1_verifystr

## Location
[src/common/wchar.c:1416-1426](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/wchar.c#L1416-L1426)

## Overview
Verifies a Latin-1 encoded string by finding the first null byte, since all non-null bytes in Latin-1 encoding are valid characters.

## Definition
```c
static int pg_latin1_verifystr(const unsigned char *s, int len)
```

## Detailed Description
This function provides string verification for Latin-1 (ISO 8859-1) encoding using an optimized approach. Since Latin-1 is a single-byte encoding where every byte value (0-255) represents a valid character, the only "invalid" sequence is when validation should stop - which occurs at null terminators.

The function uses `memchr()` to efficiently locate the first null byte (\0) in the string. This is much faster than character-by-character iteration since no actual character validation is needed - only null terminator detection.

The behavior is:
- If no null byte is found within the specified length, the entire buffer is considered valid
- If a null byte is found, validation stops at that point and returns the number of valid bytes before it

## Parameters / Member Variables
- `s`: Pointer to the beginning of the string to verify
- `len`: Maximum number of bytes to examine in the input buffer

## Dependencies
- Functions called/Symbols referenced:
  - memchr (standard C library function)
- Called from (representative examples):
  - [pg_encoding_set_invalid](pg_encoding_set_invalid.md) (used for multiple Latin-based encodings)

## Notes and Other Information
- Returns the number of valid bytes from the beginning of the string (up to the first null byte or end of buffer)
- Highly optimized implementation using `memchr()` instead of byte-by-byte iteration
- Part of PostgreSQL's character encoding validation infrastructure
- Used as the string verification function for multiple single-byte Latin-based encodings
- The function is static, indicating it's only used within the wchar.c compilation unit
- Does not perform any actual character validation since all non-null bytes are valid in Latin-1

## Simplified Source
```c
static int pg_latin1_verifystr(const unsigned char *s, int len) {
    // Find first null byte - all other bytes are valid in Latin-1
    const unsigned char *nullpos = memchr(s, 0, len);

    // Return bytes before null, or entire length if no null found
    return nullpos ? (nullpos - s) : len;
}
```