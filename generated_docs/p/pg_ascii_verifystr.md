# pg_ascii_verifystr

## Location
[src/common/wchar.c:1069-1078](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/wchar.c#L1069-L1078)

## Overview
Validates an entire ASCII-encoded string by checking for null bytes and returning the position of the first null byte or the full string length if no nulls are found.

## Definition
static int pg_ascii_verifystr(const unsigned char *s, int len)

## Detailed Description
This function is part of PostgreSQL's multibyte character validation system for ASCII encoding. It verifies an entire string by scanning for null bytes (\0) within the specified length. According to the verifystr function contract, it must test for and reject zeroes in the input, returning the byte offset of the first invalid character (null byte) or the full length if the entire string is valid.

The function uses memchr() to efficiently locate the first null byte in the string. If no null byte is found within the specified length, the entire string is considered valid and the function returns the full length. If a null byte is found, it returns the offset to that position, indicating where validation failed.

## Parameters / Member Variables
- : Pointer to the string to verify
- : Length of the string to verify in bytes

## Dependencies
- Functions called/Symbols referenced:
  - memchr (C standard library function)
- Called from (representative examples):
  - [pg_encoding_set_invalid](pg_encoding_set_invalid.md) (indirectly through function pointer tables)

## Notes and Other Information
- The function must reject null bytes as per the verifystr contract
- Uses memchr() for efficient null byte detection rather than character-by-character scanning
- This is a static function used internally by PostgreSQL's encoding validation system
- For ASCII, the main validation concern is null byte detection rather than character encoding validity
- Returns the number of valid input bytes, which equals len when the whole string is valid

## Simplified Source

```c
static int pg_ascii_verifystr(const unsigned char *s, int len) {
    // Find first null byte in the string
    const unsigned char *null_pos = memchr(s, 0, len);

    // Return position of null byte or full length if no nulls found
    return (null_pos == NULL) ? len : null_pos - s;
}
```