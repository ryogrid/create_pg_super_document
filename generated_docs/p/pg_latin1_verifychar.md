# pg_latin1_verifychar

## Location
[src/common/wchar.c:1410-1415](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/wchar.c#L1410-L1415)

## Overview
A trivial character verification function for Latin-1 encoding that always validates any single byte as a valid character.

## Definition
```c
static int pg_latin1_verifychar(const unsigned char *s, int len)
```

## Detailed Description
This function provides character verification for Latin-1 (ISO 8859-1) encoding, which is a single-byte character encoding that covers all possible byte values (0-255) as valid characters. Since Latin-1 encoding maps every possible byte value to a valid character, no actual validation is needed - the function simply returns 1 to indicate that any single byte is valid.

This implementation reflects the nature of Latin-1 encoding where:
- Bytes 0-127 represent the same characters as ASCII
- Bytes 128-255 represent additional Latin characters, symbols, and control characters
- Every byte value corresponds to a defined character, so validation always succeeds

## Parameters / Member Variables
- `s`: Pointer to the character to verify (parameter present for API consistency but not used)
- `len`: Available buffer length (parameter present for API consistency but not used)

## Dependencies
- Functions called/Symbols referenced:
  - None (trivial implementation)
- Called from (representative examples):
  - [pg_encoding_set_invalid](pg_encoding_set_invalid.md) (used for multiple Latin-based encodings)

## Notes and Other Information
- Always returns 1, indicating that one byte was successfully validated
- Part of PostgreSQL's character encoding verification framework
- Used as the verification function for multiple single-byte Latin-based encodings including ISO-8859-1, ISO-8859-2, ISO-8859-3, etc.
- The function parameters are not actually used in the implementation but maintain consistency with the character verification function interface
- The function is static, indicating it's only used within the wchar.c compilation unit

## Simplified Source
```c
static int pg_latin1_verifychar(const unsigned char *s, int len) {
    // Latin-1 encoding: all 256 byte values are valid characters
    // No validation needed - always succeeds
    return 1;
}
```