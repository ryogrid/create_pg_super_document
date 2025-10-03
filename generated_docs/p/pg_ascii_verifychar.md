# pg_ascii_verifychar

## Location
[src/common/wchar.c:1063-1068](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/wchar.c#L1063-L1068)

## Overview
Validates a single character in ASCII encoding by always returning 1, indicating that any single byte is a valid ASCII character.

## Definition

```c
static int
pg_ascii_verifychar(const unsigned char *s, int len)
```
## Detailed Description
This function is part of PostgreSQL's multibyte character validation system for different text encodings. For ASCII encoding, since every single byte (0-255) represents a valid character, the function simply returns 1 without examining the actual byte value. This is the simplest possible implementation of a character verification function, as ASCII is a single-byte encoding where each byte represents exactly one character.

The function follows the general contract for verifychar functions: given a pointer to the first byte of a string and the remaining length, it returns the length in bytes of a validly encoded character beginning at that position, or -1 if invalid. For ASCII, every single byte is valid, so it always returns 1.

## Parameters / Member Variables
- `*s`: Pointer to the first byte of the character to verify (not examined in ASCII case)
- `len`: Remaining length of the string (assumed to be > 0 by contract)
## Dependencies
- Functions called/Symbols referenced: None
- Called from (representative examples):
  - [pg_encoding_set_invalid](pg_encoding_set_invalid.md) (indirectly through function pointer tables)

## Notes and Other Information
- This function can assume that len > 0 and that *s != '\0' according to the verifychar function contract
- The function doesn't actually examine the input bytes since ASCII is single-byte and all byte values are valid
- This is a static function used internally by PostgreSQL's encoding validation system
- ASCII verification is trivial compared to multibyte encodings like UTF-8, EUC-JP, or EUC-KR where actual byte sequence validation is required