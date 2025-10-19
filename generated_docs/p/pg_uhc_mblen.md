# pg_uhc_mblen

## Location
[src/common/wchar.c:976-987](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/wchar.c#L976-L987)

## Overview
Returns the byte length of a UHC-encoded character to enable proper parsing and processing of Korean text.

## Definition
```c
static int pg_uhc_mblen(const unsigned char *s)
```

## Detailed Description
This function determines the byte length of a character in UHC (Unified Hangul Code) encoding. UHC is a character encoding scheme used primarily for Korean text that extends EUC-KR. The function implements a simple detection algorithm: if the first byte has the high bit set, it assumes the character is a 2-byte Korean character; otherwise, it treats it as a 1-byte ASCII character.

This function is essential for PostgreSQLs multi-byte character processing, allowing the system to correctly parse UHC-encoded strings by determining where one character ends and the next begins.

## Parameters / Member Variables
- `s`: Pointer to the first byte of the character to examine. Must point to a valid UHC-encoded character sequence.

## Dependencies
- Functions called/Symbols referenced:
  - IS_HIGHBIT_SET (macro to check if high bit is set)
- Called from (representative examples):
  - [pg_uhc_verifychar](pg_uhc_verifychar.md) (uses this to validate UHC character sequences)
  - [pg_encoding_set_invalid](pg_encoding_set_invalid.md) (indirectly through encoding function tables)

## Notes and Other Information
- This is a static function within src/common/wchar.c, used internally by PostgreSQLs character encoding subsystem
- UHC encoding uses 1 byte for ASCII characters (0x00-0x7F) and 2 bytes for Korean Hangul and Hanja characters
- The function assumes all multi-byte characters in UHC are exactly 2 bytes, which is correct for the UHC encoding standard
- This function is part of the encoding-specific function dispatch system that allows PostgreSQL to handle multiple character encodings uniformly
- Unlike some other multi-byte encodings, UHC has a relatively simple structure that makes this byte-length detection straightforward

## Simplified Source

```c
static int pg_uhc_mblen(const unsigned char *s) {
    // High bit set: 2-byte Korean character
    if (IS_HIGHBIT_SET(*s))
        return 2;

    // Low bit: 1-byte ASCII character
    return 1;
}
```