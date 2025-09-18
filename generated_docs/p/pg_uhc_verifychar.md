# pg_uhc_verifychar

## Location
[src/common/wchar.c:1587-1611](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/wchar.c#L1587-L1611)

## Overview
Validates a single character in UHC (Unified Hangul Code) encoding format, ensuring the character sequence is properly formed and does not contain invalid byte sequences.

## Definition
static int pg_uhc_verifychar(const unsigned char *s, int len)

## Detailed Description
This function performs character validation for UHC encoding, which is a Korean character encoding system. It verifies that a character starting at the given byte sequence is valid according to UHC encoding rules. The function first determines the expected multibyte character length using pg_uhc_mblen(), then validates that the buffer contains enough bytes and that the character doesn't contain the special invalid byte sequence (0x8d followed by a space character). It also ensures that no null bytes appear within the character sequence, as null bytes are not valid within multibyte characters.

## Parameters / Member Variables
- `s`: Pointer to the byte sequence to validate
- `len`: Length of the available buffer in bytes

## Dependencies
- Functions called/Symbols referenced:
  - pg_uhc_mblen
  - NONUTF8_INVALID_BYTE0
  - NONUTF8_INVALID_BYTE1
- Called from (representative examples):
  - [pg_uhc_verifystr](pg_uhc_verifystr.md)
  - [pg_encoding_set_invalid](pg_encoding_set_invalid.md)

## Notes and Other Information
- Returns the length of the validated character in bytes on success, or -1 on failure
- Specifically rejects the byte sequence 0x8d + space (0x20), which is a historically problematic sequence that could bypass validation in some contexts
- The function ensures that multibyte characters don't contain embedded null bytes, which would indicate corruption or improper character boundaries
- UHC is primarily used for Korean text encoding and supports both ASCII characters (1 byte) and Korean characters (2 bytes)