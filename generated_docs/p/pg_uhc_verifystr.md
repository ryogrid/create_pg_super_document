# pg_uhc_verifystr

## Location
src/common/wchar.c: 1612 - 1640

## Overview
Validates a string of characters in UHC (Unified Hangul Code) encoding format, checking each character in the string for proper formatting and encoding compliance.

## Definition
static int pg_uhc_verifystr(const unsigned char *s, int len)

## Detailed Description
This function performs string validation for UHC encoding by iterating through each character in the provided buffer. It uses an optimized approach where ASCII characters (those without the high bit set) are processed quickly in a fast path, while multibyte characters are validated using pg_uhc_verifychar(). The function continues validation until it encounters a null terminator, an invalid character, or reaches the end of the buffer. This approach provides efficient validation for mixed ASCII/UHC content, which is common in Korean text processing.

## Parameters / Member Variables
- `s`: Pointer to the byte sequence (string) to validate
- `len`: Maximum length of the buffer to process in bytes

## Dependencies
- Functions called/Symbols referenced:
  - IS_HIGHBIT_SET
  - [pg_uhc_verifychar](pg_uhc_verifychar.md)
- Called from (representative examples):
  - [pg_encoding_set_invalid](pg_encoding_set_invalid.md)

## Notes and Other Information
- Returns the number of bytes successfully validated before encountering an error, null terminator, or end of buffer
- Uses a fast path optimization for ASCII characters, avoiding the overhead of full multibyte validation for single-byte characters
- Stops validation at the first null byte encountered, treating it as a string terminator
- The function is designed to work with null-terminated strings as well as fixed-length buffers
- Part of PostgreSQL's character encoding validation infrastructure for supporting Korean text