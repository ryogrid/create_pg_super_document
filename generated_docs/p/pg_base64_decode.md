# pg_base64_decode

## Location
[src/backend/utils/adt/encode.c:314-384](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/encode.c#L314-L384)

## Overview
Decodes Base64-encoded text back into binary data with comprehensive validation and error handling.

## Definition
```c
static uint64 pg_base64_decode(const char *src, size_t len, char *dst)
```

## Detailed Description
This function converts Base64-encoded text back into its original binary form with thorough validation of the input format. It processes the input character by character, skipping whitespace and validating each Base64 character against a lookup table (b64lookup). The decoder handles padding characters ("=") correctly and ensures the input sequence is properly formatted according to Base64 standards.

The decoding process works by:
1. Reading 4 Base64 characters into a 24-bit buffer (accumulating 6 bits per character)
2. Extracting 3 bytes of binary data from the 24-bit buffer
3. Handling padding sequences (1 or 2 "=" characters) to decode partial groups
4. Validating that the input contains only valid Base64 characters
5. Ensuring proper sequence termination and completeness

The function includes comprehensive error reporting for invalid characters, malformed padding sequences, and incomplete input data.

## Parameters / Member Variables
- `src`: Pointer to the source Base64-encoded string to decode
- `len`: Length of the source string in bytes
- `dst`: Pointer to the destination buffer where decoded binary data will be written

## Dependencies
- Functions called/Symbols referenced:
  - [pg_mblen](pg_mblen.md) (multibyte character length function for error reporting)
  - b64lookup (Base64 character lookup table - referenced implicitly)
  - ereport (error reporting function)
- Called from (representative examples):
  - [esc_dec_len](../e/esc_dec_len.md) (escape decoder length calculation function)

## Notes and Other Information
- Returns the number of bytes written to the destination buffer (uint64)
- Automatically skips whitespace characters (space, tab, newline, carriage return)
- Validates all input characters and provides detailed error messages for invalid input
- Handles Base64 padding ("=") characters correctly for sequences that are not multiples of 4
- Uses hard error reporting (ereport with ERROR) that will abort the current transaction on invalid input
- The function is static (internal linkage) within src/backend/utils/adt/encode.c
- Part of PostgreSQL's encoding/decoding subsystem for handling text-to-binary conversion
- Ensures complete sequence validation - detects truncated or malformed Base64 input
- Provides helpful error hints for common issues like missing padding or corrupted data