# pg_base64_encode

## Location
src/backend/utils/adt/encode.c: 267 - 313

## Overview
Encodes binary data into Base64 format with automatic line wrapping at 76 characters per line.

## Definition
```c
static uint64 pg_base64_encode(const char *src, size_t len, char *dst)
```

## Detailed Description
This function converts binary data into Base64 encoded text format following RFC standards. It processes input data in 3-byte groups, converting each group into 4 Base64 characters using a lookup table (_base64). The encoder automatically inserts newline characters every 76 characters to create properly formatted Base64 output that conforms to MIME standards for line length.

The encoding process works by:
1. Reading 3 bytes of input data into a 24-bit buffer
2. Extracting four 6-bit values from the buffer
3. Using each 6-bit value as an index into the Base64 character set
4. Adding padding characters (=) when the input length is not divisible by 3
5. Inserting newlines every 76 characters for proper formatting

## Parameters / Member Variables
- `src`: Pointer to the source binary data to encode
- `len`: Length of the source data in bytes
- `dst`: Pointer to the destination buffer where Base64 encoded text will be written

## Dependencies
- Functions called/Symbols referenced:
  - _base64 (Base64 character lookup table - referenced implicitly)
- Called from (representative examples):
  - esc_dec_len (escape decoder length calculation function)

## Notes and Other Information
- Returns the number of characters written to the destination buffer (uint64)
- Automatically handles padding with "=" characters for inputs not divisible by 3
- Inserts newline characters every 76 characters for MIME compliance
- Uses bit manipulation and shifting for efficient encoding
- The function is static (internal linkage) within src/backend/utils/adt/encode.c
- Assumes the destination buffer is large enough to hold the encoded output plus newlines
- Does not null-terminate the output string - caller is responsible for this if needed
- Part of PostgreSQL's encoding/decoding subsystem for handling binary data representation