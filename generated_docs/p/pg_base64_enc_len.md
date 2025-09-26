# pg_base64_enc_len

## Location
src/backend/utils/adt/encode.c: 385 - 391

## Overview
Calculates the required buffer length for Base64 encoding of binary data, accounting for both encoded output and line feed characters inserted for formatting.

## Definition
```c
static uint64 pg_base64_enc_len(const char *src, size_t srclen)
```

## Detailed Description
This function computes the total buffer size needed to encode binary data into Base64 format with line breaks. Base64 encoding converts every 3 bytes of input into 4 bytes of output, with additional line feed characters inserted every 76 characters for proper formatting according to Base64 standards.

The calculation includes:
1. Base encoded length: ((srclen + 2) / 3) * 4 - converts groups of 3 bytes to 4 Base64 characters
2. Line feed overhead: srclen / (76 * 3 / 4) - adds line breaks every 76 output characters

## Parameters / Member Variables
- `src`: Input buffer containing binary data to be encoded (parameter unused in calculation)
- `srclen`: Length of the input data in bytes

## Dependencies
- Functions called/Symbols referenced: None
- Called from (representative examples):
  - esc_dec_len (indirectly referenced)

## Notes and Other Information
- This is a static utility function used internally for memory allocation planning
- The function performs integer arithmetic to avoid floating point operations
- Line breaks are inserted every 76 characters to comply with MIME Base64 encoding standards
- The calculation accounts for partial groups at the end of input data by adding 2 to srclen before division