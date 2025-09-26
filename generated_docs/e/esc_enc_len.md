# esc_enc_len

## Location
[src/backend/utils/adt/encode.c:502-522](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/encode.c#L502-L522)

## Overview
Calculates the required buffer length for escape sequence encoding of binary data, accounting for character expansion during the encoding process.

## Definition
```c
static uint64 esc_enc_len(const char *src, size_t srclen)
```

## Detailed Description
This function computes the exact buffer size needed to encode binary data using PostgreSQL's escape sequence format. It analyzes each byte in the input to determine how many bytes it will require in the encoded output:

1. Null bytes (\0) and high-bit characters (>= 128) require 4 bytes each (\\nnn octal format)
2. Backslash characters (\\) require 2 bytes each (doubled to \\\\)
3. All other characters require 1 byte each (no change)

The calculation ensures precise memory allocation for the encoding operation, preventing buffer overruns while avoiding waste.

## Parameters / Member Variables
- `src`: Input buffer containing binary data to be analyzed for encoding length
- `srclen`: Length of the input data in bytes

## Dependencies
- Functions called/Symbols referenced:
  - IS_HIGHBIT_SET (macro to check if high bit is set in a character)
- Called from (representative examples):
  - esc_dec_len (indirectly referenced)

## Notes and Other Information
- This is a static utility function used internally for memory allocation planning
- Must be called before esc_encode to determine the required output buffer size
- The function performs a complete scan of the input data to provide an exact length calculation
- Returns the precise number of bytes needed, not an upper bound estimate
- Critical for proper memory management in PostgreSQL's binary data encoding operations
- Used as part of the bytea data type implementation and other binary data handling routines