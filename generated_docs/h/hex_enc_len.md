# hex_enc_len

## Location
[src/backend/utils/adt/encode.c:237-242](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/encode.c#L237-L242)

## Overview
Calculates the required buffer length for hexadecimal encoding of binary data.

## Definition
```c
static uint64 hex_enc_len(const char *src, size_t srclen)
```

## Detailed Description
This is a simple utility function that computes the output buffer size needed to store the hexadecimal representation of binary data. Since each byte of binary data requires exactly two hexadecimal digits to represent (one for each 4-bit nibble), the function performs a left bit shift by 1 position, which is equivalent to multiplying by 2. The function is marked as static, indicating it is intended for internal use within the encode.c compilation unit.

## Parameters / Member Variables
- `src`: Pointer to the source binary data (parameter name present but not used in calculation)
- `srclen`: Length of the source binary data in bytes

## Dependencies
- Functions called/Symbols referenced:
  - (None - simple arithmetic operation)
- Called from (representative examples):
  - [esc_dec_len](../e/esc_dec_len.md) (escape decoder length calculation function)

## Notes and Other Information
- Returns the calculated length as uint64 to handle large data sizes
- Uses bit shifting (`<< 1`) instead of multiplication for efficiency
- The src parameter is not actually used in the calculation, only srclen is needed
- Part of PostgreSQL's encoding/decoding subsystem in src/backend/utils/adt/encode.c
- Function is static (internal linkage) and serves as a utility for encoding operations
- Does not perform any validation on input parameters