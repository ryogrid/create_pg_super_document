# hex_dec_len

## Location
[src/backend/utils/adt/encode.c:243-266](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/encode.c#L243-L266)

## Overview
Calculates the required buffer length for decoding hexadecimal-encoded data into binary format.

## Definition
```c
static uint64 hex_dec_len(const char *src, size_t srclen)
```

## Detailed Description
This utility function computes the output buffer size needed to store binary data decoded from a hexadecimal string. Since each pair of hexadecimal digits represents one byte of binary data, the function performs a right bit shift by 1 position, which is equivalent to dividing by 2. This calculation assumes that the input hexadecimal string has an even number of digits (complete pairs). The function is marked as static, indicating it is for internal use within the encode.c compilation unit.

## Parameters / Member Variables
- `src`: Pointer to the source hexadecimal string (parameter name present but not used in calculation)
- `srclen`: Length of the source hexadecimal string in bytes

## Dependencies
- Functions called/Symbols referenced:
  - int8 (referenced in related code context)
- Called from (representative examples):
  - [esc_dec_len](../e/esc_dec_len.md) (escape decoder length calculation function)

## Notes and Other Information
- Returns the calculated length as uint64 to handle large data sizes
- Uses bit shifting (`>> 1`) instead of division for efficiency
- The src parameter is not actually used in the calculation, only srclen is needed
- Assumes input has an even number of hexadecimal digits (does not validate this)
- Part of PostgreSQL's encoding/decoding subsystem in src/backend/utils/adt/encode.c
- Function is static (internal linkage) and serves as a utility for decoding operations
- Does not perform any validation on input parameters - caller must ensure proper input format

## Simplified Source

```c
static uint64 hex_dec_len(const char *src, size_t srclen) {
    // Each pair of hex digits becomes 1 byte, so divide by 2
    return (uint64) srclen >> 1;  // Efficient bit shift instead of / 2
}
```