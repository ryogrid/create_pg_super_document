# pg_base64_dec_len

## Location
[src/backend/utils/adt/encode.c:392-410](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/encode.c#L392-L410)

## Overview
Calculates the maximum possible output buffer length needed for Base64 decoding of encoded data.

## Definition
```c
static uint64 pg_base64_dec_len(const char *src, size_t srclen)
```

## Detailed Description
This function computes the upper bound for the buffer size needed to decode Base64-encoded data back to binary format. Base64 encoding converts every 4 characters of encoded input back to 3 bytes of binary output. The calculation uses bit shifting for efficient division by 4 and multiplication by 3.

The formula (srclen * 3) >> 2 is equivalent to (srclen * 3) / 4, which accounts for the 4:3 ratio of Base64 decoding while using efficient bitwise operations.

## Parameters / Member Variables
- `src`: Input buffer containing Base64-encoded data (parameter unused in calculation)
- `srclen`: Length of the encoded input data in bytes

## Dependencies
- Functions called/Symbols referenced: None
- Called from (representative examples):
  - [esc_dec_len](../e/esc_dec_len.md) (indirectly referenced)

## Notes and Other Information
- This is a static utility function used internally for memory allocation planning
- Returns the maximum possible decoded length; actual decoded length may be shorter due to padding
- Uses bit shifting (>> 2) instead of division for better performance
- The calculation provides an upper bound estimate and doesn't account for whitespace or padding characters that might be present in the encoded input
- Used as part of the broader encoding/decoding infrastructure in PostgreSQL

## Simplified Source

```c
static uint64 pg_base64_dec_len(const char *src, size_t srclen) {
    // Base64: 4 bytes input -> 3 bytes output
    return ((uint64) srclen * 3) >> 2;  // Efficient multiply by 3/4
}
```