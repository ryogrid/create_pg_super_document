# pg_popcount_fast

## Location
[src/port/pg_bitutils.c:249-294](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/pg_bitutils.c#L249-L294)

## Overview
An optimized population count function that efficiently counts the number of set bits in a buffer using hardware-accelerated instructions and alignment-aware processing.

## Definition
```c
static uint64 pg_popcount_fast(const char *buf, int bytes)
```

## Detailed Description
This function provides a fast implementation for counting the total number of 1-bits in a buffer of arbitrary size. It uses a multi-stage approach for optimal performance: first, it processes data in aligned 64-bit or 32-bit chunks (depending on platform architecture) using hardware popcount instructions, then handles any remaining bytes using a lookup table approach. The function automatically detects buffer alignment and chooses the most efficient processing method. On 64-bit platforms, it prioritizes 8-byte aligned processing, while on 32-bit platforms it uses 4-byte aligned processing.

## Parameters / Member Variables
- `buf`: Pointer to the buffer containing the data to count bits in
- `bytes`: Number of bytes in the buffer to process

## Dependencies
- Functions called/Symbols referenced:
  - [pg_popcount64_fast](pg_popcount64_fast.md) (for 64-bit chunk processing)
  - [pg_popcount32_fast](pg_popcount32_fast.md) (for 32-bit chunk processing)
  - TYPEALIGN (for alignment checking)
  - pg_number_of_ones (lookup table for remaining bytes)
- Called from (representative examples):
  - [choose_popcount_functions](../c/choose_popcount_functions.md)

## Notes and Other Information
- Uses conditional compilation based on SIZEOF_VOID_P to optimize for platform architecture
- Implements alignment-aware processing to maximize hardware instruction efficiency
- Falls back to lookup table processing for unaligned or remaining bytes
- Part of PostgreSQL's dynamic function selection mechanism for bit manipulation
- Returns uint64 to accommodate large bit counts from substantial buffers
- Static function used internally within the popcount optimization framework

## Simplified Source

```c
static uint64 pg_popcount_fast(const char *buf, int bytes) {
    uint64 total_bits = 0;

    // Process aligned chunks efficiently
    if (buf == (const char *) TYPEALIGN(8, buf)) {
        const uint64 *words = (const uint64 *) buf;
        while (bytes >= 8) {
            total_bits += pg_popcount64_fast(*words++);
            bytes -= 8;
        }
        buf = (const char *) words;
    }

    // Process remaining bytes using lookup table
    while (bytes--)
        total_bits += pg_number_of_ones[(unsigned char) *buf++];

    return total_bits;
}
```