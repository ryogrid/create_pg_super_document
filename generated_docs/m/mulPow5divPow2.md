# mulPow5divPow2

## Location
[src/common/f2s.c:168-173](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/f2s.c#L168-L173)

## Overview
A helper function that multiplies a value by a power of 5 and then divides by a power of 2, used in floating-point to decimal string conversion.

## Definition
```c
static inline uint32 mulPow5divPow2(const uint32 m, const uint32 i, const int32 j)
```

## Detailed Description
This function performs the operation `m * 5^i / 2^j` efficiently using precomputed values. It leverages the `FLOAT_POW5_SPLIT` lookup table which contains precomputed values for powers of 5 split into appropriate bit patterns for efficient multiplication. The function is a thin wrapper around `mulShift` that selects the appropriate precomputed value from the lookup table.

This is part of the Ryu algorithm implementation for fast and accurate floating-point to string conversion, specifically handling the multiplication by powers of 5 that are needed during the conversion process.

## Parameters / Member Variables
- `m`: The mantissa value to be multiplied
- `i`: The index into the FLOAT_POW5_SPLIT table (power of 5 exponent)
- `j`: The right shift amount (effectively division by 2^j)

## Dependencies
- Functions called/Symbols referenced:
  - [mulShift](mulShift.md) (performs the actual multiplication and bit shifting)
  - FLOAT_POW5_SPLIT (lookup table containing precomputed power-of-5 values)
- Called from (representative examples):
  - [f2d](../f/f2d.md) (float-to-decimal conversion function, called 4 times at lines 320-322, 327)

## Notes and Other Information
- This is an inline static function for performance optimization
- The FLOAT_POW5_SPLIT table contains 47 precomputed 64-bit values representing powers of 5 in a format suitable for the mulShift operation
- Part of the Ryu algorithm, which provides exact and fast conversion of binary floating-point numbers to decimal strings
- The function is specifically designed for 32-bit float (single precision) conversion as indicated by the file name f2s.c (float-to-string)

## Simplified Source

```c
static inline uint32
mulPow5divPow2(const uint32 m, const uint32 i, const int32 j)
{
    // Multiply m by 5^i, then divide by 2^j
    // Uses precomputed powers of 5 lookup table
    return mulShift(m, FLOAT_POW5_SPLIT[i], j);
}
```