# f2d_small_int

## Location
[src/common/f2s.c:689-741](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/f2s.c#L689-L741)

## Overview
A static inline function that handles the conversion of IEEE 754 single-precision floating-point numbers to decimal representation when the number represents a small integer value.

## Definition
```c
static inline bool f2d_small_int(const uint32 ieeeMantissa, const uint32 ieeeExponent, floating_decimal_32 *v)
```

## Detailed Description
This function is part of the Ryu floating-point to string conversion algorithm. It specifically optimizes the case where a float value represents an exact small integer. The function checks if the given IEEE 754 representation corresponds to an integer value in the range [1, 2^24) and, if so, directly computes the decimal representation without going through the full Ryu algorithm.

The function performs bit manipulation to determine if the fractional part is zero by checking if the lower bits (determined by the exponent) of the mantissa are all zero. If they are, the number is an exact integer and can be converted directly.

## Parameters / Member Variables
- `ieeeMantissa`: The 23-bit mantissa portion of the IEEE 754 single-precision float
- `ieeeExponent`: The 8-bit exponent portion of the IEEE 754 single-precision float  
- `v`: Output parameter - pointer to floating_decimal_32 structure to store the result

## Dependencies
- Functions called/Symbols referenced:
  - [floating_decimal_32](floating_decimal_32.md) (struct type)
  - FLOAT_BIAS (constant)
  - FLOAT_MANTISSA_BITS (constant)
- Called from:
  - [float_to_shortest_decimal_bufn](float_to_shortest_decimal_bufn.md)

## Notes and Other Information
- This is an optimization path in the Ryu algorithm for handling small integers efficiently
- The function returns true if the value was successfully converted as a small integer, false otherwise
- When successful, it sets v->mantissa to the integer value and v->exponent to 0
- The algorithm avoids multiple return points to prevent compiler from creating multiple inlined copies
- Only handles cases where the exponent allows for exact integer representation (e2 >= -FLOAT_MANTISSA_BITS && e2 <= 0)

## Simplified Source

```c
static inline bool
f2d_small_int(const uint32 ieeeMantissa, const uint32 ieeeExponent, floating_decimal_32 *v)
{
    // Calculate binary exponent
    const int32 e2 = (int32)ieeeExponent - FLOAT_BIAS - FLOAT_MANTISSA_BITS;

    // Check if this could be a small integer (exponent in valid range)
    if (e2 >= -FLOAT_MANTISSA_BITS && e2 <= 0) {
        // Check if fractional part is zero (all lower bits are 0)
        const uint32 mask = (1U << -e2) - 1;  // Mask for fractional bits
        const uint32 fraction = ieeeMantissa & mask;

        if (fraction == 0) {
            // No fractional part - this is an exact integer
            // Add back the implicit leading 1 bit
            const uint32 m2 = (1U << FLOAT_MANTISSA_BITS) | ieeeMantissa;

            // Calculate the integer value by right-shifting
            v->mantissa = m2 >> -e2;
            v->exponent = 0;  // Integer has zero decimal exponent
            return true;
        }
    }

    return false;  // Not a small integer
}
```