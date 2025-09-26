# d2d_small_int

## Location
[src/common/d2s.c:962-1014](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/d2s.c#L962-L1014)

## Overview
Optimized fast path for converting IEEE 754 double-precision floating-point numbers that represent small integers to decimal form without expensive arithmetic operations.

## Definition
```c
static inline bool d2d_small_int(const uint64 ieeeMantissa, const uint32 ieeeExponent, floating_decimal_64 *v)
```

## Detailed Description
This function provides a performance optimization for the common case where a double-precision floating-point number represents a small integer value. It detects when the IEEE 754 representation corresponds to an exact integer in the range [1, 2^53) and converts it directly to decimal form using simple bit operations rather than the complex general-purpose algorithm.

The optimization works by:
1. **Range Check**: Verifying the exponent is in the range where integer representation is possible
2. **Fraction Test**: Checking if the fractional part is exactly zero by masking the lower bits
3. **Direct Conversion**: If conditions are met, directly computing the integer value by shifting the normalized mantissa

This fast path is particularly important for PostgreSQL applications where integer-valued doubles are common (such as in JSON numeric processing or when doubles store what are conceptually integer values).

The function handles the edge case properly by using the IEEE mantissa directly for fraction testing, since the implied leading 1 bit cannot affect the fractional part determination for the exponent ranges being tested.

## Parameters / Member Variables
- `ieeeMantissa`: The 52-bit mantissa portion of the IEEE 754 double (without the implicit leading 1 for normal numbers)
- `ieeeExponent`: The 11-bit exponent portion of the IEEE 754 double (biased by 1023)  
- `v`: Output parameter - pointer to floating_decimal_64 structure to store the result

## Dependencies
- Functions called/Symbols referenced:
  - DOUBLE_BIAS: IEEE 754 double exponent bias constant (1023)
  - DOUBLE_MANTISSA_BITS: Number of mantissa bits in IEEE 754 double (52)
  - [floating_decimal_64](../f/floating_decimal_64.md): Output structure type
- Called from (representative examples):
  - [double_to_shortest_decimal_bufn](double_to_shortest_decimal_bufn.md)

## Notes and Other Information
- The function is marked as `static inline` for performance optimization
- Returns `true` if the optimization was applied, `false` if the general algorithm should be used
- Handles integers in the range [1, 2^53) which covers all exactly representable integers in double precision
- The resulting mantissa may contain trailing decimal zeros, which is acceptable since this represents an exact integer
- Uses careful bit manipulation to avoid multiple return paths, which helps prevent compiler from creating multiple inline copies of the general d2d function
- The optimization is mathematically sound: for exponents in the valid range, if the fractional bits are zero, the value is guaranteed to be an exact integer
- No decimal length adjustment is needed since 2^53 < 10^16, ensuring the result fits within expected decimal digit limits