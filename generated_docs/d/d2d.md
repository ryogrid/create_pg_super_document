# d2d

## Location
[src/common/d2s.c:346-630](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/d2s.c#L346-L630)

## Overview
Converts IEEE 754 double-precision floating-point components (mantissa and exponent) into the shortest decimal representation that can round-trip back to the original double value.

## Definition
```c
static inline floating_decimal_64 d2d(const uint64 ieeeMantissa, const uint32 ieeeExponent)
```

## Detailed Description
This function implements the core algorithm for converting IEEE 754 double-precision floating-point numbers to their shortest decimal representation. It takes the raw mantissa and exponent components of a double and produces a decimal representation with the minimum number of digits necessary to uniquely identify the original floating-point value.

The algorithm works in several key steps:
1. Normalizes the input mantissa and exponent, accounting for subnormal numbers
2. Determines the interval of legal decimal representations using bounds computation
3. Converts to decimal using 128-bit arithmetic with precomputed power-of-5 tables
4. Finds the shortest representation within the legal interval using digit removal optimization

The implementation includes sophisticated optimizations such as:
- Special handling for trailing zeros to minimize output length
- Fast division by 10 and 100 using multiplication and shifting
- Conditional logic to handle different rounding scenarios
- Performance optimizations that remove multiple digits at once when possible

## Parameters / Member Variables
- `ieeeMantissa`: The 52-bit mantissa portion of the IEEE 754 double (without the implicit leading 1 for normal numbers)
- `ieeeExponent`: The 11-bit exponent portion of the IEEE 754 double (biased by 1023)

## Dependencies
- Functions called/Symbols referenced:
  - [log10Pow2](../l/log10Pow2.md): Computes floor(log₁₀(2^e))
  - [log10Pow5](../l/log10Pow5.md): Computes floor(log₁₀(5^e))  
  - [pow5bits](../p/pow5bits.md): Computes number of bits needed for 5^e
  - [mulShiftAll](../m/mulShiftAll.md): Performs 128-bit multiplication with shifting
  - [div5](div5.md), div10, div100: Fast division functions
  - [multipleOfPowerOf5](../m/multipleOfPowerOf5.md): Checks divisibility by powers of 5
  - [multipleOfPowerOf2](../m/multipleOfPowerOf2.md): Checks divisibility by powers of 2
  - DOUBLE_BIAS, DOUBLE_MANTISSA_BITS: IEEE 754 double constants
  - DOUBLE_POW5_INV_SPLIT, DOUBLE_POW5_SPLIT: Precomputed power tables
  - [floating_decimal_64](../f/floating_decimal_64.md): Return type structure
- Called from (representative examples):
  - [double_to_shortest_decimal_bufn](double_to_shortest_decimal_bufn.md)

## Notes and Other Information
- The function is marked as `static inline` for performance optimization
- Uses conditional compilation with STRICTLY_SHORTEST to control rounding behavior
- Implements the Ryu algorithm for fast and accurate floating-point to string conversion
- Handles both normal and subnormal IEEE 754 double values
- The algorithm guarantees the shortest decimal representation that round-trips correctly
- Performance is optimized for common cases, with the general case handling rare scenarios (~0.7%)
- Contains detailed comments explaining the mathematical reasoning behind various optimizations