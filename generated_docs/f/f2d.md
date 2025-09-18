# f2d

## Location
src/common/f2s.c: 222 - 439

## Overview
Core function of the Ryu algorithm that converts IEEE 754 single-precision floating-point representation to an exact decimal representation.

## Definition
```c
static inline floating_decimal_32 f2d(const uint32 ieeeMantissa, const uint32 ieeeExponent)
```

## Detailed Description
This function implements the main conversion algorithm from IEEE 754 binary floating-point format to decimal format. It takes the mantissa and exponent components of a single-precision float and returns a `floating_decimal_32` structure containing the exact decimal representation.

The algorithm follows these key steps:
1. **Normalization**: Handle subnormal numbers and add the implicit leading bit for normal numbers
2. **Interval Determination**: Calculate the interval of legal decimal representations using bounds [vm, vp] with vr as the reference value
3. **Decimal Power Base Conversion**: Convert from binary to decimal using either powers of 5 division (for positive exponents) or powers of 5 multiplication (for negative exponents)
4. **Shortest Representation**: Find the shortest decimal representation within the legal interval, handling special cases for trailing zeros

The function handles two main cases based on the binary exponent:
- **e2 >= 0**: Uses inverse powers of 5 with `mulPow5InvDivPow2`
- **e2 < 0**: Uses powers of 5 with `mulPow5divPow2`

## Parameters / Member Variables
- `ieeeMantissa`: The mantissa bits from the IEEE 754 representation (23 bits for single precision)
- `ieeeExponent`: The exponent bits from the IEEE 754 representation (8 bits for single precision)

## Dependencies
- Functions called/Symbols referenced:
  - [log10Pow2](../l/log10Pow2.md), log10Pow5 (logarithm calculations)
  - [pow5bits](../p/pow5bits.md) (bit count for powers of 5)
  - [mulPow5InvDivPow2](../m/mulPow5InvDivPow2.md), mulPow5divPow2 (multiplication/division operations)
  - [multipleOfPowerOf5](../m/multipleOfPowerOf5.md), multipleOfPowerOf2 (divisibility checks)
  - Constants: FLOAT_BIAS, FLOAT_MANTISSA_BITS, FLOAT_POW5_INV_BITCOUNT, FLOAT_POW5_BITCOUNT
  - STRICTLY_SHORTEST macro for precision control
- Called from (representative examples):
  - [float_to_shortest_decimal_bufn](float_to_shortest_decimal_bufn.md) (at src/common/f2s.c:766)

## Notes and Other Information
- This is an inline static function optimized for performance
- The algorithm guarantees that the output can be exactly converted back to the original float (round-trip property)
- Uses 64-bit arithmetic internally for precision while working with 32-bit inputs and outputs
- Handles special cases including trailing zeros, rounding modes, and boundary conditions
- The `acceptBounds` variable controls whether to include boundary values in the shortest representation
- Contains extensive performance optimizations, including specialized handling for common cases (~96% of inputs)
- Part of the Ryu algorithm which provides faster conversion than traditional approaches while maintaining exact results
- Returns a `floating_decimal_32` structure with mantissa and exponent fields representing the decimal value as mantissa × 10^exponent