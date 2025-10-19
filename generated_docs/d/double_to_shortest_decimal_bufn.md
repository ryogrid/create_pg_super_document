# double_to_shortest_decimal_bufn

## Location
[src/common/d2s.c:1015-1052](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/d2s.c#L1015-L1052)

## Overview
Converts a double-precision floating-point number to its shortest decimal representation as an unterminated string, implementing the core algorithm for efficient double-to-string conversion.

## Definition

```c
int
double_to_shortest_decimal_bufn(double f, char *result)
```
## Detailed Description
This function performs the core conversion of IEEE 754 double-precision floating-point numbers to their shortest decimal string representation. It implements an optimized algorithm that:

1. Decodes the IEEE 754 bit representation into sign, mantissa, and exponent components
2. Handles special cases (infinity, NaN, zero) early for efficiency
3. Uses specialized small integer optimization for common cases
4. Falls back to the general d2d (double-to-decimal) algorithm for complex cases
5. Formats the result using an optimized character output routine

The function stores the result as an unterminated string to avoid unnecessary null termination overhead when the caller will process the string further.

## Parameters / Member Variables
- `f`: The double-precision floating-point number to convert
- `*result`: Caller-provided buffer to store the decimal string (must be at least DOUBLE_SHORTEST_DECIMAL_LEN-1 bytes)
## Dependencies
- Functions called/Symbols referenced:
  - : Extracts IEEE 754 bit representation
  - : Handles special values (infinity, NaN, zero)
  - : Optimized conversion for small integers
  - : General double-to-decimal conversion algorithm
  - : Formats the floating decimal as character string
  - : Constant defining mantissa bit count
  - : Constant defining exponent bit count
  - : Structure for intermediate decimal representation
- Called from:
  - : Higher-level wrapper function

## Notes and Other Information
- Returns the number of bytes stored in the result buffer
- The result string is NOT null-terminated; callers must handle termination
- Buffer must be pre-allocated by caller with sufficient space
- Implements the Ryu algorithm for efficient double-to-string conversion
- Handles both normalized and subnormal floating-point numbers uniformly
- Special cases (±infinity, NaN, ±0) are processed early for performance

## Simplified Source

```c
int double_to_shortest_decimal_bufn(double f, char *result) {
    // Step 1: Extract IEEE 754 bit representation
    const uint64 bits = double_to_bits(f);

    // Decode into sign, mantissa, and exponent components
    const bool ieeeSign = ((bits >> (DOUBLE_MANTISSA_BITS + DOUBLE_EXPONENT_BITS)) & 1) != 0;
    const uint64 ieeeMantissa = bits & ((UINT64CONST(1) << DOUBLE_MANTISSA_BITS) - 1);
    const uint32 ieeeExponent = (uint32)((bits >> DOUBLE_MANTISSA_BITS) & ((1u << DOUBLE_EXPONENT_BITS) - 1));

    // Handle special cases early: infinity, NaN, and zero
    if (ieeeExponent == ((1u << DOUBLE_EXPONENT_BITS) - 1u) ||
        (ieeeExponent == 0 && ieeeMantissa == 0)) {
        return copy_special_str(result, ieeeSign, (ieeeExponent != 0), (ieeeMantissa != 0));
    }

    floating_decimal_64 v;

    // Try small integer optimization first (common case)
    const bool isSmallInt = d2d_small_int(ieeeMantissa, ieeeExponent, &v);

    if (!isSmallInt) {
        // Use general double-to-decimal algorithm for complex cases
        v = d2d(ieeeMantissa, ieeeExponent);
    }

    // Convert the decimal representation to character string
    return to_chars(v, ieeeSign, result);
}
```