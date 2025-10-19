# float_to_shortest_decimal_bufn

## Location
[src/common/f2s.c:742-779](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/f2s.c#L742-L779)

## Overview
Converts a single-precision floating-point number to its shortest decimal string representation and stores it in a caller-supplied buffer without null termination.

## Definition
```c
int float_to_shortest_decimal_bufn(float f, char *result)
```

## Detailed Description
This function is the core implementation of the Ryu algorithm for converting IEEE 754 single-precision floating-point numbers to their shortest decimal string representation. It follows a multi-step process:

1. **Bit Extraction**: Decodes the float into its IEEE 754 components (sign, mantissa, exponent)
2. **Special Case Handling**: Handles special values like infinity, NaN, and zero early
3. **Optimization Path**: Attempts to use the optimized small integer path via f2d_small_int()  
4. **Full Conversion**: Falls back to the full Ryu algorithm f2d() if not a small integer
5. **String Generation**: Converts the computed decimal representation to a character string

The function produces an unterminated string (no null character) in the provided buffer and returns the number of characters written.

## Parameters / Member Variables
- `f`: The single-precision floating-point number to convert
- `result`: Output buffer that must be at least FLOAT_SHORTEST_DECIMAL_LEN-1 bytes long

## Dependencies
- Functions called/Symbols referenced:
  - [float_to_bits](float_to_bits.md) (bit manipulation utility)
  - [copy_special_str](../c/copy_special_str.md) (handles special values)
  - [f2d_small_int](f2d_small_int.md) (optimization for small integers) 
  - [f2d](f2d.md) (main Ryu conversion algorithm)
  - [to_chars](../t/to_chars.md) (converts decimal representation to string)
  - [floating_decimal_32](floating_decimal_32.md) (result structure)
  - FLOAT_MANTISSA_BITS, FLOAT_EXPONENT_BITS (constants)
- Called from:
  - [float_to_shortest_decimal_buf](float_to_shortest_decimal_buf.md)
  - FLOAT_SHORTEST_DECIMAL_LEN (macro context)

## Notes and Other Information
- Part of the Ryu floating-point to string conversion algorithm implementation
- The result buffer is NOT null-terminated - callers must handle termination if needed
- Buffer size requirement is FLOAT_SHORTEST_DECIMAL_LEN-1 bytes minimum
- Returns the actual number of characters written, allowing for proper string handling
- Handles all IEEE 754 single-precision values including special cases (±0, ±∞, NaN)
- Uses optimizations for common cases like small integers to improve performance

## Simplified Source

```c
int
float_to_shortest_decimal_bufn(float f, char *result)
{
    // Step 1: Extract IEEE 754 components
    const uint32 bits = float_to_bits(f);
    const bool ieeeSign = ((bits >> (FLOAT_MANTISSA_BITS + FLOAT_EXPONENT_BITS)) & 1) != 0;
    const uint32 ieeeMantissa = bits & ((1u << FLOAT_MANTISSA_BITS) - 1);
    const uint32 ieeeExponent = (bits >> FLOAT_MANTISSA_BITS) & ((1u << FLOAT_EXPONENT_BITS) - 1);

    // Step 2: Handle special cases (infinity, NaN, zero)
    if (ieeeExponent == ((1u << FLOAT_EXPONENT_BITS) - 1u) || (ieeeExponent == 0 && ieeeMantissa == 0)) {
        return copy_special_str(result, ieeeSign, (ieeeExponent != 0), (ieeeMantissa != 0));
    }

    // Step 3: Try optimized small integer conversion first
    floating_decimal_32 v;
    const bool isSmallInt = f2d_small_int(ieeeMantissa, ieeeExponent, &v);

    // Step 4: Use full Ryu algorithm if not a small integer
    if (!isSmallInt) {
        v = f2d(ieeeMantissa, ieeeExponent);
    }

    // Step 5: Convert decimal representation to string
    return to_chars(v, ieeeSign, result);
}
```