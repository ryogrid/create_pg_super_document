# to_chars

## Location
src/common/f2s.c: 563 - 688

## Overview
High-level function that converts a floating-point decimal representation to its complete string format, handling both fixed-point and scientific notation based on the exponent range.

## Definition
```c
static inline int to_chars(const floating_decimal_32 v, const bool sign, char *const result)
```

## Detailed Description
This function serves as the main string formatting entry point for single-precision floating-point numbers. It determines whether to use fixed-point notation or scientific notation based on the display exponent, and then delegates to the appropriate formatting function.

The function performs several key operations:
1. **Sign handling**: Adds a minus sign if the number is negative
2. **Format selection**: Chooses between fixed-point (-4 <= exp < 6) and scientific notation
3. **Trailing zero optimization**: For numbers that came through the small integer fast path, removes trailing decimal zeros and adjusts the display length
4. **Digit generation**: Converts the mantissa to decimal digits using optimized bulk processing
5. **Scientific notation formatting**: Adds decimal point and exponent for scientific format

The thresholds for format selection (-4 to +6) are chosen to match standard printf behavior, ensuring compatibility with existing applications.

## Parameters / Member Variables
- `v`: The floating_decimal_32 structure containing the mantissa and exponent of the decimal representation
- `sign`: Boolean flag indicating whether to prepend a minus sign
- `result`: The output character buffer where the formatted string will be written

## Dependencies
- Functions called/Symbols referenced:
  - [decimalLength](../d/decimalLength.md) (calculates number of digits in mantissa)
  - [to_chars_f](to_chars_f.md) (handles fixed-point formatting)
  - DIGIT_TABLE (lookup table for efficient digit generation)
  - memcpy (for copying digit pairs)
- Called from (representative examples):
  - [float_to_shortest_decimal_bufn](../f/float_to_shortest_decimal_bufn.md) (at src/common/f2s.c:769)

## Notes and Other Information
- This is an inline static function optimized for performance
- Returns the total length of the generated string including sign and exponent
- Uses the same DIGIT_TABLE optimization as other functions for fast digit pair generation
- Contains special logic to handle trailing zeros that may result from the small integer optimization path
- The scientific notation format includes explicit '+' for positive exponents (e.g., "1.23e+02")
- Implements the complete string generation for the final step of the Ryu algorithm
- For scientific notation, always places the decimal point after the first digit (normalized form)
- The function handles the boundary between fixed-point and scientific notation seamlessly
- Part of the complete float-to-string conversion pipeline in PostgreSQL's Ryu implementation