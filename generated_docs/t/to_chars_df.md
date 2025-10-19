# to_chars_df

## Location
[src/common/d2s.c:631-786](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/d2s.c#L631-L786)

## Overview
Converts a floating-point decimal representation to its fixed-point string format, handling decimal point placement and zero padding as needed.

## Definition
```c
static inline int to_chars_df(const floating_decimal_64 v, const uint32 olength, char *const result)
```

## Detailed Description
This function takes a decimal floating-point representation (mantissa and exponent) and converts it to a fixed-point string format suitable for human reading. It handles the complex logic of placing the decimal point in the correct position based on the exponent value, and manages zero padding for various formatting scenarios.

The function supports several output formats depending on the exponent:
- Large positive exponents: `ddddddddd000000` (trailing zeros)
- Zero exponent: `ddddddddd` (no decimal point needed)
- Small negative exponents: `dddddddd.d` to `d.ddddddddd` (decimal point within digits)
- Large negative exponents: `0.ddddddddd` to `0.000dddddd` (leading zeros after decimal)

The implementation is optimized for performance by:
- Using 32-bit arithmetic when possible, even on 64-bit platforms
- Processing digits in chunks using a precomputed digit table
- Handling large numbers by dividing by 10^8 first to fit remaining digits in 32 bits
- Using efficient memory operations for digit placement and movement

## Parameters / Member Variables
- `v`: A floating_decimal_64 structure containing the mantissa and exponent of the decimal representation
- `olength`: The length of the output digits (number of significant digits)
- `result`: Character buffer where the formatted string will be written

## Dependencies
- Functions called/Symbols referenced:
  - [div1e8](../d/div1e8.md): Fast division by 10^8 for handling large numbers
  - [floating_decimal_64](../f/floating_decimal_64.md): Input structure type
  - DIGIT_TABLE: Precomputed table for efficient digit pair conversion
- Called from (representative examples):
  - [to_chars](to_chars.md)

## Notes and Other Information
- The function is marked as `static inline` for performance optimization
- Returns the final length of the formatted string
- Assumes the caller has already handled the negative sign if needed
- Uses efficient bit manipulation and table lookups for digit conversion
- Includes assertions to validate expected ranges and prevent buffer overflows
- The decimal point placement logic handles edge cases where scientific notation would normally be used
- Memory operations are optimized using memcpy, memmove, and memset for bulk operations
- The implementation prioritizes 32-bit operations for better performance on most architectures

## Simplified Source

```c
static inline int to_chars_df(const floating_decimal_64 v, const uint32 olength, char *const result) {
    // Convert decimal floating-point to fixed-point string format

    int index = 0;
    uint64 output = v.mantissa;
    int32 exp = v.exponent;
    int32 nexp = exp + olength;  // Net exponent (where decimal point goes)

    // Determine output format and prepare buffer
    if (nexp <= 0) {
        // Format: 0.000ddddd (leading zeros after decimal point)
        index = 2 - nexp;  // Account for "0." and leading zeros
        memcpy(result, "0.000000", 8);
    } else if (exp < 0) {
        // Format: dddd.dddd (decimal point within digits)
        index = 1;  // Leave space to move decimal point later
    } else {
        // Format: ddddddddd000000 (trailing zeros, no decimal point)
        memset(result, '0', 16);  // Pre-fill with zeros
    }

    // Convert mantissa to digits, working from right to left
    uint32 i = 0;

    // Handle large numbers (>32 bits) by processing 8 digits first
    if ((output >> 32) != 0) {
        const uint64 q = div1e8(output);  // Divide by 10^8
        uint32 output2 = (uint32)(output - 100000000 * q);

        // Process 8 digits in two 4-digit chunks
        const uint32 c = output2 % 10000;
        output2 /= 10000;
        const uint32 d = output2 % 10000;

        // Convert digits using lookup table (2 digits at a time)
        memcpy(result + index + olength - i - 2, DIGIT_TABLE + ((c % 100) << 1), 2);
        memcpy(result + index + olength - i - 4, DIGIT_TABLE + ((c / 100) << 1), 2);
        memcpy(result + index + olength - i - 6, DIGIT_TABLE + ((d % 100) << 1), 2);
        memcpy(result + index + olength - i - 8, DIGIT_TABLE + ((d / 100) << 1), 2);

        output = q;
        i += 8;
    }

    // Process remaining digits (now fits in 32 bits)
    uint32 output2 = (uint32)output;

    // Process 4 digits at a time
    while (output2 >= 10000) {
        const uint32 c = output2 % 10000;
        memcpy(result + index + olength - i - 2, DIGIT_TABLE + ((c % 100) << 1), 2);
        memcpy(result + index + olength - i - 4, DIGIT_TABLE + ((c / 100) << 1), 2);
        output2 /= 10000;
        i += 4;
    }

    // Process 2 digits at a time
    if (output2 >= 100) {
        const uint32 c = (output2 % 100) << 1;
        memcpy(result + index + olength - i - 2, DIGIT_TABLE + c, 2);
        output2 /= 100;
        i += 2;
    }

    // Process final 1-2 digits
    if (output2 >= 10) {
        const uint32 c = output2 << 1;
        memcpy(result + index + olength - i - 2, DIGIT_TABLE + c, 2);
    } else {
        result[index] = (char)('0' + output2);
    }

    // Handle decimal point placement
    if (index == 1) {
        // Move digits and insert decimal point for dddd.dddd format
        // Use bit manipulation for efficient copying
        if (nexp & 8) { memmove(result + index - 1, result + index, 8); index += 8; }
        if (nexp & 4) { memmove(result + index - 1, result + index, 4); index += 4; }
        if (nexp & 2) { memmove(result + index - 1, result + index, 2); index += 2; }
        if (nexp & 1) { result[index - 1] = result[index]; }

        result[nexp] = '.';
        index = olength + 1;
    } else if (exp >= 0) {
        // Trailing zeros format - set final length
        index = olength + exp;
    } else {
        // Leading zeros format - account for "0." and zeros
        index = olength + (2 - nexp);
    }

    return index;  // Return total string length
}
```