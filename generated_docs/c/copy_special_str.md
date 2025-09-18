# copy_special_str

## Location
src/common/ryu_common.h: 95 - 115

## Overview
Copies special floating-point value strings (NaN, Infinity, -Infinity, 0, -0) to a result buffer based on IEEE 754 floating-point bit patterns.

## Definition
```c
static inline int copy_special_str(char *const result, const bool sign, const bool exponent, const bool mantissa)
```

## Detailed Description
This function handles the string representation of special floating-point values by examining the IEEE 754 bit pattern components (sign, exponent, mantissa). It determines which special value to output based on the boolean flags and copies the appropriate string to the result buffer. The function returns the number of characters written to the buffer.

The logic follows IEEE 754 special value encoding:
- If mantissa is non-zero (NaN): outputs "NaN"
- If exponent is all 1s but mantissa is zero (Infinity): outputs "Infinity" or "-Infinity" 
- If exponent is all 0s (zero or subnormal): outputs "0" or "-0"

## Parameters / Member Variables
- `result`: Output character buffer where the special value string will be written
- `sign`: Boolean indicating if the sign bit is set (true for negative values)
- `exponent`: Boolean indicating if all exponent bits are set (true for infinity/NaN)
- `mantissa`: Boolean indicating if mantissa has non-zero bits (true for NaN)

## Dependencies
- Functions called/Symbols referenced:
  - memcpy (for efficient string copying)
- Called from (representative examples):
  - [double_to_shortest_decimal_bufn](../d/double_to_shortest_decimal_bufn.md) (in src/common/d2s.c at line 1031)
  - [float_to_shortest_decimal_bufn](../f/float_to_shortest_decimal_bufn.md) (in src/common/f2s.c at line 758)

## Notes and Other Information
- Part of the Ryu algorithm for fast floating-point to string conversion
- Efficiently handles all IEEE 754 special values in a single function
- Uses memcpy for fast string copying instead of character-by-character assignment
- The sign parameter affects the starting position for string placement when handling infinity/zero
- Returns the total length of the string written, including sign character if present
- Located in src/common/ryu_common.h:95-115