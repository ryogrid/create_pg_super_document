# float_to_bits

## Location
src/common/ryu_common.h: 116 - 124

## Overview
Safely extracts the raw IEEE 754 bit representation of a single-precision (32-bit) floating-point value as an unsigned integer.

## Definition
```c
static inline uint32 float_to_bits(const float f)
```

## Detailed Description
This function converts a float to its underlying 32-bit IEEE 754 binary representation without invoking undefined behavior. It uses memcpy to perform a safe type-punning operation, copying the bytes of the float directly into a uint32 variable. This approach avoids potential undefined behavior that could result from using pointer casting or union-based type punning, ensuring portability across different compilers and architectures.

The function is essential for floating-point algorithms that need to examine or manipulate the individual bit fields (sign, exponent, mantissa) of IEEE 754 floating-point numbers.

## Parameters / Member Variables
- `f`: The single-precision floating-point value to convert to its bit representation

## Dependencies
- Functions called/Symbols referenced:
  - memcpy (for safe type conversion)
  - sizeof (for determining float size)
- Called from (representative examples):
  - float_to_shortest_decimal_bufn (in src/common/f2s.c at line 748)

## Notes and Other Information
- Uses memcpy for standards-compliant type punning, avoiding undefined behavior of direct pointer casting
- Returns the exact IEEE 754 binary32 bit pattern as a 32-bit unsigned integer
- Essential for algorithms that need to examine sign bit (bit 31), exponent field (bits 30-23), and mantissa field (bits 22-0)
- Part of the Ryu algorithm infrastructure for fast floating-point to string conversion
- Provides a portable way to access float bit patterns across different architectures and compilers
- Complements similar double-precision conversion functions in the same codebase
- Located in src/common/ryu_common.h:116-124