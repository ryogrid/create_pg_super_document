# decimalLength

## Location
[src/common/f2s.c:174-214](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/f2s.c#L174-L214)

## Overview
Efficiently calculates the number of decimal digits in a 32-bit unsigned integer value, optimized for floating-point to string conversion.

## Definition
```c
static inline uint32 decimalLength(const uint32 v)
```

## Detailed Description
This function determines the number of decimal digits required to represent a given 32-bit unsigned integer. It uses a cascaded series of if-statements rather than a loop for better performance. The function is optimized for the typical range of values encountered during float-to-string conversion, checking from higher values to lower values since the expected output patterns favor longer numbers.

The function includes a precondition that the input value must be less than 1,000,000,000 (10^9), as 9 digits are sufficient for round-tripping single-precision floating-point numbers. This constraint is enforced with an assertion.

## Parameters / Member Variables
- `v`: The 32-bit unsigned integer value for which to count decimal digits (must be < 1,000,000,000)

## Dependencies
- Functions called/Symbols referenced:
  - Assert (for precondition checking)
- Called from (representative examples):
  - [to_chars](../t/to_chars.md) (in f2s.c at line 569)
  - [to_chars](../t/to_chars.md) (in d2s.c at line 793)

## Notes and Other Information
- This is an inline static function for performance optimization
- The cascaded if-statement approach is faster than using logarithms or loops for this specific use case
- The function is designed specifically for the float-to-string conversion context where values are limited to what can be exactly represented in single precision
- The precondition limiting input to < 10^9 is based on the mathematical properties of IEEE 754 single-precision floating-point representation
- Part of the Ryu algorithm implementation for fast floating-point to decimal conversion

## Simplified Source

```c
static inline uint32 decimalLength(const uint64 v) {
    // Count decimal digits by checking against powers of 10
    // Optimized cascade for better performance than loops
    // Note: Function precondition: v < 100000000000000000L (17 digits max)

    if (v >= 10000000000000000L) return 17;
    if (v >= 1000000000000000L)  return 16;
    if (v >= 100000000000000L)   return 15;
    if (v >= 10000000000000L)    return 14;
    if (v >= 1000000000000L)     return 13;
    if (v >= 100000000000L)      return 12;
    if (v >= 10000000000L)       return 11;
    if (v >= 1000000000L)        return 10;
    if (v >= 100000000L)         return 9;
    if (v >= 10000000L)          return 8;
    if (v >= 1000000L)           return 7;
    if (v >= 100000L)            return 6;
    if (v >= 10000L)             return 5;
    if (v >= 1000L)              return 4;
    if (v >= 100L)               return 3;
    if (v >= 10L)                return 2;
    return 1;
}
```