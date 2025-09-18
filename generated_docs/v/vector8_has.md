# vector8_has

## Location
src/include/port/simd.h: 162 - 194

## Overview
Tests whether any elements in a Vector8 SIMD register are equal to a given scalar uint8 value, providing efficient vectorized equality testing for byte data.

## Definition
```c
static inline bool vector8_has(const Vector8 v, const uint8 c)
```

## Detailed Description
This function performs a vectorized equality test to determine if any of the 8 bytes in the Vector8 register matches the specified scalar value. The implementation uses different strategies based on available SIMD capabilities:

- **SIMD-enabled path**: Uses `vector8_eq()` to generate equality masks, then `vector8_is_highbit_set()` to check if any comparison matched
- **Non-SIMD fallback**: Uses XOR operation with `vector8_broadcast(c)` followed by `vector8_has_zero()` to detect matches

The function includes comprehensive assertion checking in debug builds to verify correctness by comparing the SIMD result against a scalar reference implementation that iterates through each byte.

## Parameters / Member Variables
- `v`: The Vector8 register containing 8 uint8 values to search within
- `c`: The scalar uint8 value to search for within the vector

## Dependencies
- Functions called/Symbols referenced:
  - `vector8_broadcast` (to create comparison vector)
  - `vector8_eq` (SIMD equality comparison, SIMD path)
  - `vector8_is_highbit_set` (to check for matches, SIMD path)
  - `vector8_has_zero` (to detect zero bytes after XOR, fallback path)
- Called from (representative examples):
  - `pg_lfind8` (SIMD-optimized linear search for 8-bit values)
  - `vector8_has_zero` (as part of zero detection logic)

## Notes and Other Information
- This is a static inline function defined in `src/include/port/simd.h` for optimal performance
- Returns a boolean value indicating whether any element matches the target value
- Includes debug assertion checking that compares SIMD results against a reference scalar implementation
- The fallback implementation cleverly uses XOR: `v ^ vector8_broadcast(c)` produces zero bytes where elements match `c`
- Critical component for implementing efficient search algorithms in PostgreSQL's SIMD infrastructure
- Used extensively in linear search operations where finding the existence of a target byte is more important than finding its position
- The function abstracts the complexity of SIMD equality testing and provides a simple boolean interface for higher-level algorithms