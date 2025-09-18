# vector8_eq

## Location
src/include/port/simd.h: 385 - 396

## Overview
Performs element-wise equality comparison between two 8-byte SIMD vectors, returning a mask vector indicating which elements are equal.

## Definition
```c
static inline Vector8
vector8_eq(const Vector8 v1, const Vector8 v2)
```

## Detailed Description
The `vector8_eq` function performs element-wise equality comparison between two Vector8 inputs, returning a Vector8 mask where each byte contains 0xFF if the corresponding elements are equal, or 0x00 if they differ. The implementation uses platform-specific SIMD instructions:
- On SSE2-capable processors, uses `_mm_cmpeq_epi8()` intrinsic for 8-bit integer comparison
- On ARM NEON processors, uses `vceqq_u8()` intrinsic for unsigned 8-bit comparison
- No fallback implementation for non-SIMD platforms (SSE2/NEON required)

This function is fundamental for vectorized search operations, pattern matching, and conditional processing in SIMD code.

## Parameters / Member Variables
- `v1`: First Vector8 operand for comparison
- `v2`: Second Vector8 operand for comparison

## Dependencies
- Functions called/Symbols referenced:
  - `_mm_cmpeq_epi8` (SSE2 intrinsic for equality comparison)
  - `vceqq_u8` (NEON intrinsic for equality comparison)
  - Vector8 type
- Called from (representative examples):
  - `[RT_NODE_16_SEARCH_EQ](../R/RT_NODE_16_SEARCH_EQ.md)` (radix tree node search)
  - `[RT_NODE_16_GET_INSERTPOS](../R/RT_NODE_16_GET_INSERTPOS.md)` (radix tree insertion position finding)
  - `[vector8_has](vector8_has.md)` (vector element search helper)
  - `[is_valid_ascii](../i/is_valid_ascii.md)` (ASCII validation utility)

## Notes and Other Information
- Defined as static inline for optimal performance
- Requires SIMD support (SSE2 or NEON) - no scalar fallback provided
- Part of the portable SIMD interface in src/include/port/simd.h
- Returns a mask vector with 0xFF for equal elements, 0x00 for unequal elements
- Widely used in radix tree operations and character validation functions
- Essential building block for other comparison and search operations