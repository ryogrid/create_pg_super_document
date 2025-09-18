# vector32_eq

## Location
src/include/port/simd.h: 397 - 411

## Overview
Performs element-wise equality comparison between two 32-byte SIMD vectors, returning a mask vector indicating which 32-bit elements are equal.

## Definition
```c
static inline Vector32
vector32_eq(const Vector32 v1, const Vector32 v2)
```

## Detailed Description
The `vector32_eq` function performs element-wise equality comparison between two Vector32 inputs, returning a Vector32 mask where each 32-bit element contains 0xFFFFFFFF if the corresponding elements are equal, or 0x00000000 if they differ. The implementation uses platform-specific SIMD instructions:
- On SSE2-capable processors, uses `_mm_cmpeq_epi32()` intrinsic for 32-bit integer comparison
- On ARM NEON processors, uses `vceqq_u32()` intrinsic for unsigned 32-bit comparison
- No fallback implementation for non-SIMD platforms (SSE2/NEON required)

This function is optimized for comparing 32-bit values such as integers, array indices, or other word-sized data in parallel.

## Parameters / Member Variables
- `v1`: First Vector32 operand for comparison
- `v2`: Second Vector32 operand for comparison

## Dependencies
- Functions called/Symbols referenced:
  - `_mm_cmpeq_epi32` (SSE2 intrinsic for 32-bit equality comparison)
  - `vceqq_u32` (NEON intrinsic for 32-bit equality comparison)
  - Vector32 type
- Called from (representative examples):
  - `[pg_lfind32_simd_helper](../p/pg_lfind32_simd_helper.md)` (optimized linear search for 32-bit values)

## Notes and Other Information
- Defined as static inline for optimal performance
- Requires SIMD support (SSE2 or NEON) - no scalar fallback provided
- Part of the portable SIMD interface in src/include/port/simd.h
- Returns a mask vector with 0xFFFFFFFF for equal elements, 0x00000000 for unequal elements
- Specialized for 32-bit element comparisons, more efficient than byte-wise operations for larger data types
- Primarily used in optimized search functions for 32-bit integer arrays