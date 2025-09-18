# vector32_or

## Location
[src/include/port/simd.h:351-368](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/port/simd.h#L351-L368)

## Overview
Performs bitwise OR operation on two 32-byte SIMD vectors using platform-optimized instructions.

## Definition
```c
static inline Vector32
vector32_or(const Vector32 v1, const Vector32 v2)
```

## Detailed Description
The `vector32_or` function performs a bitwise OR operation on two Vector32 inputs, returning a Vector32 result. The implementation uses platform-specific SIMD instructions:
- On SSE2-capable processors, uses `_mm_or_si128()` intrinsic
- On ARM NEON processors, uses `vorrq_u32()` intrinsic  
- No fallback implementation for non-SIMD platforms (SSE2/NEON required)

This function is part of PostgreSQL's portable SIMD abstraction layer for high-performance vector operations on 32-bit elements.

## Parameters / Member Variables
- `v1`: First Vector32 operand for the bitwise OR operation
- `v2`: Second Vector32 operand for the bitwise OR operation

## Dependencies
- Functions called/Symbols referenced:
  - `_mm_or_si128` (SSE2 intrinsic)
  - `vorrq_u32` (NEON intrinsic)
  - Vector32 type
- Called from (representative examples):
  - [pg_lfind32_simd_helper](../p/pg_lfind32_simd_helper.md) (optimized linear search helper)

## Notes and Other Information
- Defined as static inline for optimal performance
- Requires SIMD support (SSE2 or NEON) - no scalar fallback provided
- Part of the portable SIMD interface in src/include/port/simd.h
- Used primarily for 32-bit element operations in search and comparison functions
- More specialized than vector8_or as it only supports SIMD-capable platforms