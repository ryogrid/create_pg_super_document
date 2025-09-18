# vector32_broadcast

## Location
src/include/port/simd.h: 148 - 161

## Overview
Creates a Vector32 SIMD register with all elements (4 uint32 values) set to the same input value, providing platform-optimized broadcast operations for SIMD-enabled code paths that work with 32-bit integer data.

## Definition
```c
static inline Vector32 vector32_broadcast(const uint32 c)
```

## Detailed Description
This function provides a platform-abstracted interface for creating a Vector32 SIMD register where all 4 uint32 elements are set to the same input value. This broadcasting operation replicates a single 32-bit integer across all vector lanes, enabling efficient vectorized operations on 32-bit data. The implementation uses conditional compilation to select the appropriate SIMD instruction:

- **SSE2 (x86/x64)**: Uses `_mm_set1_epi32(c)` to replicate the 32-bit value across all 4 lanes of the 128-bit register
- **NEON (ARM)**: Uses `vdupq_n_u32(c)` to duplicate the 32-bit value across all 4 lanes of the 128-bit register
- **No fallback**: Unlike vector8_broadcast, this function only provides implementations for SIMD-capable platforms

This function is essential for vectorized comparison and arithmetic operations where a single 32-bit target value needs to be processed against multiple 32-bit data elements simultaneously.

## Parameters / Member Variables
- `c`: The uint32 value to broadcast across all 4 elements of the Vector32 register

## Dependencies
- Functions called/Symbols referenced:
  - `_mm_set1_epi32` (SSE2 implementation)
  - `vdupq_n_u32` (NEON implementation)
- Called from (representative examples):
  - [pg_lfind32](../p/pg_lfind32.md) (SIMD-optimized linear search for 32-bit values)

## Notes and Other Information
- This is a static inline function defined in `src/include/port/simd.h` for optimal performance
- Returns a Vector32 value (unlike load functions which take pointer parameters)
- Unlike vector8_broadcast, this function does not provide a fallback implementation for non-SIMD platforms
- The function assumes the availability of SIMD instructions and is only compiled when USE_SSE2 or USE_NEON is defined
- Used primarily in specialized SIMD search and comparison operations where 32-bit integer processing is required
- Part of PostgreSQL's performance optimization infrastructure for operations on larger integer data types
- Enables efficient vectorized operations such as equality testing, hash comparisons, and numerical computations on arrays of 32-bit integers