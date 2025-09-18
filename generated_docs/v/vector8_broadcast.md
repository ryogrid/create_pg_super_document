# vector8_broadcast

## Location
src/include/port/simd.h: 135 - 147

## Overview
Creates a Vector8 SIMD register with all elements (8 bytes) set to the same uint8 value, providing platform-optimized broadcast operations for SIMD-enabled code paths.

## Definition
```c
static inline Vector8 vector8_broadcast(const uint8 c)
```

## Detailed Description
This function provides a platform-abstracted interface for creating a Vector8 SIMD register where all 8 byte elements are set to the same input value. This operation is commonly known as "broadcasting" or "splatting" a scalar value across all vector lanes. The implementation uses conditional compilation to select the most appropriate SIMD instruction:

- **SSE2 (x86/x64)**: Uses `_mm_set1_epi8(c)` to replicate the byte value across all 16 bytes of the 128-bit register
- **NEON (ARM)**: Uses `vdupq_n_u8(c)` to duplicate the byte value across all 16 bytes of the 128-bit register  
- **Fallback**: Uses `~UINT64CONST(0) / 0xFF * c` to create a 64-bit value with the byte replicated 8 times

This function is essential for vectorized comparison and search operations where a single target value needs to be compared against multiple data elements simultaneously.

## Parameters / Member Variables
- `c`: The uint8 value to broadcast across all elements of the Vector8 register

## Dependencies
- Functions called/Symbols referenced:
  - `_mm_set1_epi8` (SSE2 implementation)
  - `vdupq_n_u8` (NEON implementation)
  - UINT64CONST (fallback implementation)
- Called from (representative examples):
  - `[RT_NODE_16_SEARCH_EQ](../R/RT_NODE_16_SEARCH_EQ.md)` (radix tree equality search operations)
  - `[RT_NODE_16_GET_INSERTPOS](../R/RT_NODE_16_GET_INSERTPOS.md)` (radix tree insertion position finding)
  - `[vector8_has](vector8_has.md)` (SIMD-based element existence checking)
  - `[vector8_has_le](vector8_has_le.md)` (SIMD-based less-than-or-equal comparison)
  - `[vector8_is_highbit_set](vector8_is_highbit_set.md)` (high bit detection operations)
  - `[is_valid_ascii](../i/is_valid_ascii.md)` (ASCII validation routines)

## Notes and Other Information
- This is a static inline function defined in `src/include/port/simd.h` for optimal performance
- Returns a Vector8 value (unlike load functions which take pointer parameters)
- The fallback implementation uses an arithmetic trick: `~UINT64CONST(0) / 0xFF * c` creates a 64-bit value where each byte equals `c`
- Heavily used in comparison operations where a single target byte needs to be compared against multiple data bytes simultaneously
- Critical component of PostgreSQL's SIMD-accelerated search and validation algorithms
- The function works with both 128-bit SIMD registers (SSE2/NEON) and falls back gracefully to scalar operations