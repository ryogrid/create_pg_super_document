# vector32_load

## Location
[src/include/port/simd.h:121-134](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/port/simd.h#L121-L134)

## Overview
Loads a chunk of memory (4 uint32 values) into a Vector32 SIMD register, providing platform-optimized memory loading operations for SIMD-enabled code paths that work with 32-bit integer data.

## Definition
```c
static inline void vector32_load(Vector32 *v, const uint32 *s)
```

## Detailed Description
This function provides a platform-abstracted interface for loading 4 consecutive 32-bit integers (16 bytes total) into a Vector32 SIMD register. The implementation uses conditional compilation to select the most appropriate SIMD instruction set available:

- **SSE2 (x86/x64)**: Uses `_mm_loadu_si128()` for unaligned 128-bit loads, treating the 32-bit data as a 128-bit vector
- **NEON (ARM)**: Uses `vld1q_u32()` for 128-bit vector loads with 32-bit lane interpretation
- **No fallback**: Unlike vector8_load, this function only provides implementations for SIMD-capable platforms

The function is specifically designed for operations that require vectorized processing of 32-bit integer data, such as hash computation, array comparisons, and numerical operations on integer arrays.

## Parameters / Member Variables
- `v`: Pointer to the Vector32 destination register where the loaded data will be stored
- `s`: Pointer to the source memory location containing 4 consecutive uint32 values to load (16 bytes total)

## Dependencies
- Functions called/Symbols referenced:
  - `_mm_loadu_si128` (SSE2 implementation)
  - `vld1q_u32` (NEON implementation)
  - Vector32 (type definition)
- Called from (representative examples):
  - [pg_lfind32_simd_helper](../p/pg_lfind32_simd_helper.md) (SIMD-optimized linear search for 32-bit values)

## Notes and Other Information
- This is a static inline function defined in `src/include/port/simd.h` for optimal performance
- Unlike vector8_load, this function does not provide a fallback implementation for non-SIMD platforms
- The function assumes the availability of SIMD instructions and is only compiled when USE_SSE2 or USE_NEON is defined
- Used primarily in specialized SIMD search and comparison operations where 32-bit integer processing is required
- The unaligned load capability ensures the function works with arbitrary memory addresses, not just 16-byte aligned ones
- Part of PostgreSQL's performance optimization infrastructure for operations on larger integer data types

## Simplified Source

```c
static inline void
vector32_load(Vector32 *v, const uint32 *s)
{
    // Load 4 consecutive 32-bit integers (16 bytes) into Vector32 register
#ifdef USE_SSE2
    *v = _mm_loadu_si128((const __m128i *) s);
#elif defined(USE_NEON)
    *v = vld1q_u32(s);
#endif
}
```