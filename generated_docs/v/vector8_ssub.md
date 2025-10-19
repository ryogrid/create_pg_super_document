# vector8_ssub

## Location
[src/include/port/simd.h:369-384](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/port/simd.h#L369-L384)

## Overview
Performs saturated subtraction on two 8-byte SIMD vectors, preventing underflow by clamping results to zero.

## Definition
```c
static inline Vector8
vector8_ssub(const Vector8 v1, const Vector8 v2)
```

## Detailed Description
The `vector8_ssub` function performs saturated subtraction between two Vector8 inputs. Unlike regular subtraction, saturated subtraction clamps the result to prevent underflow - if the subtraction would result in a negative value, it returns zero instead. The implementation uses platform-specific SIMD instructions:
- On SSE2-capable processors, uses `_mm_subs_epu8()` intrinsic for unsigned 8-bit saturated subtraction
- On ARM NEON processors, uses `vqsubq_u8()` intrinsic for unsigned saturated subtraction
- No fallback implementation for non-SIMD platforms (SSE2/NEON required)

This function is commonly used in comparison operations and range checking where underflow must be avoided.

## Parameters / Member Variables
- `v1`: Minuend Vector8 (value being subtracted from)
- `v2`: Subtrahend Vector8 (value being subtracted)

## Dependencies
- Functions called/Symbols referenced:
  - `_mm_subs_epu8` (SSE2 intrinsic for saturated subtraction)
  - `vqsubq_u8` (NEON intrinsic for saturated subtraction)
  - Vector8 type
- Called from (representative examples):
  - [vector8_has_le](vector8_has_le.md) (less-than-or-equal comparison helper)

## Notes and Other Information
- Defined as static inline for optimal performance
- Requires SIMD support (SSE2 or NEON) - no scalar fallback provided
- Part of the portable SIMD interface in src/include/port/simd.h
- Saturated arithmetic prevents wraparound, making it safer for range comparisons
- Primarily used in building other SIMD comparison functions like vector8_has_le

## Simplified Source

```c
static inline Vector8
vector8_ssub(const Vector8 v1, const Vector8 v2)
{
    // Saturated subtraction: clamps to zero instead of underflowing
    #ifdef USE_SSE2
        return _mm_subs_epu8(v1, v2);
    #elif defined(USE_NEON)
        return vqsubq_u8(v1, v2);
    #endif
    // Note: No fallback implementation - requires SIMD support
}
```