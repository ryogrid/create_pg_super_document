# vector8_highbit_mask

## Location
[src/include/port/simd.h:309-337](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/port/simd.h#L309-L337)

## Overview
A function that returns a bitmask representing which bytes in a Vector8 have their high bit (most significant bit) set.

## Definition
static inline uint32 vector8_highbit_mask(const Vector8 v)

## Detailed Description
This function extracts the high bit from each byte in a Vector8 and returns them as a compact 32-bit bitmask. Each bit in the returned value corresponds to one byte in the input vector, with the bit set if that bytes high bit was set. The function uses platform-specific optimizations:
- SSE2: Uses _mm_movemask_epi8() which directly extracts high bits into a mask
- NEON: Implements a complex sequence using bit manipulation, vector shifting, and bit extraction due to lack of direct equivalent instruction
- No fallback implementation provided (only available on SIMD platforms)

This function is particularly useful for radix tree operations and other data structures that need to quickly identify which bytes have certain characteristics.

## Parameters / Member Variables
- v: The Vector8 to extract high bit mask from

## Dependencies
- Functions called/Symbols referenced:
  - Vector8 (type)
  - _mm_movemask_epi8 (SSE2 intrinsic)
  - vld1q_u8, vandq_u8, vshrq_n_s8, vextq_u8, vaddvq_u16, vzip1q_u8 (NEON intrinsics)
  - USE_SSE2, USE_NEON (preprocessor conditions)
- Called from (representative examples):
  - [RT_NODE_16_SEARCH_EQ](../R/RT_NODE_16_SEARCH_EQ.md)
  - [RT_NODE_16_GET_INSERTPOS](../R/RT_NODE_16_GET_INSERTPOS.md)

## Notes and Other Information
- Implemented as a static inline function for performance optimization
- Returns a uint32 where each bit represents the high bit of the corresponding input byte
- The NEON implementation is notably complex due to architecture differences
- NEON version includes optimization comment about faster alternatives that were avoided for convenience
- Critical for radix tree node operations in PostgreSQL
- Only available on SIMD-capable platforms (no scalar fallback)
- Part of PostgreSQLs SIMD abstraction layer for efficient bit manipulation operations

## Simplified Source

```c
static inline uint32
vector8_highbit_mask(const Vector8 v)
{
    // Extract high bit from each byte in the vector
    // SSE2: Direct extraction using movemask instruction
    #ifdef USE_SSE2
        return (uint32) _mm_movemask_epi8(v);

    // NEON: Complex bit manipulation due to lack of direct equivalent
    #elif defined(USE_NEON)
        // Create bit position mask for each byte
        static const uint8 mask[16] = {
            1<<0, 1<<1, 1<<2, 1<<3, 1<<4, 1<<5, 1<<6, 1<<7,
            1<<0, 1<<1, 1<<2, 1<<3, 1<<4, 1<<5, 1<<6, 1<<7
        };

        // Apply mask to shifted high bits and combine results
        uint8x16_t masked = vandq_u8(vld1q_u8(mask),
                                    (uint8x16_t) vshrq_n_s8((int8x16_t) v, 7));
        uint8x16_t maskedhi = vextq_u8(masked, masked, 8);

        return (uint32) vaddvq_u16((uint16x8_t) vzip1q_u8(masked, maskedhi));
    #endif
}
```