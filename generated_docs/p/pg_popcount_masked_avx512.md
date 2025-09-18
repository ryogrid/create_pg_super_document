# pg_popcount_masked_avx512

## Location
[src/port/pg_popcount_avx512.c:86-141](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/pg_popcount_avx512.c#L86-L141)

## Overview
pg_popcount_masked_avx512 is an AVX-512 optimized function that counts the number of 1-bits in a buffer after applying a bit mask to each byte, using Intel's AVX-512 SIMD instructions for high performance.

## Definition
uint64 pg_popcount_masked_avx512(const char *buf, int bytes, bits8 mask)

## Detailed Description
This function extends the basic popcount operation by applying a bitwise AND mask to each byte before counting the 1-bits. It uses the same AVX-512 optimization strategy as pg_popcount_avx512 but adds an additional masking step. The function:

1. Creates a 512-bit vector filled with the input mask using _mm512_set1_epi8
2. Aligns the buffer pointer down to 64-byte boundaries for optimal memory access
3. Uses masked loads for partial chunks at the beginning and end
4. For each 64-byte chunk, applies the mask using _mm512_and_si512 before counting bits
5. Accumulates popcount results across all processed masked chunks

This is particularly useful for operations that need to count specific bit patterns or ignore certain bits in the input data, such as bitmap operations with validity masks or selective bit counting in encoded data structures.

## Parameters / Member Variables
- : Pointer to the input buffer containing bytes to process
- : Number of bytes to process from the buffer  
- : 8-bit mask value applied to each byte via bitwise AND before counting bits

## Dependencies
- Functions called/Symbols referenced:
  - TYPEALIGN_DOWN (macro for pointer alignment)
  - _mm512_setzero_si512 (AVX-512 intrinsic)
  - _mm512_set1_epi8 (AVX-512 broadcast intrinsic)
  - _mm512_maskz_loadu_epi8 (AVX-512 masked load intrinsic)
  - _mm512_load_si512 (AVX-512 load intrinsic)
  - _mm512_and_si512 (AVX-512 bitwise AND intrinsic)
  - _mm512_popcnt_epi64 (AVX-512 popcount intrinsic)
  - _mm512_add_epi64 (AVX-512 addition intrinsic)
  - _mm512_reduce_add_epi64 (AVX-512 reduction intrinsic)
- Called from (representative examples):
  - TRY_POPCNT_FAST (macro in pg_bitutils.h:317)
  - [choose_popcount_functions](../c/choose_popcount_functions.md) (in pg_bitutils.c:177)

## Notes and Other Information
- This function is only compiled when TRY_POPCNT_FAST is defined and AVX-512 support is available
- Requires Intel processors with AVX-512 instruction set support including VPOPCNTDQ extension
- The mask parameter allows selective bit counting, enabling efficient implementation of bitmap operations
- Part of PostgreSQL's runtime CPU feature detection and function pointer selection system
- Performance scales well with buffer size due to SIMD processing of 64 bytes per iteration
- Commonly used in database bitmap index operations and visibility map processing where only certain bits are relevant