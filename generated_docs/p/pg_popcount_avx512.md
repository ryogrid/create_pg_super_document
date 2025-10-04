# pg_popcount_avx512

## Location
[src/port/pg_popcount_avx512.c:31-85](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/pg_popcount_avx512.c#L31-L85)

## Overview
pg_popcount_avx512 is an AVX-512 optimized function that efficiently counts the number of 1-bits (population count) in a buffer of bytes using Intel's AVX-512 SIMD instructions.

## Definition
uint64 pg_popcount_avx512(const char *buf, int bytes)

## Detailed Description
This function provides a high-performance implementation of population count (popcount) operation using AVX-512 instructions. It processes data in 64-byte chunks (512 bits) to maximize throughput. The function handles arbitrary buffer alignments and sizes by:

1. Aligning the buffer pointer down to 64-byte boundaries to optimize memory access patterns
2. Using masked loads for the first and last iterations to handle partial chunks
3. Processing full 64-byte chunks in a tight loop using unmasked loads for maximum performance
4. Accumulating popcount results across all processed chunks

The implementation leverages Intel's _mm512_popcnt_epi64 intrinsic for hardware-accelerated bit counting within each 64-bit lane, then sums all lanes using _mm512_reduce_add_epi64.

## Parameters / Member Variables
- : Pointer to the input buffer containing bytes to count bits in
- : Number of bytes to process from the buffer

## Dependencies
- Functions called/Symbols referenced:
  - TYPEALIGN_DOWN (macro for pointer alignment)
  - _mm512_setzero_si512 (AVX-512 intrinsic)
  - _mm512_maskz_loadu_epi8 (AVX-512 masked load intrinsic)
  - _mm512_load_si512 (AVX-512 load intrinsic)
  - _mm512_popcnt_epi64 (AVX-512 popcount intrinsic)
  - _mm512_add_epi64 (AVX-512 addition intrinsic)
  - _mm512_reduce_add_epi64 (AVX-512 reduction intrinsic)
- Called from (representative examples):
  - TRY_POPCNT_FAST (macro in pg_bitutils.h:316)
  - [choose_popcount_functions](../c/choose_popcount_functions.md) (in pg_bitutils.c:176)

## Notes and Other Information
- This function is only compiled when TRY_POPCNT_FAST is defined and AVX-512 support is available
- Requires Intel processors with AVX-512 instruction set support
- Optimized for large buffer sizes where the SIMD overhead is amortized
- Part of PostgreSQL's runtime CPU feature detection and function pointer selection system
- The function uses careful pointer arithmetic and masking to handle unaligned buffers and arbitrary byte counts safely
- Performance is typically much higher than scalar implementations, especially for larger data sizes

## Simplified Source

```c
uint64 pg_popcount_avx512(const char *buf, int bytes)
{
    __m512i accum = _mm512_setzero_si512();
    const char *final;
    int tail_idx;
    __mmask64 mask = ~UINT64CONST(0);

    // Align buffer and calculate masks for unaligned access
    mask <<= ((uintptr_t) buf) % sizeof(__m512i);
    tail_idx = (((uintptr_t) buf + bytes - 1) % sizeof(__m512i)) + 1;
    final = (const char *) TYPEALIGN_DOWN(sizeof(__m512i), buf + bytes - 1);
    buf = (const char *) TYPEALIGN_DOWN(sizeof(__m512i), buf);

    // Process all chunks except the final one
    if (buf < final) {
        // First iteration with mask for alignment
        __m512i val = _mm512_maskz_loadu_epi8(mask, (const __m512i *) buf);
        __m512i cnt = _mm512_popcnt_epi64(val);
        accum = _mm512_add_epi64(accum, cnt);

        buf += sizeof(__m512i);
        mask = ~UINT64CONST(0);

        // Main loop - process full 64-byte chunks
        for (; buf < final; buf += sizeof(__m512i)) {
            val = _mm512_load_si512((const __m512i *) buf);
            cnt = _mm512_popcnt_epi64(val);
            accum = _mm512_add_epi64(accum, cnt);
        }
    }

    // Final iteration with mask for remaining bytes
    mask &= (~UINT64CONST(0) >> (sizeof(__m512i) - tail_idx));
    __m512i val = _mm512_maskz_loadu_epi8(mask, (const __m512i *) buf);
    __m512i cnt = _mm512_popcnt_epi64(val);
    accum = _mm512_add_epi64(accum, cnt);

    // Sum all lanes and return total
    return _mm512_reduce_add_epi64(accum);
}
```