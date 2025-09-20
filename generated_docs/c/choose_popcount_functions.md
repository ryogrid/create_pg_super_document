# choose_popcount_functions

## Location
[src/port/pg_bitutils.c:156-182](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/pg_bitutils.c#L156-L182)

## Overview
Selects and configures the optimal popcount function implementations based on CPU capabilities detected at runtime.

## Definition
```c
static inline void choose_popcount_functions(void)
```

## Detailed Description
This function implements PostgreSQL's runtime CPU feature detection and function pointer redirection system for popcount operations. It first calls pg_popcount_available() to determine if the CPU supports the POPCNT instruction. Based on this result, it reassigns global function pointers to either fast (assembly-optimized) or slow (portable C) implementations.

Additionally, on systems with USE_AVX512_POPCNT_WITH_RUNTIME_CHECK enabled, it also checks for AVX-512 VPOPCNT support and will further optimize by using AVX-512 implementations when available.

The function uses a lazy initialization pattern - it's called only on the first invocation of popcount functions, after which subsequent calls bypass the detection overhead.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [pg_popcount_available](../p/pg_popcount_available.md)
  - [pg_popcount32_fast](../p/pg_popcount32_fast.md), pg_popcount32_slow
  - [pg_popcount64_fast](../p/pg_popcount64_fast.md), pg_popcount64_slow
  - [pg_popcount_fast](../p/pg_popcount_fast.md), pg_popcount_slow
  - [pg_popcount_masked_fast](../p/pg_popcount_masked_fast.md), pg_popcount_masked_slow
  - [pg_popcount_avx512_available](../p/pg_popcount_avx512_available.md) (conditionally)
  - [pg_popcount_avx512](../p/pg_popcount_avx512.md), pg_popcount_masked_avx512 (conditionally)
- Called from:
  - [pg_popcount32_choose](../p/pg_popcount32_choose.md) at src/port/pg_bitutils.c:185
  - [pg_popcount64_choose](../p/pg_popcount64_choose.md) at src/port/pg_bitutils.c:192
  - [pg_popcount_choose](../p/pg_popcount_choose.md) at src/port/pg_bitutils.c:199
  - [pg_popcount_masked_choose](../p/pg_popcount_masked_choose.md) at src/port/pg_bitutils.c:206

## Notes and Other Information
- This is a static inline function for optimal performance during the one-time initialization
- Implements the chooser pattern for runtime CPU feature detection
- Function pointers are globally reassigned, affecting all subsequent popcount calls
- AVX-512 support is conditionally compiled and represents the highest optimization tier
- Part of PostgreSQL's adaptive optimization strategy to maximize performance across different hardware configurations