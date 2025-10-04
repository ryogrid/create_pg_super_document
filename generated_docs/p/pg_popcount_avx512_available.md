# pg_popcount_avx512_available

## Location
[src/port/pg_popcount_avx512_choose.c:95-102](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/pg_popcount_avx512_choose.c#L95-L102)

## Overview
A public function that determines if the CPU and operating system support all requirements for using AVX-512 optimized population count implementations by combining multiple feature detection checks.

## Definition
```c
bool pg_popcount_avx512_available(void)
```

## Detailed Description
This function serves as the comprehensive gate-keeper for enabling AVX-512 optimized population count operations in PostgreSQL. It performs a complete validation chain by calling three prerequisite detection functions and returns true only when all conditions are met:

1. **XSAVE support**: The OS must support saving/restoring extended processor state
2. **ZMM register availability**: The OS must have enabled 512-bit ZMM registers for AVX-512
3. **Specific AVX-512 instructions**: The CPU must support both AVX512-VPOPCNTDQ and AVX512-BW instruction sets

This layered approach ensures that not only does the CPU have the necessary instruction support, but that the operating system is properly configured to handle the extended register state that AVX-512 requires. This prevents crashes or corruption that could occur if AVX-512 instructions were used without proper OS support.

## Parameters / Member Variables



## Dependencies
- Functions called/Symbols referenced:
  - [xsave_available](../x/xsave_available.md) at Line 97
  - [zmm_regs_available](../z/zmm_regs_available.md) at Line 98  
  - [avx512_popcnt_available](../a/avx512_popcnt_available.md) at Line 99
- Called from (representative examples):
  - `TRY_POPCNT_FAST` at src/include/port/pg_bitutils.h:315
  - [choose_popcount_functions](../c/choose_popcount_functions.md) at src/port/pg_bitutils.c:174

## Notes and Other Information
- This is a public function (not static), making it available to other compilation units
- All three prerequisite functions must return true for this function to return true
- Used by PostgreSQL's runtime function selection mechanism to choose the most efficient popcount implementation
- Part of PostgreSQL's adaptive optimization system that selects the best available SIMD implementation at runtime
- The function enables significant performance improvements for bit manipulation operations when AVX-512 is fully supported
- Failure of any single prerequisite check will cause the entire function to return false, falling back to less optimized implementations

## Simplified Source

```c
bool pg_popcount_avx512_available(void)
{
    // Check all three requirements for AVX-512 popcount support:
    // 1. OS supports XSAVE for extended state management
    // 2. OS has enabled 512-bit ZMM registers
    // 3. CPU supports required AVX-512 instructions
    return xsave_available() &&
           zmm_regs_available() &&
           avx512_popcnt_available();
}
```