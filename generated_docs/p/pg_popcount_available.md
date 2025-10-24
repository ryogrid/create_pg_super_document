# pg_popcount_available

## Location
[src/port/pg_bitutils.c:134-155](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/pg_bitutils.c#L134-L155)

## Overview
Detects whether the CPU supports the POPCNT instruction by examining CPUID flags.

## Definition

```c
struction not available
#endif

	return (exx[2] & (1 << 23)) != 0;
```
## Detailed Description
This function uses CPU identification (CPUID) instruction to determine if the processor supports the POPCNT (population count) instruction. It checks bit 23 of the ECX register returned by CPUID function 1, which indicates POPCNT availability. The function is part of PostgreSQL's runtime CPU feature detection system that allows the database to use optimized assembly implementations when hardware support is available.

The function uses platform-specific CPUID access methods - either  (GCC) or  (MSVC) - and will fail to compile if neither is available.

## Parameters / Member Variables
- Returns:  - true if POPCNT instruction is supported, false otherwise

## Dependencies  
- Functions called/Symbols referenced:
  -  (when HAVE__GET_CPUID is defined)
  -  (when HAVE__CPUID is defined)
- Called from:
  -  at src/port/pg_bitutils.c:158

## Notes and Other Information
- This is a static function, only accessible within pg_bitutils.c
- Requires either HAVE__GET_CPUID or HAVE__CPUID to be defined at compile time
- The POPCNT instruction significantly accelerates bit counting operations
- Part of PostgreSQL's adaptive optimization strategy for bit manipulation functions
- CPUID function 1 provides processor info and feature bits, with POPCNT support indicated by ECX bit 23

## Simplified Source

```c
static bool pg_popcount_available(void) {
    unsigned int exx[4] = {0, 0, 0, 0};

    // Get CPU feature information using platform-specific CPUID
#if defined(HAVE__GET_CPUID)
    __get_cpuid(1, &exx[0], &exx[1], &exx[2], &exx[3]);
#elif defined(HAVE__CPUID)
    __cpuid(exx, 1);
#else
#error cpuid instruction not available
#endif

    // Check POPCNT bit (bit 23) in ECX register
    return (exx[2] & (1 << 23)) != 0;
}
```