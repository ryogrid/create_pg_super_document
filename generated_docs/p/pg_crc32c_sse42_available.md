# pg_crc32c_sse42_available

## Location
[src/port/pg_crc32c_sse42_choose.c:34-53](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/pg_crc32c_sse42_choose.c#L34-L53)

## Overview
Detects whether the current CPU supports Intel SSE 4.2 instructions, specifically for hardware-accelerated CRC-32C computation.

## Definition
```c
static bool pg_crc32c_sse42_available(void)
```

## Detailed Description
This function performs CPU feature detection to determine if the processor supports Intel SSE 4.2 instruction set extensions. It uses the CPUID instruction to query CPU capabilities and specifically checks for the SSE 4.2 feature bit. The function is used to enable runtime selection between hardware-accelerated CRC-32C computation (when SSE 4.2 is available) and software-based fallback implementation.

The function uses conditional compilation to support different CPUID access methods:
- Uses `__get_cpuid` on systems with GCC-style cpuid support
- Uses `__cpuid` on systems with Microsoft-style intrinsics
- Generates a compile error if no CPUID method is available

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - `__get_cpuid` (GCC intrinsic, conditional)
  - `__cpuid` (Microsoft intrinsic, conditional)

- Called from (representative examples):
  - [pg_comp_crc32c_choose](pg_comp_crc32c_choose.md) (in src/port/pg_crc32c_sse42_choose.c:56)

## Notes and Other Information
- This is a static function, only accessible within the same compilation unit
- The function checks bit 20 of the ECX register (exx[2]) which corresponds to the SSE 4.2 feature flag
- The SSE 4.2 instruction set includes the CRC32 instruction that can significantly accelerate CRC-32C computations
- Part of PostgreSQL's runtime CPU feature detection system for optimal performance
- The function is called only once during the first CRC computation to determine the best implementation to use