# pg_comp_crc32c_armv8

## Location
src/port/pg_crc32c_armv8.c: 22 - 75

## Overview
ARMv8-specific implementation of CRC-32C checksum computation that utilizes ARMv8 CRC Extension instructions for hardware-accelerated checksum calculation.

## Definition
```c
pg_crc32c pg_comp_crc32c_armv8(pg_crc32c crc, const void *data, size_t len)
```

## Detailed Description
This function provides an optimized implementation of CRC-32C (Castagnoli) checksum computation specifically for ARMv8 processors with CRC Extension support. The implementation uses hardware acceleration through ARMv8 CRC Extension instructions (`__crc32cb`, `__crc32ch`, `__crc32cw`, `__crc32cd`) to achieve high performance.

The function processes data with an alignment-optimized approach:
1. First processes unaligned leading bytes to achieve 8-byte alignment
2. Processes aligned 8-byte chunks in the main loop for maximum efficiency  
3. Handles remaining trailing bytes (0-7 bytes)

While ARMv8 doesn't strictly require aligned memory access, the implementation prioritizes alignment because aligned access is significantly faster on these processors.

## Parameters / Member Variables
- `crc`: Initial CRC-32C value to continue computation from (or INIT_CRC32C for new computation)
- `data`: Pointer to the data buffer to compute checksum over
- `len`: Size of the data buffer in bytes

## Dependencies
- Functions called/Symbols referenced:
  - PointerIsAligned (alignment checking macro)
  - __crc32cb (ARMv8 CRC instruction for 8-bit data)
  - __crc32ch (ARMv8 CRC instruction for 16-bit data) 
  - __crc32cw (ARMv8 CRC instruction for 32-bit data)
  - __crc32cd (ARMv8 CRC instruction for 64-bit data)
- Called from (representative examples):
  - COMP_CRC32C macro (when USE_ARMV8_CRC32C is defined)
  - pg_comp_crc32c_choose (runtime function selection)
  - pg_crc32c_armv8_available (availability checking)

## Notes and Other Information
- This function is only available when PostgreSQL is compiled with ARMv8 CRC Extension support (USE_ARMV8_CRC32C defined)
- Requires ARMv8 processors with CRC Extension capability and the arm_acle.h header
- The function is typically selected at runtime through pg_comp_crc32c_choose based on CPU capabilities
- Alignment optimization makes this implementation significantly faster than generic implementations on supported hardware
- Part of PostgreSQL's hardware-accelerated checksum infrastructure alongside SSE4.2 (x86) and LoongArch implementations