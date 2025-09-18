# pg_comp_crc32c_choose

## Location
src/port/pg_crc32c_sse42_choose.c: 54 - 64

## Overview
A runtime function pointer initialization function that selects the optimal CRC-32C computation implementation based on available CPU features, then redirects future calls to the chosen implementation.

## Definition
```c
static pg_crc32c pg_comp_crc32c_choose(pg_crc32c crc, const void *data, size_t len)
```

## Detailed Description
This function implements a "choose-once" pattern for runtime CPU optimization in PostgreSQL's CRC-32C computation. It is called only on the first invocation of CRC-32C calculation and performs the following operations:

1. Detects available CPU capabilities using platform-specific detection functions
2. Updates the global function pointer `pg_comp_crc32c` to point to the optimal implementation:
   - Hardware-accelerated version (SSE 4.2 on x86/x64, ARMv8 CRC on ARM64) if supported
   - Software fallback implementation (slicing-by-8) otherwise
3. Immediately calls the chosen implementation with the provided arguments
4. All subsequent calls bypass this chooser and go directly to the selected implementation

The function exists in multiple architecture-specific variants:
- SSE 4.2 version for x86/x64 platforms
- ARMv8 version for ARM64 platforms

## Parameters / Member Variables
- `crc`: Initial CRC-32C value to continue computation from
- `data`: Pointer to the data buffer to compute CRC over
- `len`: Length of the data buffer in bytes

## Dependencies
- Functions called/Symbols referenced (SSE 4.2 version):
  - `[pg_crc32c_sse42_available](pg_crc32c_sse42_available.md)`
  - `pg_comp_crc32c_sse42`
  - `pg_comp_crc32c_sb8`

- Functions called/Symbols referenced (ARMv8 version):
  - `pg_crc32c_armv8_available`
  - `pg_comp_crc32c_armv8`
  - `pg_comp_crc32c_sb8`

- Called from:
  - Initially assigned to `pg_comp_crc32c` function pointer
  - Self-recursive call after pointer reassignment

## Notes and Other Information
- This is a static function that implements the "resolver" pattern for runtime optimization
- The function modifies the global `pg_comp_crc32c` function pointer, effectively replacing itself
- After the first call, the chooser function is never called again - all subsequent calls go directly to the chosen implementation
- This pattern provides optimal performance with minimal runtime overhead after initialization
- The slicing-by-8 (`pg_comp_crc32c_sb8`) implementation serves as the universal fallback for CPUs without hardware CRC support
- Part of PostgreSQL's performance optimization strategy for frequently-used operations like checksumming