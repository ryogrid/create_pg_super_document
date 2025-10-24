# pg_comp_crc32c_loongarch

## Location
[src/port/pg_crc32c_loongarch.c:20-73](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/pg_crc32c_loongarch.c#L20-L73)

## Overview
A hardware-accelerated CRC32C computation function specifically optimized for LoongArch architecture processors using dedicated CRCC instructions for maximum performance.

## Definition

```c
pg_crc32c
pg_comp_crc32c_loongarch(pg_crc32c crc, const void *data, size_t len)
```
## Detailed Description
This function implements CRC32C (Castagnoli) checksum computation using LoongArch architecture's native CRCC (Cyclic Redundancy Check Castagnoli) instructions. The implementation is highly optimized for performance through strategic memory alignment and bulk processing strategies.

The function employs a multi-stage processing approach:
1. **Alignment Phase**: Processes unaligned leading bytes (1, 2, or 4 bytes) to achieve 8-byte alignment
2. **Bulk Processing**: Processes data in 8-byte chunks using the most efficient 64-bit CRCC instruction
3. **Remainder Processing**: Handles remaining bytes (0-7) in decreasing size order (4, 2, 1 bytes)

While LoongArch doesn't require memory alignment for correctness, aligned memory access provides significant performance improvements, making the alignment preprocessing worthwhile.

## Parameters / Member Variables
- `crc`: Input CRC32C value to continue computation from (typically initialized with INIT_CRC32C macro)
- `*data`: Pointer to the data buffer to compute CRC32C checksum for
- `len`: Size of the data buffer in bytes
## Dependencies
- Functions called/Symbols referenced:
  -  (return type)
  -  (alignment checking macro)
  -  (LoongArch builtin for 8-bit CRC)
  -  (LoongArch builtin for 16-bit CRC)
  -  (LoongArch builtin for 32-bit CRC)
  -  (LoongArch builtin for 64-bit CRC)
- Called from (representative examples):
  -  macro (when USE_LOONGARCH_CRC32C is defined)
  -  macro (for finalizing CRC computation)

## Notes and Other Information
- This function is only compiled and used when  is defined at build time
- The function leverages LoongArch processor's dedicated CRC32C hardware instructions for optimal performance
- Memory alignment optimization significantly improves performance despite not being required for correctness
- The function is part of PostgreSQL's multi-architecture CRC32C implementation strategy, providing hardware acceleration on LoongArch systems
- Located in 
- The implementation follows PostgreSQL's portable CRC32C interface, making it interchangeable with other architecture-specific implementations

## Simplified Source

```c
pg_crc32c pg_comp_crc32c_loongarch(pg_crc32c crc, const void *data, size_t len) {
    const unsigned char *p = data;
    const unsigned char *pend = p + len;

    // Process leading bytes to achieve 8-byte alignment for performance
    // LoongArch allows unaligned access but aligned is much faster
    if (!PointerIsAligned(p, uint16) && p + 1 <= pend) {
        crc = __builtin_loongarch_crcc_w_b_w(*p, crc);
        p += 1;
    }
    if (!PointerIsAligned(p, uint32) && p + 2 <= pend) {
        crc = __builtin_loongarch_crcc_w_h_w(*(uint16 *) p, crc);
        p += 2;
    }
    if (!PointerIsAligned(p, uint64) && p + 4 <= pend) {
        crc = __builtin_loongarch_crcc_w_w_w(*(uint32 *) p, crc);
        p += 4;
    }

    // Process aligned 8-byte chunks for maximum efficiency
    while (p + 8 <= pend) {
        crc = __builtin_loongarch_crcc_w_d_w(*(uint64 *) p, crc);
        p += 8;
    }

    // Process remaining bytes in decreasing size order
    if (p + 4 <= pend) {
        crc = __builtin_loongarch_crcc_w_w_w(*(uint32 *) p, crc);
        p += 4;
    }
    if (p + 2 <= pend) {
        crc = __builtin_loongarch_crcc_w_h_w(*(uint16 *) p, crc);
        p += 2;
    }
    if (p < pend) {
        crc = __builtin_loongarch_crcc_w_b_w(*p, crc);
    }

    return crc;
}
```