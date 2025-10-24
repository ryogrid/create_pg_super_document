# pg_comp_crc32c_sb8

## Location
[src/port/pg_crc32c_sb8.c:35-1169](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/pg_crc32c_sb8.c#L35-L1169)

## Overview
Computes CRC-32C checksums using the slicing-by-8 algorithm, which provides high-performance CRC calculation through optimized lookup tables.

## Definition
```c
pg_crc32c pg_comp_crc32c_sb8(pg_crc32c crc, const void *data, size_t len)
```

## Detailed Description
This function implements the slicing-by-8 algorithm for CRC-32C calculation as described in the IEEE paper by Kounavis and Berry (2008). The algorithm processes data in three phases:

1. **Alignment Phase**: Processes 0-3 initial bytes one at a time to align the data pointer to a 4-byte boundary, ensuring efficient memory access in the main loop.

2. **Bulk Processing Phase**: Processes data in 8-byte chunks using lookup tables. It reads two 32-bit words, extracts individual bytes, and uses 8 separate lookup tables (one for each byte position) to compute the CRC in parallel.

3. **Remainder Phase**: Processes any remaining bytes (less than 8) one at a time using the single-byte CRC8 macro.

The function handles both big-endian and little-endian architectures through conditional compilation, adjusting byte order extraction accordingly. On big-endian systems, the intermediate CRC value is kept in reverse byte order to avoid byte-swapping during calculation.

## Parameters / Member Variables
- `crc`: The current CRC-32C accumulator value to continue calculation from
- `data`: Pointer to the data buffer to process for CRC calculation  
- `len`: Number of bytes in the data buffer to process

## Dependencies
- Functions called/Symbols referenced:
  - `CRC8` (macro for single-byte CRC processing)
  - `pg_crc32c_table` (static 8x256 lookup table array)
  - `pg_crc32c` (return type typedef)
- Called from (representative examples):
  - `FIN_CRC32C` (macro in pg_crc32c.h)
  - `COMP_CRC32C` (macro in pg_crc32c.h)  
  - `[pg_comp_crc32c_choose](pg_comp_crc32c_choose.md)` (runtime selection functions)

## Notes and Other Information
- This implementation is used as the fallback CRC-32C algorithm when hardware-accelerated instructions (SSE 4.2, ARMv8 CRC, LoongArch CRCC) are not available
- The slicing-by-8 algorithm significantly outperforms byte-by-byte calculation by processing multiple bytes in parallel using precomputed lookup tables
- The function maintains 4-byte alignment for optimal memory access performance during bulk processing
- Endianness handling is compile-time optimized through preprocessor conditionals
- Located in `src/port/pg_crc32c_sb8.c:35-99` with lookup table definitions continuing to line 1169

## Simplified Source

```c
pg_crc32c pg_comp_crc32c_sb8(pg_crc32c crc, const void *data, size_t len) {
    const unsigned char *p = data;
    const uint32 *p4;

    // Process initial bytes to achieve 4-byte alignment
    while (len > 0 && ((uintptr_t) p & 3)) {
        crc = CRC8(*p++);
        len--;
    }

    // Process 8 bytes at a time using slicing-by-8 algorithm
    p4 = (const uint32 *) p;
    while (len >= 8) {
        uint32 a = *p4++ ^ crc;  // First 4 bytes XOR with current CRC
        uint32 b = *p4++;        // Next 4 bytes

        // Extract bytes with endianness handling
#ifdef WORDS_BIGENDIAN
        const uint8 c0 = b, c1 = b >> 8, c2 = b >> 16, c3 = b >> 24;
        const uint8 c4 = a, c5 = a >> 8, c6 = a >> 16, c7 = a >> 24;
#else
        const uint8 c0 = b >> 24, c1 = b >> 16, c2 = b >> 8, c3 = b;
        const uint8 c4 = a >> 24, c5 = a >> 16, c6 = a >> 8, c7 = a;
#endif

        // Compute CRC using 8 parallel table lookups
        crc = pg_crc32c_table[0][c0] ^ pg_crc32c_table[1][c1] ^
              pg_crc32c_table[2][c2] ^ pg_crc32c_table[3][c3] ^
              pg_crc32c_table[4][c4] ^ pg_crc32c_table[5][c5] ^
              pg_crc32c_table[6][c6] ^ pg_crc32c_table[7][c7];

        len -= 8;
    }

    // Process remaining bytes one at a time
    p = (const unsigned char *) p4;
    while (len > 0) {
        crc = CRC8(*p++);
        len--;
    }

    return crc;
}
```