# pg_popcount

## Location
src/include/port/pg_bitutils.h: 339 - 369

## Overview
The pg_popcount function counts the number of 1-bits (population count) in a buffer of bytes, providing an optimized implementation that switches between simple lookup and hardware-accelerated approaches based on buffer size.

## Definition


## Detailed Description
pg_popcount implements an efficient bit counting algorithm that adapts its strategy based on the size of the input buffer. For small buffers (less than 4 or 8 bytes depending on pointer size), it uses a simple loop with lookup table access via pg_number_of_ones. For larger buffers, it delegates to pg_popcount_optimized which can leverage specialized CPU instructions or other optimizations to achieve better performance. The threshold is set to the point where the function call overhead is justified by the performance gains of the optimized version.

## Parameters / Member Variables
- `buf`: Pointer to the character buffer whose bits should be counted
- `bytes`: Number of bytes in the buffer to process

## Dependencies
- Functions called/Symbols referenced:
  - pg_number_of_ones (lookup table for single byte bit counts)
  - [pg_popcount_optimized](pg_popcount_optimized.md) (optimized implementation for larger buffers)
- Called from (representative examples):
  - [brin_bloom_union](../b/brin_bloom_union.md) (BRIN bloom filter operations)
  - [bloom_prop_bits_set](../b/bloom_prop_bits_set.md) (bloom filter bit counting)
  - [sizebitvec](../s/sizebitvec.md) (text search bit vector sizing)
  - [bit_bit_count](../b/bit_bit_count.md) (bit string bit counting)
  - [bytea_bit_count](../b/bytea_bit_count.md) (bytea bit counting)

## Notes and Other Information
- The function is declared as static inline for performance
- Threshold determination is architecture-dependent (8 bytes for 64-bit systems, 4 bytes for 32-bit systems)
- The threshold aligns with where special CPU instructions become beneficial in the optimized version
- Return type is uint64 to handle large bit counts without overflow
- Used extensively in bloom filters, bit vectors, and various bit manipulation operations throughout PostgreSQL