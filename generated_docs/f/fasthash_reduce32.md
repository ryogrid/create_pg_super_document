# fasthash_reduce32

## Location
src/include/common/hashfn_unstable.h: 337 - 347

## Overview
A utility function that reduces a 64-bit hash value to 32 bits using Fermat residue computation to preserve information from both upper and lower portions of the original hash.

## Definition
```c
static inline uint32
fasthash_reduce32(uint64 h)
```

## Detailed Description
This function provides an intelligent reduction from 64-bit to 32-bit hash values that goes beyond simple truncation. Instead of just discarding the upper 32 bits, it uses a mathematical approach based on Fermat residue computation to retain information from both the high and low portions of the 64-bit hash.

The operation `h - (h >> 32)` effectively:
1. Takes the lower 32 bits of the hash (h)
2. Subtracts the upper 32 bits (h >> 32) from it
3. The arithmetic naturally wraps around 32-bit boundaries due to the uint32 return type

This technique provides better hash distribution compared to simple truncation, as it ensures that changes in the upper bits of the 64-bit hash will still affect the final 32-bit result. This is particularly important for hash table implementations that need good distribution across bucket arrays.

## Parameters / Member Variables
- `h`: The 64-bit hash value to be reduced to 32 bits

## Dependencies
- Functions called/Symbols referenced:
  - None (pure arithmetic operation)
- Called from (representative examples):
  - `[fasthash_final32](fasthash_final32.md)` (in src/include/common/hashfn_unstable.h:350)
  - `[fasthash32](fasthash32.md)` (in src/include/common/hashfn_unstable.h:384)

## Notes and Other Information
- Returns a 32-bit hash value with better distribution than simple truncation
- The Fermat residue approach is a mathematically sound way to preserve entropy from the full 64-bit value
- Used as an optional step in the hash pipeline when 32-bit hash values are needed
- Provides "additional mixing" beyond what would be achieved by just taking the lower 32 bits
- Part of PostgreSQL's strategy to provide both 32-bit and 64-bit hash variants while maintaining good statistical properties