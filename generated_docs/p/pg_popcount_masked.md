# pg_popcount_masked

## Location
src/include/port/pg_bitutils.h: 370 - 397

## Overview
The pg_popcount_masked function counts the number of 1-bits in a buffer after applying a bitmask to each byte, providing optimized performance for masked bit counting operations commonly used in visibility maps and other PostgreSQL subsystems.

## Definition


## Detailed Description
pg_popcount_masked extends the functionality of pg_popcount by applying a bitwise AND mask to each byte before counting the 1-bits. This is particularly useful for operations like visibility map processing where only certain bits in each byte are relevant. Similar to pg_popcount, it uses a threshold-based approach to decide between a simple lookup table method for small buffers and an optimized implementation for larger buffers. The mask is applied to each byte individually before the bit count lookup.

## Parameters / Member Variables
- `buf`: Pointer to the character buffer whose masked bits should be counted
- `bytes`: Number of bytes in the buffer to process
- `mask`: 8-bit mask (bits8 type) to apply to each byte before counting

## Dependencies
- Functions called/Symbols referenced:
  - bits8 (8-bit unsigned integer type for the mask)
  - pg_number_of_ones (lookup table for single byte bit counts)
  - pg_popcount_masked_optimized (optimized implementation for larger buffers)
- Called from (representative examples):
  - visibilitymap_count (counting visible tuples in heap visibility maps)
  - pg_popcount_masked_choose (function pointer selection mechanism)

## Notes and Other Information
- The function is declared as static inline for performance
- Uses the same architecture-dependent threshold as pg_popcount (8 bytes for 64-bit, 4 bytes for 32-bit)
- The mask parameter allows selective bit counting, essential for visibility map operations where only certain bit positions indicate tuple visibility
- Returns uint64 to handle large bit counts without overflow
- Primarily used in heap access methods and visibility tracking subsystems