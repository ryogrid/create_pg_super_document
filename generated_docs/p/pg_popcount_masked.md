# pg_popcount_masked

## Location
[src/include/port/pg_bitutils.h:370-397](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/port/pg_bitutils.h#L370-L397)

## Overview
The pg_popcount_masked function counts the number of 1-bits in a buffer after applying a bitmask to each byte, providing optimized performance for masked bit counting operations commonly used in visibility maps and other PostgreSQL subsystems.

## Definition

```c
structions in the optimized version.
	 */
#if SIZEOF_VOID_P >= 8
	int			threshold = 8;
```
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
  - [pg_popcount_masked_optimized](pg_popcount_masked_optimized.md) (optimized implementation for larger buffers)
- Called from (representative examples):
  - [visibilitymap_count](../v/visibilitymap_count.md) (counting visible tuples in heap visibility maps)
  - [pg_popcount_masked_choose](pg_popcount_masked_choose.md) (function pointer selection mechanism)

## Notes and Other Information
- The function is declared as static inline for performance
- Uses the same architecture-dependent threshold as pg_popcount (8 bytes for 64-bit, 4 bytes for 32-bit)
- The mask parameter allows selective bit counting, essential for visibility map operations where only certain bit positions indicate tuple visibility
- Returns uint64 to handle large bit counts without overflow
- Primarily used in heap access methods and visibility tracking subsystems

## Simplified Source

```c
static inline uint64 pg_popcount_masked(const char *buf, int bytes, bits8 mask) {
    // Set threshold based on architecture
#if SIZEOF_VOID_P >= 8
    int threshold = 8;    // 64-bit systems
#else
    int threshold = 4;    // 32-bit systems
#endif

    // Use simple lookup table for small buffers
    if (bytes < threshold) {
        uint64 popcnt = 0;

        while (bytes--) {
            // Apply mask and count bits using lookup table
            popcnt += pg_number_of_ones[(unsigned char) *buf++ & mask];
        }
        return popcnt;
    }

    // Use optimized implementation for larger buffers
    return pg_popcount_masked_optimized(buf, bytes, mask);
}
```