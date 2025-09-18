# pg_popcount_masked_fast

## Location
[src/port/pg_bitutils.c:295-347](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/pg_bitutils.c#L295-L347)

## Overview
An optimized masked population count function that efficiently counts the number of set bits in a buffer after applying a bit mask to each byte, using hardware-accelerated instructions and alignment-aware processing.

## Definition
```c
static uint64 pg_popcount_masked_fast(const char *buf, int bytes, bits8 mask)
```

## Detailed Description
This function provides a fast implementation for counting the number of 1-bits in a buffer after applying a specific bit mask to each byte. It combines the efficiency of hardware popcount instructions with selective bit filtering. The function uses a sophisticated approach: it creates expanded mask patterns that replicate the 8-bit mask across 64-bit or 32-bit words (depending on platform architecture), then processes aligned chunks using bitwise AND operations followed by hardware popcount instructions. For remaining unaligned bytes, it applies the mask directly and uses lookup table processing. This enables efficient selective bit counting for specific bit positions across large buffers.

## Parameters / Member Variables
- `buf`: Pointer to the buffer containing the data to count bits in
- `bytes`: Number of bytes in the buffer to process
- `mask`: 8-bit mask specifying which bit positions to count (bits8 type)

## Dependencies
- Functions called/Symbols referenced:
  - [pg_popcount64_fast](pg_popcount64_fast.md) (for 64-bit masked chunk processing)
  - [pg_popcount32_fast](pg_popcount32_fast.md) (for 32-bit masked chunk processing)
  - TYPEALIGN (for alignment checking)
  - pg_number_of_ones (lookup table for remaining masked bytes)
  - UINT64CONST (for 64-bit constant generation)
- Called from (representative examples):
  - [choose_popcount_functions](../c/choose_popcount_functions.md)

## Notes and Other Information
- Creates platform-specific mask expansion: 64-bit masks on 64-bit platforms, 32-bit masks on 32-bit platforms
- Uses bit manipulation trick (~UINT64CONST(0) / 0xFF * mask) to replicate the 8-bit mask across all byte positions in a word
- Implements alignment-aware processing to maximize hardware instruction efficiency
- Applies mask using bitwise AND before calling hardware popcount functions
- Part of PostgreSQL's dynamic function selection mechanism for selective bit manipulation
- Static function used internally within the popcount optimization framework
- Essential for bitmap operations where only specific bit positions are relevant