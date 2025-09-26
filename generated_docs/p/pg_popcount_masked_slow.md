# pg_popcount_masked_slow

## Location
[src/port/pg_bitutils.c:444-498](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/pg_bitutils.c#L444-L498)

## Overview
A fallback implementation for counting the number of 1 bits in a buffer after applying a mask to each byte, processing data in optimal word-sized chunks when possible.

## Definition

```c
static uint64
pg_popcount_masked_slow(const char *buf, int bytes, bits8 mask)
```
## Detailed Description
This function provides a portable masked buffer-based population count implementation that operates on arbitrary-length byte arrays with a bitwise mask applied to each byte. It serves as a fallback when hardware-optimized vectorized popcount instructions are unavailable. The function employs several optimization strategies similar to , but with an additional masking step:

1. **Mask expansion**: The 8-bit mask is expanded to fill entire words:
   - On 64-bit platforms: Creates a 64-bit mask by replicating the byte mask across all 8 bytes using 
   - On 32-bit platforms: Creates a 32-bit mask by replicating the byte mask across all 4 bytes using 

2. **Word-aligned masked processing**: When the buffer is properly aligned, processes data in word-sized chunks with masking:
   - On 64-bit platforms: Processes in 8-byte chunks using 
   - On 32-bit platforms: Processes in 4-byte chunks using 

3. **Byte-by-byte masked fallback**: Any remaining bytes are processed individually with the mask applied using 

The mask expansion technique allows efficient word-level masking by creating a pattern where each byte position contains the original mask value.

## Parameters / Member Variables
- : Pointer to the buffer containing the data to be processed
- : The number of bytes in the buffer to process
- : An 8-bit mask to be applied to each byte before counting bits

## Dependencies
- Functions called/Symbols referenced:
  -  (for 64-bit chunk processing)
  -  (for 32-bit chunk processing)  
  -  (lookup table for individual byte processing)
  -  (macro for alignment checking)
  -  (macro for 64-bit constants)
- Called from (representative examples):
  - 
  - 

## Notes and Other Information
- The mask expansion technique () cleverly creates a word-sized mask by exploiting integer arithmetic
- Automatically adapts processing strategy based on platform word size and buffer alignment
- Uses efficient word-based processing with masking when possible, falling back to byte-wise processing when necessary
- Returns  to accommodate large buffer popcount results
- Part of PostgreSQL's specialized bit manipulation utilities for masked operations
- The masking operation is applied before the popcount, allowing selective bit counting based on the mask pattern