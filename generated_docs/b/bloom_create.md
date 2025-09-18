# bloom_create

## Location
src/backend/lib/bloomfilter.c: 87 - 125

## Overview
Creates a Bloom filter data structure with configurable memory usage and false positive rate, optimized for probabilistic set membership testing in PostgreSQL.

## Definition
```c
bloom_filter *bloom_create(int64 total_elems, int bloom_work_mem, uint64 seed)
```

## Detailed Description
The `bloom_create` function initializes a new Bloom filter in the caller's memory context, targeting a false positive rate between 1% and 2%. The implementation uses a power-of-two bitset size and employs multiple hash functions for optimal space-time tradeoffs.

The function calculates the optimal bitset size based on the estimated total elements and available memory, ensuring efficient memory usage while maintaining the target false positive rate. The bitset is allocated as a single contiguous memory block with the bloom_filter structure header.

The algorithm aims for approximately two bytes per element to achieve the target false positive rate, with automatic sizing adjustments to stay within memory constraints while maintaining performance characteristics.

## Parameters / Member Variables
- `total_elems`: Estimated total number of elements to be added to the filter (approximate, can be off by factor of 5+ without significant impact)
- `bloom_work_mem`: Memory limit in KB for the underlying bitset allocation
- `seed`: Seed value for hash function initialization to prevent consistent false positives across multiple filter instances

## Dependencies
- Functions called/Symbols referenced:
  - `my_bloom_power`: Calculates optimal power-of-two bitset size
  - `optimal_k`: Determines optimal number of hash functions
  - `palloc0`: PostgreSQL memory allocation with zero initialization
  - `BITS_PER_BYTE`: Constant for byte-to-bit conversion
  - `bloom_filter`: The filter structure type

- Called from (representative examples):
  - `roles_list_append`: ACL role list processing
  - `create_and_test_bloom`: Test module functionality

## Notes and Other Information
- Bitset size is constrained to be between 1MB minimum and 512MB maximum (2^32 bits)
- Uses power-of-two sizing for efficient bit manipulation operations
- False positive rate remains below 2% even when bitset size is rounded down significantly
- Seed parameter allows preventing repeated false positives when same dataset is processed multiple times
- Memory allocation includes both the bloom_filter header and the variable-length bitset in a single block