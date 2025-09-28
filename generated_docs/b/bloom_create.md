# bloom_create

## Location
[src/backend/lib/bloomfilter.c:87-125](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/lib/bloomfilter.c#L87-L125)

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
  - [my_bloom_power](../m/my_bloom_power.md): Calculates optimal power-of-two bitset size
  - [optimal_k](../o/optimal_k.md): Determines optimal number of hash functions
  - [palloc0](../p/palloc0.md): PostgreSQL memory allocation with zero initialization
  - `BITS_PER_BYTE`: Constant for byte-to-bit conversion
  - [bloom_filter](bloom_filter.md): The filter structure type

- Called from (representative examples):
  - `[roles_list_append](../r/roles_list_append.md)`: ACL role list processing
  - `[create_and_test_bloom](../c/create_and_test_bloom.md)`: Test module functionality

## Notes and Other Information
- Bitset size is constrained to be between 1MB minimum and 512MB maximum (2^32 bits)
- Uses power-of-two sizing for efficient bit manipulation operations
- False positive rate remains below 2% even when bitset size is rounded down significantly
- Seed parameter allows preventing repeated false positives when same dataset is processed multiple times
- Memory allocation includes both the bloom_filter header and the variable-length bitset in a single block

## Simplified Source

```c
// Simplified version of bloom_create
bloom_filter *bloom_create(int64 total_elems, int bloom_work_mem, uint64 seed) {
    bloom_filter *filter;
    int bloom_power;
    uint64 bitset_bytes;
    uint64 bitset_bits;

    // Calculate target bitset size (2 bytes per element)
    bitset_bytes = Min(bloom_work_mem * 1024, total_elems * 2);
    bitset_bytes = Max(1024 * 1024, bitset_bytes);  // Min 1MB

    // Round down to highest power of two
    bloom_power = my_bloom_power(bitset_bytes * BITS_PER_BYTE);
    bitset_bits = 1ULL << bloom_power;
    bitset_bytes = bitset_bits / BITS_PER_BYTE;

    // Allocate filter with bitset
    filter = palloc0(offsetof(bloom_filter, bitset) + sizeof(unsigned char) * bitset_bytes);
    filter->k_hash_funcs = optimal_k(bitset_bits, total_elems);
    filter->seed = seed;
    filter->m = bitset_bits;

    return filter;
}
```

Key simplifications made:
- Preserved size calculation and power-of-two alignment logic
- Maintained memory constraints (1MB min, work_mem max)
- Kept optimal hash function count calculation
- Focused on core allocation and initialization