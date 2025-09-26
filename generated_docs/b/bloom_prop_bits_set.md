# bloom_prop_bits_set

## Location
[src/backend/lib/bloomfilter.c:187-209](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/lib/bloomfilter.c#L187-L209)

## Overview  
Calculates and returns the proportion of bits currently set in the Bloom filter's bitset, used for performance analysis and debugging.

## Definition
```c
double bloom_prop_bits_set(bloom_filter *filter)
```

## Detailed Description
The `bloom_prop_bits_set` function computes the ratio of set bits to total bits in the filter's bitset, expressed as a proportion between 0.0 and 1.0. This metric provides insight into the filter's current state and effectiveness.

Under optimal conditions with well-distributed hash functions, this proportion should approach 0.5 (50%) as the filter approaches its designed capacity. Values significantly higher than 0.5 may indicate the filter is oversaturated, leading to higher false positive rates.

The function uses PostgreSQL's efficient `pg_popcount` function to count set bits across the entire bitset in a single operation, making this the only performance instrumentation with low enough overhead for debug traces.

## Parameters / Member Variables  
- `filter`: Pointer to the bloom_filter structure to analyze

## Dependencies
- Functions called/Symbols referenced:
  - [pg_popcount](../p/pg_popcount.md): Efficiently counts set bits in a memory region
  - `BITS_PER_BYTE`: Constant for converting between bits and bytes
  - [bloom_filter](bloom_filter.md): The filter structure type

- Called from (representative examples):
  - `[create_and_test_bloom](../c/create_and_test_bloom.md)`: Test module validation and performance analysis

## Notes and Other Information
- Return value approaches 0.5 under optimal loading conditions regardless of available memory
- Higher values may indicate oversaturation and increased false positive rates  
- Lower values suggest the filter is underutilized relative to its capacity
- This is the preferred metric for runtime debugging due to its low computational overhead
- More direct false positive rate testing is recommended for thorough filter validation
- Uses integer division with double casting for accurate floating-point results