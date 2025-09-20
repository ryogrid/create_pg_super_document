# bloom_add_value

## Location
[src/backend/access/brin/brin_bloom.c:370-406](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin_bloom.c#L370-L406)

## Overview
Adds a 32-bit value to an existing Bloom filter by computing multiple hash functions and setting the corresponding bits in the filter's bit array.

## Definition

```c
static BloomFilter *
bloom_add_value(BloomFilter *filter, uint32 value, bool *updated)
```
## Detailed Description
This function implements the standard Bloom filter insertion algorithm. It computes two primary hash values using different seeds (BLOOM_SEED_1 and BLOOM_SEED_2) and then generates the required number of hash functions using double hashing: h(i) = (h1 + i * h2) % nbits. For each generated hash value, it sets the corresponding bit in the filter's data array. The function tracks whether any new bits were set and updates a counter of set bits for monitoring filter saturation.

The double hashing approach provides good hash function independence while being computationally efficient compared to computing entirely separate hash functions.

## Parameters / Member Variables
- : Pointer to the BloomFilter structure to add the value to
- : The 32-bit unsigned integer value to add to the filter
- : Optional output parameter that indicates whether any new bits were set (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [hash_bytes_uint32_extended](../h/hash_bytes_uint32_extended.md): PostgreSQL's extended hash function for 32-bit values
  - BLOOM_SEED_1: First seed constant for hash function diversity
  - BLOOM_SEED_2: Second seed constant for hash function diversity
  - [BloomFilter](../B/BloomFilter.md): The bloom filter data structure
- Called from (representative examples):
  - [brin_bloom_add_value](brin_bloom_add_value.md)

## Notes and Other Information
- The function is static, meaning it's only accessible within the brin_bloom.c file
- Returns the same filter pointer that was passed in (for API consistency)
- Uses double hashing technique for generating multiple independent hash functions
- Tracks the number of set bits in the filter (nbits_set) for saturation monitoring
- Only sets the updated flag to true if new bits were actually set (not if they were already set)
- The bit manipulation uses standard techniques: byte indexing and bit masking
- Located in src/backend/access/brin/brin_bloom.c:370-406