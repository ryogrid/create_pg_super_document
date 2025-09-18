# bloom_contains_value

## Location
[src/backend/access/brin/brin_bloom.c:407-433](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin_bloom.c#L407-L433)

## Overview
Checks whether a specific 32-bit value might be contained in the Bloom filter by verifying that all corresponding hash positions have their bits set.

## Definition


## Detailed Description
This function implements the standard Bloom filter lookup operation. It computes the same hash values that would be used when adding the value (using identical seeds and double hashing technique), then checks if all corresponding bits in the filter are set. If any bit is not set, the function returns false immediately, guaranteeing the value was never added. If all bits are set, the function returns true, indicating the value might be in the set (with the possibility of false positives based on the filter's configured false positive rate).

The function uses the same double hashing approach as bloom_add_value to ensure consistency between insertion and lookup operations.

## Parameters / Member Variables
- : Pointer to the BloomFilter structure to query
- : The 32-bit unsigned integer value to check for membership

## Dependencies
- Functions called/Symbols referenced:
  - [hash_bytes_uint32_extended](../h/hash_bytes_uint32_extended.md): PostgreSQL's extended hash function for 32-bit values
  - BLOOM_SEED_1: First seed constant for hash function diversity  
  - BLOOM_SEED_2: Second seed constant for hash function diversity
  - [BloomFilter](../B/BloomFilter.md): The bloom filter data structure
- Called from (representative examples):
  - [brin_bloom_consistent](brin_bloom_consistent.md)

## Notes and Other Information
- The function is static, meaning it's only accessible within the brin_bloom.c file
- Returns false if the value is definitely not in the set (no false negatives)
- Returns true if the value might be in the set (possible false positives)
- Uses identical hashing logic to bloom_add_value for consistency
- Short-circuits on the first unset bit for efficiency
- The double hashing technique mirrors that used in bloom_add_value
- Located in src/backend/access/brin/brin_bloom.c:407-433