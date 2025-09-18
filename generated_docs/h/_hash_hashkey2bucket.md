# _hash_hashkey2bucket

## Location
[src/backend/access/hash/hashutil.c:125-141](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/hash/hashutil.c#L125-L141)

## Overview
Function that maps a hash key to its corresponding bucket number using the hash index's split algorithm.

## Definition
```c
Bucket _hash_hashkey2bucket(uint32 hashkey, uint32 maxbucket, uint32 highmask, uint32 lowmask)
```

## Detailed Description
This function implements the core bucket selection algorithm for PostgreSQL's hash indexes. It uses a two-level masking approach to determine which bucket a given hash key should map to. The algorithm first applies the high mask to the hash key, then checks if the result exceeds the maximum bucket number. If it does, it applies the low mask instead to map to a bucket in the lower range.

This design supports the dynamic bucket splitting mechanism used in hash indexes, where buckets are split incrementally as the index grows. The high and low masks correspond to different levels in the split sequence, allowing the index to gradually expand its bucket space while maintaining consistent hash key distribution.

## Parameters / Member Variables
- `hashkey`: uint32 hash key value to be mapped to a bucket
- `maxbucket`: uint32 maximum valid bucket number currently in use
- `highmask`: uint32 bitmask for the higher-level bucket calculation
- `lowmask`: uint32 bitmask for the lower-level bucket calculation (fallback)

## Dependencies
- Functions called/Symbols referenced:
  - Bucket (bucket number type)
- Called from (representative examples):
  - [hashbucketcleanup](hashbucketcleanup.md) (in hash.c at line 752)
  - [_hash_splitbucket](_hash_splitbucket.md) (in hashpage.c at line 1152)
  - [_hash_getbucketbuf_from_hashkey](_hash_getbucketbuf_from_hashkey.md) (in hashpage.c at line 1584)
  - [_h_indexbuild](_h_indexbuild.md) (in hashsort.c at line 142)
  - [comparetup_index_hash](../c/comparetup_index_hash.md) (in tuplesortvariants.c at lines 1606 and 1610)

## Notes and Other Information
- Core algorithm for bucket selection in PostgreSQL's hash indexes
- Supports incremental bucket splitting through dual masking approach
- Simple but effective algorithm that maintains good hash distribution
- Used throughout the hash index implementation for bucket mapping
- Critical for both search operations and index maintenance tasks
- The dual-mask approach allows for gradual index expansion without rehashing all data