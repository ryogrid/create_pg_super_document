# expand_table

## Location
[src/backend/utils/hash/dynahash.c:1511-1607](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/hash/dynahash.c#L1511-L1607)

## Overview
Expands a hash table by adding one more hash bucket, redistributing existing entries to maintain proper hash distribution.

## Definition


## Detailed Description
The expand_table function is a critical component of PostgreSQL's dynamic hash table implementation that handles table growth. When called, it adds exactly one new hash bucket to the table and redistributes existing entries between the old bucket and the newly created bucket based on their hash values. The function carefully manages the hash table's internal structures including segments, buckets, and hash masks. It allocates new segments when necessary and updates the table's masking parameters to accommodate the larger bucket space. The redistribution process ensures that only entries from one specific old bucket need to be examined and potentially moved to the new bucket, maintaining the hash table's performance characteristics.

## Parameters / Member Variables
- : Pointer to the HTAB (hash table) structure to be expanded

## Dependencies
- Functions called/Symbols referenced:
  - [dir_realloc](../d/dir_realloc.md)
  - [seg_alloc](../s/seg_alloc.md)
  - [calc_bucket](../c/calc_bucket.md)
  - IS_PARTITIONED
  - MOD
  - [HASHHDR](../H/HASHHDR.md), HASHSEGMENT, HASHBUCKET (structure access)
- Called from (representative examples):
  - [hash_search_with_hash_value](../h/hash_search_with_hash_value.md)

## Notes and Other Information
- Returns true on success, false on failure (typically due to memory allocation failure)
- This is a static function, only used internally within dynahash.c
- Cannot be used on partitioned hash tables (checked with Assert)
- Increments hash_expansions counter when HASH_STATISTICS is enabled
- Only one old bucket needs to be split due to the hash masking algorithm used
- Updates low_mask and high_mask when crossing power-of-2 boundaries
- Terminates rebuilt hash chains with NULL pointers to prevent corruption
- Part of the PostgreSQL dynamic hash table expansion mechanism