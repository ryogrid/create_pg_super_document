# HASH_SEQ_STATUS

## Location
[src/include/utils/hsearch.h:125-153](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/hsearch.h#L125-L153)

## Overview
HASH_SEQ_STATUS is a state structure that maintains the current position during sequential iteration through all entries in a PostgreSQL hash table.

## Definition

```c
typedef struct
{
	HTAB	   *hashp;
	uint32		curBucket;		/* index of current bucket */
	HASHELEMENT *curEntry;		/* current entry in bucket */
} HASH_SEQ_STATUS;
```
## Detailed Description
HASH_SEQ_STATUS serves as an iterator state object for traversing all entries in a hash table sequentially. It maintains the necessary state information to continue iteration across hash_seq_search() calls, tracking both the current bucket being examined and the current entry within that bucket.

The structure is used with the hash table sequential search API: hash_seq_init() initializes the status, hash_seq_search() retrieves the next entry and advances the state, and hash_seq_term() cleans up the iteration state. This design allows for safe iteration through hash tables while supporting interruption and resumption of the traversal process.

## Parameters / Member Variables
- `*hashp`: Pointer to the hash table being iterated over (HTAB type)
- `curBucket`: Index of the current bucket being examined in the hash table
- `*curEntry`: Pointer to the current HASHELEMENT entry within the current bucket
## Dependencies
- Functions called/Symbols referenced:
  - [HTAB](HTAB.md)
  - [HASHELEMENT](HASHELEMENT.md)
- Called from (representative examples):
  - [hash_seq_init](../h/hash_seq_init.md)
  - [hash_seq_search](../h/hash_seq_search.md)
  - [hash_seq_term](../h/hash_seq_term.md)
  - [end_heap_rewrite](../e/end_heap_rewrite.md)
  - [DropAllPreparedStatements](../D/DropAllPreparedStatements.md)
  - [rebuild_database_list](../r/rebuild_database_list.md)
  - [LockReleaseAll](../L/LockReleaseAll.md)
  - [RelationCacheInvalidate](../R/RelationCacheInvalidate.md)

## Notes and Other Information
- Must be initialized with hash_seq_init() before use
- Should be terminated with hash_seq_term() to clean up resources
- Used extensively throughout PostgreSQL for cleanup operations and cache invalidation
- The iteration order is not guaranteed and depends on the internal hash table organization
- Multiple concurrent iterations on the same hash table are supported with separate status objects
- Commonly used in cleanup routines, cache invalidation callbacks, and administrative functions
- The structure size is small and typically allocated on the stack