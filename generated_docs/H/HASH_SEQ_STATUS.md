# HASH_SEQ_STATUS

## Location
src/include/utils/hsearch.h: 125 - 153

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
- : Pointer to the hash table being iterated over (HTAB type)
- : Index of the current bucket being examined in the hash table
- : Pointer to the current HASHELEMENT entry within the current bucket

## Dependencies
- Functions called/Symbols referenced:
  - HTAB
  - HASHELEMENT
- Called from (representative examples):
  - hash_seq_init
  - hash_seq_search
  - hash_seq_term
  - end_heap_rewrite
  - DropAllPreparedStatements
  - rebuild_database_list
  - LockReleaseAll
  - RelationCacheInvalidate

## Notes and Other Information
- Must be initialized with hash_seq_init() before use
- Should be terminated with hash_seq_term() to clean up resources
- Used extensively throughout PostgreSQL for cleanup operations and cache invalidation
- The iteration order is not guaranteed and depends on the internal hash table organization
- Multiple concurrent iterations on the same hash table are supported with separate status objects
- Commonly used in cleanup routines, cache invalidation callbacks, and administrative functions
- The structure size is small and typically allocated on the stack