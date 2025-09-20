# MemoizeEntry

## Location
[src/backend/executor/nodeMemoize.c:115-123](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeMemoize.c#L115-L123)

## Overview
MemoizeEntry is the main data structure stored in PostgreSQL's memoization hash table, containing cached results and metadata for a specific set of input parameters.

## Definition

```c
typedef struct MemoizeEntry
{
	MemoizeKey *key;			/* Hash key for hash table lookups */
	MemoizeTuple *tuplehead;	/* Pointer to the first tuple or NULL if no
								 * tuples are cached for this entry */
	uint32		hash;			/* Hash value (cached) */
	char		status;			/* Hash status */
	bool		complete;		/* Did we read the outer plan to completion? */
} MemoizeEntry;
```
## Detailed Description
MemoizeEntry represents a complete cache entry in PostgreSQL's memoization system. It serves as the value type in the hash table, containing all necessary information about a cached result set for a specific combination of input parameters. The structure maintains a pointer to the hash key, a linked list of result tuples, cached hash value for performance, hash table status information, and a completion flag indicating whether the outer plan was fully processed.

The entry supports scenarios where multiple tuples may be returned for the same input parameters, using a linked list of MemoizeTuple structures. The completion flag is particularly important for determining whether additional tuples might be available from the outer plan, enabling proper cache behavior for partial result sets.

## Parameters / Member Variables
- : Pointer to the MemoizeKey containing parameter values and LRU list management
- : Pointer to the first MemoizeTuple in the linked list of cached results, or NULL if no tuples are cached
- hash: hash table empty: Cached hash value for efficient hash table operations without recalculation
- : Hash table status information (managed by the specialized hash table implementation)
- : Boolean flag indicating whether the outer plan has been read to completion for this parameter set

## Dependencies
- Functions called/Symbols referenced:
  - [MemoizeKey](MemoizeKey.md)
  - [MemoizeTuple](MemoizeTuple.md)
- Called from (representative examples):
  - [entry_purge_tuples](../e/entry_purge_tuples.md)
  - [remove_cache_entry](../r/remove_cache_entry.md)
  - [cache_reduce_memory](../c/cache_reduce_memory.md)
  - [cache_lookup](../c/cache_lookup.md)
  - [cache_store_tuple](../c/cache_store_tuple.md)
  - [ExecMemoize](../E/ExecMemoize.md)
  - [ExecEndMemoize](../E/ExecEndMemoize.md)

## Notes and Other Information
- Used as SH_ELEMENT_TYPE in the specialized hash table implementation
- The complete flag prevents unnecessary re-execution of fully cached result sets
- Memory overhead calculations include this structure size in cache management decisions
- The hash value caching improves performance by avoiding recalculation during hash table operations
- Proper cleanup of the tuplehead linked list is essential during entry removal