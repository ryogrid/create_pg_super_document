# InvalidateTableSpaceCacheCallback

## Location
[src/backend/utils/cache/spccache.c:55-77](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/spccache.c#L55-L77)

## Overview
A cache invalidation callback function that flushes all tablespace cache entries when the pg_tablespace system catalog is updated.

## Definition

```c
static void
InvalidateTableSpaceCacheCallback(Datum arg, int cacheid, uint32 hashvalue)
```
## Detailed Description
This function serves as a callback for PostgreSQL's cache invalidation system. When the pg_tablespace system catalog is updated (e.g., when tablespaces are created, modified, or dropped), this callback is invoked to ensure cache consistency. Rather than selectively invalidating specific entries, it takes a simple approach of flushing the entire tablespace cache. This design choice is justified by the expectation that tablespaces are not numerous and are infrequently modified, making the performance impact of full cache invalidation negligible while ensuring correctness.

The function iterates through all entries in the TableSpaceCacheHash hash table, properly deallocates any allocated options data, and removes each entry from the hash table.

## Parameters / Member Variables
- : Callback-specific data (unused in this implementation)
- : The cache ID that triggered the invalidation
- : Hash value associated with the invalidated entry (unused in this implementation)

## Dependencies
- Functions called/Symbols referenced:
  - [hash_seq_init](../h/hash_seq_init.md): Initialize hash table sequential scan
  - [hash_seq_search](../h/hash_seq_search.md): Get next entry in sequential scan
  - [hash_search](../h/hash_search.md): Remove entries from hash table with HASH_REMOVE
  - [pfree](../p/pfree.md): Free allocated memory for options
  - elog: Log error if hash table corruption is detected
- Data structures used:
  - HASH_SEQ_STATUS: Hash table sequential scan status
  - TableSpaceCacheEntry: Cache entry structure
  - TableSpaceCacheHash: Global hash table for tablespace cache
- Called from:
  - [InitializeTableSpaceCache](InitializeTableSpaceCache.md): Registered as invalidation callback

## Notes and Other Information
- This is a static function, only accessible within the spccache.c module
- Uses a "flush all" strategy rather than selective invalidation for simplicity
- Properly handles memory management by freeing options data before removing entries
- Includes error checking to detect hash table corruption
- Part of PostgreSQL's systematic cache invalidation mechanism that ensures cache consistency across system catalog updates