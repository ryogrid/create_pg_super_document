# XLogPrefetcherIsFiltered

## Location
[src/backend/access/transam/xlogprefetcher.c:916-963](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogprefetcher.c#L916-L963)

## Overview
Checks whether a specific block should be skipped for prefetching due to active filters that prevent access to certain relations or block ranges.

## Definition

```c
static inline bool
XLogPrefetcherIsFiltered(XLogPrefetcher *prefetcher, RelFileLocator rlocator,
						 BlockNumber blockno)
```
## Detailed Description
This function implements the filtering logic that determines whether a specific block should be excluded from prefetching. It performs a two-level filtering check:

1. **Relation-specific filtering**: First checks if there's an active filter for the specific relation that covers the requested block number. This handles cases where individual relations are being created, extended, or truncated.

2. **Database-level filtering**: If no relation-specific filter applies, checks for database-level filters that affect all relations within a database. This handles cases like database creation with file-copy strategy.

The function is optimized for the common case where no filters are active by first checking if the filter queue is empty, avoiding hash table lookups when possible. When filters are present, it performs efficient hash table lookups to determine if the block should be filtered.

## Parameters / Member Variables
- : Pointer to the XLogPrefetcher structure containing the filter infrastructure
- : RelFileLocator identifying the relation (tablespace, database, relation)
- : Block number being checked for filtering

## Dependencies
- Functions called/Symbols referenced:
  -  - Check if filter queue has any entries
  -  - Look up filters in hash table using HASH_FIND
  -  - Constant for database-level filter lookup
- Called from (representative examples):
  -  - Main prefetcher logic to check if blocks should be skipped

## Notes and Other Information
- Returns  if the block should be filtered (skipped),  if prefetching is allowed
- Uses  optimization hint assuming filter queue is usually empty
- Supports extensive debugging output via  when filters are applied
- Database-level filtering uses  and  as wildcards
- Inline function for optimal performance in the prefetching hot path
- Critical safety mechanism to prevent reading non-existent or invalid blocks during recovery

## Simplified Source
```c
static inline bool XLogPrefetcherIsFiltered(XLogPrefetcher *prefetcher,
                                          RelFileLocator rlocator,
                                          BlockNumber blockno) {
    // Fast path: if no filters active, allow prefetching
    if (unlikely(!dlist_is_empty(&prefetcher->filter_queue))) {
        XLogPrefetcherFilter *filter;

        // Check for relation-specific filter
        filter = hash_search(prefetcher->filter_table, &rlocator, HASH_FIND, NULL);
        if (filter && filter->filter_from_block <= blockno) {
            return true; // Block is filtered
        }

        // Check for database-level filter
        rlocator.relNumber = InvalidRelFileNumber;
        rlocator.spcOid = InvalidOid;
        filter = hash_search(prefetcher->filter_table, &rlocator, HASH_FIND, NULL);
        if (filter) {
            return true; // Database is filtered
        }
    }

    return false; // Block can be prefetched
}
```