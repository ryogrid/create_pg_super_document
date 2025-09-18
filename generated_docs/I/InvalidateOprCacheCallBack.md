# InvalidateOprCacheCallBack

## Location
src/backend/parser/parse_oper.c: 1036 - 1053

## Overview
Callback function that flushes all entries from the operator cache when pg_operator or pg_cast system catalogs are modified.

## Definition
```c
static void InvalidateOprCacheCallBack(Datum arg, int cacheid, uint32 hashvalue)
```

## Detailed Description
This function serves as an invalidation callback for PostgreSQL's operator cache system. It is registered with the system cache invalidation mechanism to be called whenever changes occur to the pg_operator or pg_cast system catalogs. When invoked, it performs a complete flush of the operator cache by iterating through all cache entries and removing them one by one.

The function uses a simple but effective strategy: rather than trying to determine which specific cache entries might be affected by catalog changes, it removes all entries to ensure cache consistency. This conservative approach guarantees that stale cache entries never persist after catalog modifications.

## Parameters / Member Variables
- `arg`: Callback argument (not used, passed as Datum)
- `cacheid`: System cache ID that triggered the invalidation
- `hashvalue`: Hash value related to the invalidated cache entry (not used for full flush)

## Dependencies
- Functions called/Symbols referenced:
  - [hash_seq_init](../h/hash_seq_init.md)
  - [hash_seq_search](../h/hash_seq_search.md)  
  - [hash_search](../h/hash_search.md) (with HASH_REMOVE flag)
  - elog (for error reporting)
- Called from (representative examples):
  - System cache invalidation mechanism (registered in find_oper_cache_entry)

## Notes and Other Information
- Registered for both OPERNAMENSP and CASTSOURCETARGET system cache callbacks
- Uses hash table sequential scan to iterate through all entries
- Conservative approach: flushes entire cache rather than selective invalidation
- Includes error checking for hash table corruption detection
- Essential for maintaining cache consistency when operators or casts are modified
- Comment acknowledges that more targeted invalidation could be implemented but is complex