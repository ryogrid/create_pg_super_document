# get_attribute_options

## Location
[src/backend/utils/cache/attoptcache.c:104-178](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/attoptcache.c#L104-L178)

## Overview
Retrieves and caches attribute-specific options for a given table column, providing efficient access to column-level configuration settings.

## Definition
```c
AttributeOpts *get_attribute_options(Oid attrelid, int attnum)
```

## Detailed Description
get_attribute_options is the primary interface for accessing attribute-specific options (such as statistics targets) for table columns in PostgreSQL. It implements a caching mechanism to avoid repeated system catalog lookups for the same attribute options, significantly improving performance when the same attribute options are accessed multiple times.

The function first checks if the requested options are already cached in AttoptCacheHash. If not found, it queries the pg_attribute system catalog to retrieve the attoptions column data. The raw option data is processed through attribute_reloptions() to parse and validate the options, then stored in the cache for future use. The function ensures proper memory management by allocating cache entries in CacheMemoryContext while returning results in the caller's context.

A key implementation detail is the careful ordering of operations: the pg_attribute lookup is performed before creating the cache entry to avoid race conditions where a cache flush (triggered by the syscache lookup) could invalidate a newly created entry.

## Parameters / Member Variables
- `attrelid`: OID of the relation (table) containing the attribute
- `attnum`: Attribute number (column number) within the relation; positive numbers for regular columns

## Dependencies
- Functions called/Symbols referenced:
  - InitializeAttoptCache - Initialize cache if not already done
  - hash_search (HASH_FIND/HASH_ENTER) - Search/insert cache entries
  - SearchSysCache2 - Look up attribute in pg_attribute catalog
  - SysCacheGetAttr - Extract attoptions field from catalog tuple
  - attribute_reloptions - Parse and validate attribute options
  - MemoryContextAlloc - Allocate memory in cache context
  - ReleaseSysCache - Release system catalog tuple
  - palloc - Allocate memory in caller's context
  - memcpy - Copy binary data
- Data structures used:
  - AttoptCacheKey - Cache key structure (attrelid + attnum)
  - AttoptCacheEntry - Cache entry containing parsed options
  - AttributeOpts - Parsed attribute options structure
  - HeapTuple - System catalog tuple representation
- Constants used:
  - HASH_FIND - Hash operation to search for existing entry
  - HASH_ENTER - Hash operation to create new entry
  - ATTNUM - System catalog cache for pg_attribute
  - Anum_pg_attribute_attoptions - Column number for attoptions field
- Called from:
  - do_analyze_rel - During table analysis for statistics collection
  - compute_expr_stats - During extended statistics computation

## Notes and Other Information
- Returns NULL if no options are defined for the specified attribute
- Handles non-existent attributes gracefully by returning NULL rather than erroring
- Uses lazy initialization pattern - cache is created on first access
- Memory management: cache entries use CacheMemoryContext, returned data uses caller's context
- The cache persists across transactions but is invalidated when pg_attribute is modified
- Part of PostgreSQL's performance optimization infrastructure for avoiding repeated catalog lookups
- Primarily used by ANALYZE command and statistics collection subsystems
- Located in src/backend/utils/cache/attoptcache.c as the main entry point for the attribute options caching system