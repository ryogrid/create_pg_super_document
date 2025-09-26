# TSAnyCacheEntry

## Location
src/include/tsearch/ts_cache.h: 23 - 27

## Overview
TSAnyCacheEntry is a common header structure shared by all text search cache entry types in PostgreSQL, providing a standardized interface for cache invalidation operations.

## Definition

```c
typedef struct TSAnyCacheEntry
{
	Oid			objId;
	bool		isvalid;
} TSAnyCacheEntry;
```
## Detailed Description
TSAnyCacheEntry serves as the base structure that must be placed at the beginning of all text search cache entry structures (TSParserCacheEntry, TSDictionaryCacheEntry, etc.). This design pattern allows the InvalidateTSCacheCallBack function to operate on any cache entry type through a common interface, enabling unified cache invalidation across different text search object types.

The structure implements a simple validity tracking mechanism where cache entries can be marked as invalid without being immediately removed from the hash table, allowing for lazy cleanup and reducing the overhead of frequent cache modifications.

## Parameters / Member Variables
- : The object identifier (OID) that serves as the hash lookup key for the cache entry
- : Boolean flag indicating whether the cached information is still valid; set to false during cache invalidation

## Dependencies
- Functions called/Symbols referenced:
  - Oid (PostgreSQL object identifier type)
- Called from (representative examples):
  - InvalidateTSCacheCallBack (for cache invalidation operations)

## Notes and Other Information
- This structure must be the first member of all text search cache entry types to ensure proper memory layout for casting operations
- The objId field is typically the primary hash key and must be placed first in derived structures
- Used as part of PostgreSQL's text search infrastructure to manage cached parser, dictionary, and configuration information
- The invalidation mechanism allows for efficient bulk invalidation of cache entries without immediate memory deallocation