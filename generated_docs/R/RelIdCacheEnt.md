# RelIdCacheEnt

## Location
[src/backend/utils/cache/relcache.c:132-163](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/relcache.c#L132-L163)

## Overview
RelIdCacheEnt is a hash table entry structure that serves as the key-value pair for PostgreSQL's relation cache, indexing cached relation descriptors by their OID for fast lookup.

## Definition

```c
typedef struct inprogressent
{
	Oid			reloid;			/* OID of relation being built */
	bool		invalidated;	/* whether an invalidation arrived for it */
} InProgressEnt;
```
## Detailed Description
RelIdCacheEnt is a fundamental data structure in PostgreSQL's relation cache system. It serves as the hash table entry type for RelationIdCache, which is the main hash table that indexes relation descriptors by their Object Identifier (OID). This structure was part of a simplification where PostgreSQL previously indexed the cache by both name and OID, but now only uses OID-based indexing for better performance and simplicity.

The relation cache is critical for PostgreSQL's performance as it avoids repeatedly reading system catalogs to get relation metadata. Each RelIdCacheEnt entry in the hash table maps a relation's OID to its complete relation descriptor, allowing fast access to table/index metadata during query processing.

## Parameters / Member Variables
- `reloid`: The Object Identifier (OID) of the relation - serves as the hash key for cache lookups
- `invalidated`: Pointer to the complete Relation descriptor containing all metadata about the relation (table, index, etc.)
## Dependencies
- Functions called/Symbols referenced:
  - [HTAB](../H/HTAB.md) (used by RelationIdCache)
- Called from (representative examples):
  - RelationCacheInsert
  - RelationIdCacheLookup  
  - RelationCacheDelete
  - [RelationCacheInvalidate](RelationCacheInvalidate.md)
  - [RelationCacheInitializePhase3](RelationCacheInitializePhase3.md)
  - [write_relcache_init_file](../w/write_relcache_init_file.md)

## Notes and Other Information
- This structure is used exclusively within the relation cache implementation in relcache.c
- The hash table using this structure (RelationIdCache) is static and not exposed outside the relcache module
- The design reflects PostgreSQL's evolution from dual-indexed (name and OID) to OID-only relation caching
- Critical for performance as it enables O(1) relation descriptor lookups during query execution
- Part of PostgreSQL's sophisticated caching system that balances memory usage with lookup performance