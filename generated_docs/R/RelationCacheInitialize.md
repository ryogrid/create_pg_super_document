# RelationCacheInitialize

## Location
src/backend/utils/cache/relcache.c: 3997 - 4042

## Overview
RelationCacheInitialize initializes the relation cache system, setting up the hash table for storing relation descriptors and allocating necessary data structures.

## Definition


## Detailed Description
This function performs the fundamental initialization of PostgreSQL's relation cache (relcache) system during backend startup. The relcache is a critical component that maintains in-memory copies of relation metadata to avoid repeatedly reading from system catalogs.

The initialization process involves several key steps:

1. **Cache Memory Context**: Ensures the CacheMemoryContext exists by calling CreateCacheMemoryContext() if needed. This memory context persists for the lifetime of the backend process and holds all cached relation descriptors.

2. **Hash Table Creation**: Creates the primary hash table that indexes cached relations by their OID (Object Identifier). The hash table uses:
   - Key size of sizeof(Oid) for relation OIDs
   - Entry size of sizeof(RelIdCacheEnt) for cache entries
   - Initial size of INITRELCACHESIZE (typically 400 entries)
   - HASH_ELEM and HASH_BLOBS flags for proper key handling

3. **In-Progress List Allocation**: Allocates an initial array to track relations currently being loaded into the cache. This prevents infinite recursion during cache loading when relations reference each other. The initial allocation provides 4 slots, which can grow as needed.

4. **Relation Mapper Initialization**: Calls RelationMapInitialize() to set up the relation mapping system, which handles the special mapping between system catalog OIDs and their physical file locations.

## Parameters / Member Variables
None - this function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - CreateCacheMemoryContext
  - hash_create
  - MemoryContextAlloc  
  - RelationMapInitialize
- Called from (representative examples):
  - InitPostgres

## Notes and Other Information
- Called once per backend during database initialization in InitPostgres()
- Must be called before any relation cache operations can be performed
- The hash table grows automatically as more relations are cached
- The in_progress_list prevents infinite recursion during cache loading of interdependent relations
- RelationMapInitialize() is essential for system catalog access since some catalogs use mapped storage
- All memory allocations use CacheMemoryContext to ensure persistence across transactions
- The initial cache size (INITRELCACHESIZE) is optimized for typical workloads but will expand dynamically
- This is part of the broader cache initialization sequence during backend startup