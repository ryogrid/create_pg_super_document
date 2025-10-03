# InitializeTableSpaceCache

## Location
[src/backend/utils/cache/spccache.c:78-106](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/spccache.c#L78-L106)

## Overview
Initializes the tablespace cache system by creating the hash table, setting up memory context, and registering invalidation callbacks.

## Definition

```c
static void
InitializeTableSpaceCache(void)
```
## Detailed Description
This function performs the one-time initialization of PostgreSQL's tablespace cache subsystem. It creates a hash table to store tablespace information for quick lookup, ensures the proper memory context is available for cache allocations, and registers a callback function to handle cache invalidation when the pg_tablespace system catalog is modified.

The hash table is configured with Oid as the key (tablespace OID) and TableSpaceCacheEntry as the entry structure. The function uses PostgreSQL's standard hash table creation mechanisms with HASH_ELEM and HASH_BLOBS flags for efficient key-based lookups. It also integrates with the system cache invalidation framework to maintain cache consistency.

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - [hash_create](../h/hash_create.md): Creates the main hash table for tablespace cache
  - [CreateCacheMemoryContext](../C/CreateCacheMemoryContext.md): Ensures cache memory context exists
  - [CacheRegisterSyscacheCallback](../C/CacheRegisterSyscacheCallback.md): Registers invalidation callback for TABLESPACEOID
  - [InvalidateTableSpaceCacheCallback](InvalidateTableSpaceCacheCallback.md): The callback function for cache invalidation
- Data structures used:
  - [HASHCTL](../H/HASHCTL.md): Hash table control structure for configuration
  - TableSpaceCacheEntry: Cache entry structure definition
  - TableSpaceCacheHash: Global hash table variable
  - CacheMemoryContext: Memory context for cache allocations
- Constants used:
  - HASH_ELEM: Hash table creation flag for element-based hashing
  - HASH_BLOBS: Hash table creation flag for blob key handling
  - TABLESPACEOID: System cache ID for tablespace catalog
- Called from:
  - [get_tablespace](../g/get_tablespace.md): Lazy initialization when cache is first accessed

## Notes and Other Information
- This is a static function, only accessible within the spccache.c module
- Performs lazy initialization - only called when the cache is first needed
- Creates a hash table with initial capacity of 16 entries
- Uses the standard PostgreSQL cache invalidation framework for consistency
- The memory context check ensures proper allocation context for cache data
- Part of PostgreSQL's systematic approach to caching frequently accessed catalog information

## Simplified Source

```c
static void
InitializeTableSpaceCache(void)
{
    HASHCTL ctl;

    // Configure hash table parameters
    ctl.keysize = sizeof(Oid);
    ctl.entrysize = sizeof(TableSpaceCacheEntry);

    // Create the hash table
    TableSpaceCacheHash = hash_create("TableSpace cache", 16, &ctl,
                                     HASH_ELEM | HASH_BLOBS);

    // Ensure cache memory context exists
    if (!CacheMemoryContext)
        CreateCacheMemoryContext();

    // Register invalidation callback for tablespace changes
    CacheRegisterSyscacheCallback(TABLESPACEOID,
                                 InvalidateTableSpaceCacheCallback,
                                 (Datum) 0);
}
```