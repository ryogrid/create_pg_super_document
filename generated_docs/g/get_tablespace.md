# get_tablespace

## Location
[src/backend/utils/cache/spccache.c:107-181](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/spccache.c#L107-L181)

## Overview
Retrieves a cached TableSpaceCacheEntry structure for a specified tablespace OID, with automatic cache initialization and lazy loading from the system catalog.

## Definition

```c
static TableSpaceCacheEntry *
get_tablespace(Oid spcid)
```
## Detailed Description
This function serves as the main entry point for accessing tablespace information within PostgreSQL. It implements a two-tiered lookup strategy: first checking the in-memory cache for existing entries, and if not found, retrieving the information from the pg_tablespace system catalog and caching it for future use.

The function handles several important cases: it treats InvalidOid as a request for the default tablespace (MyDatabaseTableSpace), automatically initializes the cache system if not already done, and gracefully handles requests for non-existent tablespaces by creating cache entries with NULL options. The tablespace options are parsed using tablespace_reloptions and stored in the cache memory context for persistence.

A key design consideration is that cache entries are created only after reading from the system catalog to avoid potential issues with cache flushes during catalog access.

## Parameters / Member Variables
- : The OID of the tablespace to look up. InvalidOid is treated as a request for the default tablespace

## Dependencies
- Functions called/Symbols referenced:
  - [InitializeTableSpaceCache](../I/InitializeTableSpaceCache.md): Initialize cache if not already done
  - [hash_search](../h/hash_search.md): Search and create hash table entries (with HASH_FIND and HASH_ENTER)
  - [SearchSysCache1](../S/SearchSysCache1.md): Look up tablespace in system catalog cache
  - [SysCacheGetAttr](../S/SysCacheGetAttr.md): Extract tablespace options from catalog tuple
  - [tablespace_reloptions](../t/tablespace_reloptions.md): Parse and validate tablespace options
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md): Allocate memory in cache context
  - [ReleaseSysCache](../R/ReleaseSysCache.md): Release system cache tuple
  - HeapTupleIsValid: Check if catalog lookup succeeded
- Data structures used:
  - TableSpaceCacheEntry: Cache entry structure for tablespace data
  - [TableSpaceOpts](../T/TableSpaceOpts.md): Parsed tablespace options structure
  - HeapTuple: System catalog tuple
  - TableSpaceCacheHash: Global cache hash table
- Constants used:
  - InvalidOid: Special OID value indicating default tablespace
  - MyDatabaseTableSpace: Default tablespace for current database
  - TABLESPACEOID: System cache identifier for tablespace catalog
  - Anum_pg_tablespace_spcoptions: Column number for tablespace options
- Called from:
  - [get_tablespace_page_costs](get_tablespace_page_costs.md): To retrieve cost configuration
  - [get_tablespace_io_concurrency](get_tablespace_io_concurrency.md): To retrieve I/O concurrency settings
  - [get_tablespace_maintenance_io_concurrency](get_tablespace_maintenance_io_concurrency.md): To retrieve maintenance I/O settings

## Notes and Other Information
- This is a static function, only accessible within the spccache.c module
- Returned pointers should not be stored as they become invalid after cache flushes
- Implements lazy cache initialization for better startup performance
- Handles non-existent tablespaces gracefully by creating entries with NULL options
- Uses CacheMemoryContext for persistent storage of cached options
- The function is designed to be safe against cache invalidation events
- Options are deep-copied into cache memory to ensure data persistence
- Part of PostgreSQL's broader caching strategy for frequently accessed catalog information