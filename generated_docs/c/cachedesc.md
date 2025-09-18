# cachedesc

## Location
src/backend/utils/cache/syscache.c: 69 - 78

## Overview
The  struct defines the configuration information for a single system cache in PostgreSQL's catalog cache system.

## Definition


## Detailed Description
The  structure serves as a configuration template that describes how to set up a system catalog cache. Each instance of this struct defines the parameters needed to create a cache for a specific PostgreSQL system catalog relation. The struct is used during cache initialization to create  objects that provide fast lookup capabilities for frequently accessed catalog data.

System caches in PostgreSQL are hash-based lookup tables that cache tuples from system catalog relations to avoid repeated disk I/O for metadata operations. Each cache must be backed by a unique index on the underlying relation, and the cache configuration specified in  must match the key structure of that index.

## Parameters / Member Variables
- : The OID (object identifier) of the system catalog relation that this cache will store data from
- : The OID of the unique index that backs this cache - this index's key structure must match the cache key specification
- : The number of key attributes used for cache lookups (maximum of 4)
- : An array containing the attribute numbers of the key columns used for cache lookups
- : The number of hash buckets to allocate for this cache (must be a power of 2)

## Dependencies
- Functions called/Symbols referenced:
  - Oid (from src/include/postgres_ext.h:31)
- Called from (representative examples):
  - InitCatalogCache() (uses cacheinfo array of cachedesc structs)
  - InitCatCache() (receives parameters from cachedesc during cache creation)

## Notes and Other Information
- The struct is defined locally in syscache.c and is used to populate a static array that drives cache initialization
- The number of hash buckets should be chosen based on the expected number of entries in a medium-size database
- Each cache requires a unique index whose key matches the cache key specification
- The maximum number of key attributes is 4, as defined by the fixed-size key array
- This structure is part of PostgreSQL's catalog cache subsystem, which provides fast access to frequently-used catalog information