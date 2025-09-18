# catcache

## Location
src/include/utils/catcache.h: 44 - 84

## Overview
The catcache struct represents an individual catalog cache in PostgreSQL, serving as a low-level cache for system catalog tuples to improve performance by avoiding repeated catalog table lookups.

## Definition


## Detailed Description
The catcache struct is the core data structure for PostgreSQL's catalog cache system. Each cache instance manages cached tuples from a specific system catalog table, using hash tables for fast lookup by key values. The cache supports up to 4 keys (CATCACHE_MAXKEYS) and maintains both individual tuple caches and list caches for partial key matches. The structure includes hash functions and equality functions for efficient key comparison, along with metadata about the underlying relation and index.

## Parameters / Member Variables
- uid=1000(ryo) gid=1000(ryo) groups=1000(ryo),4(adm),20(dialout),24(cdrom),25(floppy),27(sudo),29(audio),30(dip),44(video),46(plugdev),117(netdev),998(ollama),999(docker): Unique identifier for this cache, defined in syscache.h
- : Number of hash buckets used for tuple storage
- : Tuple descriptor copied from the cached relation's descriptor
- : Array of hash bucket heads for storing cached tuples
- : Array of hash functions, one for each cache key
- : Array of equality comparison functions for each key
- : Array of attribute numbers for each cache key
- : Number of keys used by this cache (1 to CATCACHE_MAXKEYS)
- : Current number of tuples stored in this cache
- : Current number of CatCList objects in this cache
- : Number of hash buckets for CatCList storage
- : Array of hash bucket heads for storing CatCLists
- : Name of the system catalog relation being cached
- : OID of the cached relation
- : OID of the index used for cache key lookups
- : Whether the cached relation is shared across databases
- : Link node for global cache list management
- : Precomputed scan key data for heap scans
- : (Stats) Total number of cache searches performed
- : (Stats) Number of successful cache hits
- : (Stats) Number of negative cache hits
- : (Stats) Number of new entries loaded from disk
- : (Stats) Number of cache invalidations
- : (Stats) Number of list searches performed
- : (Stats) Number of successful list cache hits

## Dependencies
- Functions called/Symbols referenced:
  - dlist_head (doubly-linked list structure)
  - CATCACHE_MAXKEYS (maximum number of cache keys)
  - slist_node (singly-linked list node)
  - ScanKeyData (scan key structure)
- Called from (representative examples):
  - InitCatCache (cache initialization)
  - SearchCatCache functions (cache lookup operations)

## Notes and Other Information
- Each catalog cache must have a corresponding unique index on the system table for key-based lookups
- The cache uses hash tables for O(1) average-case lookup performance
- Statistics collection is conditionally compiled with CATCACHE_STATS
- The structure supports both positive caching (storing actual tuples) and negative caching (remembering that no tuple exists for given keys)
- Cache invalidation is handled through the PostgreSQL invalidation message system