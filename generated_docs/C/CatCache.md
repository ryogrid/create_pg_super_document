# CatCache

## Location
src/include/utils/catcache.h: 85 - 87

## Overview
CatCache is a core data structure that represents an individual catalog cache in PostgreSQL, providing fast access to system catalog tuples by maintaining an in-memory hash table with LRU eviction and support for both positive and negative cache entries.

## Definition

```c
* cc_searches - (cc_hits + cc_neg_hits + cc_newloads) is number of failed
	 * searches, each of which will result in loading a negative entry
	 */
	long		cc_invals;		/* # of entries invalidated from cache */
	long		cc_lsearches;	/* total # list-searches */
	long		cc_lhits;		/* # of matches against existing lists */
#endif
} CatCache;


typedef struct catctup
```
## Detailed Description
CatCache is the fundamental structure for PostgreSQL's catalog caching system, which provides high-performance access to system catalog information. Each CatCache instance manages a hash table of catalog tuples (CatCTup) for a specific system relation, allowing fast lookups by key combinations instead of requiring expensive sequential scans or index lookups on system tables.

The cache uses a hash bucket organization with separate chaining for collision resolution. It supports up to 4 lookup keys (CATCACHE_MAXKEYS) and maintains both individual tuple entries and list entries (CatCList) for partial key searches. The cache implements LRU (Least Recently Used) ordering within each hash bucket to optimize repeated access patterns.

A key feature is support for negative cache entries, which record the absence of tuples matching certain keys, preventing repeated failed searches. The cache also maintains statistics when compiled with CATCACHE_STATS to monitor performance.

## Parameters / Member Variables
- uid=1000(ryo) gid=1000(ryo) groups=1000(ryo),4(adm),20(dialout),24(cdrom),25(floppy),27(sudo),29(audio),30(dip),44(video),46(plugdev),117(netdev),998(ollama),999(docker): Unique identifier for this cache instance, used to distinguish different system catalog caches
- : Number of hash buckets for individual tuple storage
- : Tuple descriptor defining the structure of cached tuples
- : Array of hash bucket heads for organizing cached tuples
- : Array of hash functions, one for each potential lookup key
- : Array of fast equality comparison functions for each key type
- : Array of attribute numbers corresponding to lookup keys in the relation
- : Number of lookup keys defined for this cache (1 to 4)
- : Current number of individual tuples stored in this cache
- : Current number of CatCList objects (partial search results) in this cache
- : Number of hash buckets dedicated to CatCList storage
- : Array of hash bucket heads for organizing CatCList entries
- : Name of the system relation being cached
- : OID of the system relation being cached
- : OID of the unique index used for lookups (must match cache keys)
- : Whether the cached relation is shared across all databases
- : Linked list node for connecting to other caches in the global cache header
- : Pre-built ScanKey structures for heap scans when cache misses occur
- Statistics fields (when CATCACHE_STATS enabled): Various counters for cache hits, misses, invalidations, and list operations

## Dependencies
- Functions called/Symbols referenced:
  - dlist_head (doubly-linked list infrastructure)
  - CCHashFN (hash function type)
  - CCFastEqualFN (equality function type)
  - TupleDesc (tuple descriptor)
  - ScanKeyData (scan key structure)
  - HeapTuple (tuple data structure)

- Called from (representative examples):
  - InitCatCache (cache initialization)
  - SearchCatCache family (cache lookup functions)
  - CatalogCacheComputeHashValue (hash computation)
  - CatCacheInvalidate (cache invalidation)
  - ResetCatalogCache (cache reset operations)

## Notes and Other Information
- Each catalog cache must have a corresponding unique index on the system table that exactly matches the cache lookup keys
- The cache supports both positive entries (actual tuples) and negative entries (assertions that no matching tuple exists)
- Cache entries use reference counting to manage memory and ensure consistency during concurrent access
- The LRU ordering within hash buckets helps optimize performance for frequently accessed entries
- Statistics collection is optional and controlled by the CATCACHE_STATS compilation flag
- Maximum of 4 lookup keys per cache is enforced by CATCACHE_MAXKEYS constant
- Cache invalidation is handled through the PrepareToInvalidateCacheTuple callback system