# catcache

## Location
[src/include/utils/catcache.h:44-84](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/catcache.h#L44-L84)

## Overview
The catcache struct represents an individual catalog cache in PostgreSQL, serving as a low-level cache for system catalog tuples to improve performance by avoiding repeated catalog table lookups.

## Definition

```c
typedef struct catcache
{
	int			id;				/* cache identifier --- see syscache.h */
	int			cc_nbuckets;	/* # of hash buckets in this cache */
	TupleDesc	cc_tupdesc;		/* tuple descriptor (copied from reldesc) */
	dlist_head *cc_bucket;		/* hash buckets */
	CCHashFN	cc_hashfunc[CATCACHE_MAXKEYS];	/* hash function for each key */
	CCFastEqualFN cc_fastequal[CATCACHE_MAXKEYS];	/* fast equal function for
													 * each key */
	int			cc_keyno[CATCACHE_MAXKEYS]; /* AttrNumber of each key */
	int			cc_nkeys;		/* # of keys (1..CATCACHE_MAXKEYS) */
	int			cc_ntup;		/* # of tuples currently in this cache */
	int			cc_nlist;		/* # of CatCLists currently in this cache */
	int			cc_nlbuckets;	/* # of CatCList hash buckets in this cache */
	dlist_head *cc_lbucket;		/* hash buckets for CatCLists */
	const char *cc_relname;		/* name of relation the tuples come from */
	Oid			cc_reloid;		/* OID of relation the tuples come from */
	Oid			cc_indexoid;	/* OID of index matching cache keys */
	bool		cc_relisshared; /* is relation shared across databases? */
	slist_node	cc_next;		/* list link */
	ScanKeyData cc_skey[CATCACHE_MAXKEYS];	/* precomputed key info for heap
											 * scans */

	/*
	 * Keep these at the end, so that compiling catcache.c with CATCACHE_STATS
	 * doesn't break ABI for other modules
	 */
#ifdef CATCACHE_STATS
	long		cc_searches;	/* total # searches against this cache */
	long		cc_hits;		/* # of matches against existing entry */
	long		cc_neg_hits;	/* # of matches against negative entry */
	long		cc_newloads;	/* # of successful loads of new entry */

	/*
	 * cc_searches - (cc_hits + cc_neg_hits + cc_newloads) is number of failed
	 * searches, each of which will result in loading a negative entry
	 */
	long		cc_invals;		/* # of entries invalidated from cache */
	long		cc_lsearches;	/* total # list-searches */
	long		cc_lhits;		/* # of matches against existing lists */
#endif
} CatCache;
```
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
  - [dlist_head](../d/dlist_head.md) (doubly-linked list structure)
  - CATCACHE_MAXKEYS (maximum number of cache keys)
  - [slist_node](../s/slist_node.md) (singly-linked list node)
  - [ScanKeyData](../S/ScanKeyData.md) (scan key structure)
- Called from (representative examples):
  - [InitCatCache](../I/InitCatCache.md) (cache initialization)
  - [SearchCatCache](../S/SearchCatCache.md) functions (cache lookup operations)

## Notes and Other Information
- Each catalog cache must have a corresponding unique index on the system table for key-based lookups
- The cache uses hash tables for O(1) average-case lookup performance
- Statistics collection is conditionally compiled with CATCACHE_STATS
- The structure supports both positive caching (storing actual tuples) and negative caching (remembering that no tuple exists for given keys)
- Cache invalidation is handled through the PostgreSQL invalidation message system