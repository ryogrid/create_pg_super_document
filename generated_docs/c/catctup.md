# catctup

## Location
[src/include/utils/catcache.h:88-135](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/catcache.h#L88-L135)

## Overview
The catctup struct represents an individual cached tuple in PostgreSQL's catalog cache system, containing both the tuple data and metadata needed for cache management.

## Definition

```c
typedef struct catctup
{
	int			ct_magic;		/* for identifying CatCTup entries */
#define CT_MAGIC   0x57261502

	uint32		hash_value;		/* hash value for this tuple's keys */

	/*
	 * Lookup keys for the entry. By-reference datums point into the tuple for
	 * positive cache entries, and are separately allocated for negative ones.
	 */
	Datum		keys[CATCACHE_MAXKEYS];

	/*
	 * Each tuple in a cache is a member of a dlist that stores the elements
	 * of its hash bucket.  We keep each dlist in LRU order to speed repeated
	 * lookups.
	 */
	dlist_node	cache_elem;		/* list member of per-bucket list */

	/*
	 * A tuple marked "dead" must not be returned by subsequent searches.
	 * However, it won't be physically deleted from the cache until its
	 * refcount goes to zero.  (If it's a member of a CatCList, the list's
	 * refcount must go to zero, too; also, remember to mark the list dead at
	 * the same time the tuple is marked.)
	 *
	 * A negative cache entry is an assertion that there is no tuple matching
	 * a particular key.  This is just as useful as a normal entry so far as
	 * avoiding catalog searches is concerned.  Management of positive and
	 * negative entries is identical.
	 */
	int			refcount;		/* number of active references */
	bool		dead;			/* dead but not yet removed? */
	bool		negative;		/* negative cache entry? */
	HeapTupleData tuple;		/* tuple management header */

	/*
	 * The tuple may also be a member of at most one CatCList.  (If a single
	 * catcache is list-searched with varying numbers of keys, we may have to
	 * make multiple entries for the same tuple because of this restriction.
	 * Currently, that's not expected to be common, so we accept the potential
	 * inefficiency.)
	 */
	struct catclist *c_list;	/* containing CatCList, or NULL if none */

	CatCache   *my_cache;		/* link to owning catcache */
	/* properly aligned tuple data follows, unless a negative entry */
} CatCTup;
```
## Detailed Description
The catctup struct represents a single cached catalog tuple within PostgreSQL's catalog cache system. It serves as a wrapper around HeapTupleData with additional cache management metadata. The structure supports both positive entries (containing actual tuple data) and negative entries (indicating that no tuple exists for the given keys). Each cached tuple is organized into hash buckets for efficient lookup and maintains LRU ordering within buckets. The structure includes reference counting for safe memory management and supports membership in CatCList objects for partial key searches.

## Parameters / Member Variables
- `ct_magic`: Magic number (0x57261502) used to identify valid CatCTup entries for debugging
- `hash_value`: Precomputed hash value of the tuple's key values for efficient bucket placement
- `keys[CATCACHE_MAXKEYS]`: Array of key values used for cache lookups, supporting up to CATCACHE_MAXKEYS
- `cache_elem`: Doubly-linked list node for organizing tuples within hash buckets in LRU order
- `refcount`: Reference count tracking active usage to prevent premature deletion
- `dead`: Flag indicating the tuple is logically deleted but still referenced
- `negative`: Flag indicating this is a negative cache entry (no matching tuple exists)
- `tuple`: HeapTupleData structure containing the actual tuple data and metadata
- `*c_list`: Pointer to containing CatCList if this tuple is part of a list search result
- `*my_cache`: Back-reference to the owning CatCache structure
## Dependencies
- Functions called/Symbols referenced:
  - CATCACHE_MAXKEYS (maximum number of cache keys)
  - [dlist_node](../d/dlist_node.md) (doubly-linked list node structure)
  - [HeapTupleData](../H/HeapTupleData.md) (heap tuple data structure)
  - [catclist](catclist.md) (catalog cache list structure)
  - [CatCache](../C/CatCache.md) (catalog cache structure)
- Called from (representative examples):
  - [CatCacheRemoveCTup](../C/CatCacheRemoveCTup.md) (cache tuple removal)
  - [SearchCatCache](../S/SearchCatCache.md) functions (cache lookup operations)

## Notes and Other Information
- The actual tuple data follows the struct in memory for positive entries
- Negative entries don't contain tuple data but use the same structure for consistent management
- The magic number helps detect memory corruption and invalid pointers during debugging
- Reference counting ensures tuples aren't freed while still in use by client code
- LRU ordering within hash buckets improves cache hit rates for frequently accessed tuples
- A tuple can belong to at most one CatCList to simplify memory management