# catclist

## Location
[src/include/utils/catcache.h:159-180](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/catcache.h#L159-L180)

## Overview
The catclist struct represents the result of a partial key search in PostgreSQL's catalog cache, storing a list of cache entries that match the first K columns of an N-key cache.

## Definition

```c
typedef struct catclist
{
	int			cl_magic;		/* for identifying CatCList entries */
#define CL_MAGIC   0x52765103

	uint32		hash_value;		/* hash value for lookup keys */

	dlist_node	cache_elem;		/* list member of per-catcache list */

	/*
	 * Lookup keys for the entry, with the first nkeys elements being valid.
	 * All by-reference are separately allocated.
	 */
	Datum		keys[CATCACHE_MAXKEYS];

	int			refcount;		/* number of active references */
	bool		dead;			/* dead but not yet removed? */
	bool		ordered;		/* members listed in index order? */
	short		nkeys;			/* number of lookup keys specified */
	int			n_members;		/* number of member tuples */
	CatCache   *my_cache;		/* link to owning catcache */
	CatCTup    *members[FLEXIBLE_ARRAY_MEMBER]; /* members */
} CatCList;
```
## Detailed Description
The catclist struct describes the result of a partial search in PostgreSQL's catalog cache system, where only the first K key columns of an N-key cache are used for lookup. It maintains a list of cache entries (CatCTup structures) that satisfy the partial key criteria. Unlike individual cache entries, CatCLists are not organized into hash buckets but are kept in a simple per-cache list. The structure supports reference counting for safe memory management and includes flags for tracking dead entries and whether member tuples are in index order.

## Parameters / Member Variables
- : Magic number (0x52765103) used to identify valid CatCList entries for debugging
- : Precomputed hash value of the partial key values used for the search
- : Doubly-linked list node for organizing CatCLists within the owning cache
- : Array of partial key values used for the search, with only first nkeys elements valid
- : Reference count tracking active usage to prevent premature deletion
- : Flag indicating the list is logically deleted but still referenced
- : Flag indicating whether member tuples appear in index order for optimization
- : Number of key values specified in the partial search (less than cache's total keys)
- : Number of CatCTup entries contained in the members array
- : Back-reference to the owning CatCache structure
- : Flexible array of pointers to CatCTup entries that match the partial key

## Dependencies
- Functions called/Symbols referenced:
  - [dlist_node](../d/dlist_node.md) (doubly-linked list node structure)
  - CATCACHE_MAXKEYS (maximum number of cache keys)
  - CatCache (catalog cache structure)
  - FLEXIBLE_ARRAY_MEMBER (flexible array member marker)
  - CatCTup (catalog cache tuple structure)
- Called from (representative examples):
  - [SearchCatCacheList](../S/SearchCatCacheList.md) (partial key search operations)
  - [ReleaseCatCacheList](../R/ReleaseCatCacheList.md) (list release operations)

## Notes and Other Information
- CatCLists contain only positive cache entries, never negative entries
- Member tuples can be shared between multiple CatCLists if they satisfy different partial key patterns
- The ordered flag helps optimize certain operations like namespace lookups when tuples are in index order
- CatCLists are not organized into hash buckets unlike individual cache entries
- Reference counting ensures lists aren't freed while still being used by client code
- All by-reference datums in the keys array are separately allocated for memory safety
- A CatCList becomes dead if any of its member entries are marked dead