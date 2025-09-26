# CatCList

## Location
[src/include/utils/catcache.h:181-183](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/catcache.h#L181-L183)

## Overview
CatCList represents the result of a partial catalog search in PostgreSQL, storing cache entries for all table rows that match a partial key combination when searching with fewer keys than the cache's maximum.

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
CatCList is a specialized cache structure that stores the results of partial key searches on catalog caches. When a search is performed using only the first K columns of an N-key cache (where K < N), PostgreSQL creates a CatCList to hold all tuples that match those K keys. This optimization allows subsequent searches with the same partial key to avoid scanning the underlying system catalog.

The structure maintains an array of pointers to CatCTup entries that match the partial key combination. These member tuples are never negative cache entries - only actual catalog tuples are included in lists. The 'ordered' flag indicates whether the member tuples are arranged in the same order as the underlying index, which allows certain operations (particularly in namespace.c) to optimize their processing.

CatCList objects are organized in their own hash table within each CatCache, separate from individual tuple entries. Like individual cache entries, they support reference counting and can be marked as 'dead' when invalidated but still have active references.

## Parameters / Member Variables
- `cl_magic`: Magic number (0x52765103) used to identify and validate CatCList structures
- `hash_value`: Hash value computed from the lookup keys for efficient hash table organization
- `cache_elem`: Double-linked list node for organizing lists within the cache's hash buckets
- `keys`: Array storing the partial key values used for this search, with only the first 'nkeys' elements being valid
- `refcount`: Reference count tracking active usage to prevent premature deletion
- `dead`: Flag indicating the list has been invalidated but cannot be removed due to active references
- `ordered`: Boolean flag indicating whether member tuples are arranged in index order
- `nkeys`: Number of key columns that were specified in the partial search (less than CATCACHE_MAXKEYS)
- `n_members`: Count of tuple entries stored in the members array
- `my_cache`: Back-pointer to the CatCache that owns this list
- `members`: Flexible array of pointers to CatCTup entries that match the partial key

## Dependencies
- Functions called/Symbols referenced:
  - [dlist_node](../d/dlist_node.md) (doubly-linked list infrastructure)
  - [CatCache](CatCache.md) (parent cache structure)
  - CatCTup (individual cache tuple entries)
  - Datum (PostgreSQL data value type)
  - FLEXIBLE_ARRAY_MEMBER (flexible array implementation)

- Called from (representative examples):
  - [SearchCatCacheList](../S/SearchCatCacheList.md) (primary search function for partial keys)
  - [ReleaseCatCacheList](../R/ReleaseCatCacheList.md) (reference management)
  - [FuncnameGetCandidates](../F/FuncnameGetCandidates.md) (function name resolution)
  - [OpernameGetCandidates](../O/OpernameGetCandidates.md) (operator name resolution)
  - [GetRelationPublications](../G/GetRelationPublications.md) (publication membership queries)
  - [roles_is_member_of](../r/roles_is_member_of.md) (role membership queries)

## Notes and Other Information
- [CatCList](CatCList.md) entries are only created for partial key searches (nkeys < cache's total keys)
- All member tuples in a list are guaranteed to be positive cache entries (never negative)
- The 'ordered' flag optimization is particularly important for namespace.c operations
- Lists can become 'dead' when any of their member entries are invalidated
- Reference counting prevents memory corruption during concurrent access
- Lists are stored in separate hash buckets from individual tuples within each cache
- The magic number provides runtime validation and debugging assistance
- Memory for the keys array elements is separately allocated for by-reference datatypes
- Lists enable efficient bulk operations on groups of related catalog entries