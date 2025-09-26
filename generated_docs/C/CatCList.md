# CatCList

## Location
src/include/utils/catcache.h: 181 - 183

## Overview
CatCList represents the result of a partial catalog search in PostgreSQL, storing cache entries for all table rows that match a partial key combination when searching with fewer keys than the cache's maximum.

## Definition


## Detailed Description
CatCList is a specialized cache structure that stores the results of partial key searches on catalog caches. When a search is performed using only the first K columns of an N-key cache (where K < N), PostgreSQL creates a CatCList to hold all tuples that match those K keys. This optimization allows subsequent searches with the same partial key to avoid scanning the underlying system catalog.

The structure maintains an array of pointers to CatCTup entries that match the partial key combination. These member tuples are never negative cache entries - only actual catalog tuples are included in lists. The 'ordered' flag indicates whether the member tuples are arranged in the same order as the underlying index, which allows certain operations (particularly in namespace.c) to optimize their processing.

CatCList objects are organized in their own hash table within each CatCache, separate from individual tuple entries. Like individual cache entries, they support reference counting and can be marked as 'dead' when invalidated but still have active references.

## Parameters / Member Variables
- : Magic number (0x52765103) used to identify and validate CatCList structures
- : Hash value computed from the lookup keys for efficient hash table organization
- : Double-linked list node for organizing lists within the cache's hash buckets
- : Array storing the partial key values used for this search, with only the first 'nkeys' elements being valid
- : Reference count tracking active usage to prevent premature deletion
- : Flag indicating the list has been invalidated but cannot be removed due to active references
- : Boolean flag indicating whether member tuples are arranged in index order
- : Number of key columns that were specified in the partial search (less than CATCACHE_MAXKEYS)
- : Count of tuple entries stored in the members array
- : Back-pointer to the CatCache that owns this list
- : Flexible array of pointers to CatCTup entries that match the partial key

## Dependencies
- Functions called/Symbols referenced:
  - dlist_node (doubly-linked list infrastructure)
  - CatCache (parent cache structure)
  - CatCTup (individual cache tuple entries)
  - Datum (PostgreSQL data value type)
  - FLEXIBLE_ARRAY_MEMBER (flexible array implementation)

- Called from (representative examples):
  - SearchCatCacheList (primary search function for partial keys)
  - ReleaseCatCacheList (reference management)
  - FuncnameGetCandidates (function name resolution)
  - OpernameGetCandidates (operator name resolution)
  - GetRelationPublications (publication membership queries)
  - roles_is_member_of (role membership queries)

## Notes and Other Information
- CatCList entries are only created for partial key searches (nkeys < cache's total keys)
- All member tuples in a list are guaranteed to be positive cache entries (never negative)
- The 'ordered' flag optimization is particularly important for namespace.c operations
- Lists can become 'dead' when any of their member entries are invalidated
- Reference counting prevents memory corruption during concurrent access
- Lists are stored in separate hash buckets from individual tuples within each cache
- The magic number provides runtime validation and debugging assistance
- Memory for the keys array elements is separately allocated for by-reference datatypes
- Lists enable efficient bulk operations on groups of related catalog entries