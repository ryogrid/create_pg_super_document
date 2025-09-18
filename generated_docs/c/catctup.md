# catctup

## Location
src/include/utils/catcache.h: 88 - 135

## Overview
The catctup struct represents an individual cached tuple in PostgreSQL's catalog cache system, containing both the tuple data and metadata needed for cache management.

## Definition


## Detailed Description
The catctup struct represents a single cached catalog tuple within PostgreSQL's catalog cache system. It serves as a wrapper around HeapTupleData with additional cache management metadata. The structure supports both positive entries (containing actual tuple data) and negative entries (indicating that no tuple exists for the given keys). Each cached tuple is organized into hash buckets for efficient lookup and maintains LRU ordering within buckets. The structure includes reference counting for safe memory management and supports membership in CatCList objects for partial key searches.

## Parameters / Member Variables
- : Magic number (0x57261502) used to identify valid CatCTup entries for debugging
- : Precomputed hash value of the tuple's key values for efficient bucket placement
- : Array of key values used for cache lookups, supporting up to CATCACHE_MAXKEYS
- : Doubly-linked list node for organizing tuples within hash buckets in LRU order
- : Reference count tracking active usage to prevent premature deletion
- : Flag indicating the tuple is logically deleted but still referenced
- : Flag indicating this is a negative cache entry (no matching tuple exists)
- : HeapTupleData structure containing the actual tuple data and metadata
- : Pointer to containing CatCList if this tuple is part of a list search result
- : Back-reference to the owning CatCache structure

## Dependencies
- Functions called/Symbols referenced:
  - CATCACHE_MAXKEYS (maximum number of cache keys)
  - [dlist_node](../d/dlist_node.md) (doubly-linked list node structure)
  - [HeapTupleData](../H/HeapTupleData.md) (heap tuple data structure)
  - [catclist](catclist.md) (catalog cache list structure)
  - CatCache (catalog cache structure)
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