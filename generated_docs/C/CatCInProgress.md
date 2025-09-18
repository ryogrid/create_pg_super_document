# CatCInProgress

## Location
src/backend/utils/cache/catcache.c: 52 - 59

## Overview
CatCInProgress is a struct used to track catalog cache entries that are currently being created to handle cache invalidation race conditions during entry construction.

## Definition


## Detailed Description
The CatCInProgress struct is designed to solve a critical race condition in PostgreSQL's catalog cache system. When a catalog cache entry (or list) is being created, there's a window of vulnerability where a cache invalidation event could apply to the entry being constructed, potentially making it invalid before it's even inserted into the cache.

To handle this scenario, PostgreSQL maintains a stack of "create-in-progress" entries using this structure. When cache invalidation occurs, it not only invalidates existing CatCTup and CatCList entries but also marks any matching entries in this in-progress stack as dead. This prevents invalid entries from being inserted into the cache after construction completes.

The structure forms a linked list (stack) where new entries are added to the front, allowing for efficient tracking of multiple concurrent cache entry creations.

## Parameters / Member Variables
- : Pointer to the CatCache that the entry being created belongs to
- : Hash value of the entry being created; this field is ignored for list entries
- : Boolean flag indicating whether this represents a list entry (CatCList) or a single entry (CatCTup)
- : Boolean flag set to true when cache invalidation determines this entry should be considered invalid
- : Pointer to the next CatCInProgress entry in the stack, forming a linked list

## Dependencies
- Functions called/Symbols referenced:
  - CatCache (referenced as member type)
  - [CatCInProgress](CatCInProgress.md) (self-reference for linked list structure)
- Called from (representative examples):
  - [CatCacheInvalidate](CatCacheInvalidate.md) (marks matching entries as dead)
  - [ResetCatalogCache](../R/ResetCatalogCache.md) (processes the stack during cache reset)
  - [SearchCatCacheList](../S/SearchCatCacheList.md) (manages stack during list creation)
  - [CatalogCacheCreateEntry](CatalogCacheCreateEntry.md) (manages stack during entry creation)

## Notes and Other Information
- This structure is part of PostgreSQL's catalog cache system located in src/backend/utils/cache/catcache.c
- The design prevents a subtle but serious race condition where cache invalidations could be missed during entry creation
- The stack-based approach allows for nested or concurrent cache entry creations
- The  flag provides a simple mechanism to mark entries as invalid without removing them from the stack immediately
- This is an internal implementation detail of the catalog cache and is not exposed to external modules