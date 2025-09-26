# CatCacheHeader

## Location
src/include/utils/catcache.h: 188 - 231

## Overview
CatCacheHeader is the global management structure that coordinates all catalog caches in PostgreSQL, maintaining a linked list of all CatCache instances and tracking overall cache statistics.

## Definition

```c
structs */
	int			ch_ntup;
```
## Detailed Description
CatCacheHeader serves as the master control structure for PostgreSQL's entire catalog caching system. It acts as a central registry that maintains references to all individual catalog caches (CatCache instances) in the system through a singly-linked list. This global structure enables system-wide operations such as cache invalidation, statistics reporting, and memory management across all catalog caches.

The header structure is minimal by design, containing only essential information needed for global cache coordination. The primary purpose is to provide a single entry point for operations that must affect all catalog caches simultaneously, such as during transaction rollback, cache reset operations, or system-wide invalidation events.

The ch_ntup field provides a quick way to monitor the total memory usage and cache utilization across the entire catalog caching system without requiring traversal of all individual caches.

## Parameters / Member Variables
- : Head of the singly-linked list that connects all CatCache instances in the system
- : Total count of cached tuples across all individual catalog caches, used for monitoring and statistics

## Dependencies
- Functions called/Symbols referenced:
  - slist_head (singly-linked list infrastructure from lib/ilist.h)
  - CatCache (individual cache instances linked through this header)

- Called from (representative examples):
  - InitCatCache (cache initialization adds new caches to the global list)
  - ResetCatalogCaches (system-wide cache reset operations)
  - ResetCatalogCachesExt (extended cache reset with debugging)
  - CatalogCacheFlushCatalog (cache invalidation operations)

## Notes and Other Information
- There is typically only one global CatCacheHeader instance per PostgreSQL backend process
- The structure provides the foundation for system-wide cache management operations
- The singly-linked list design is sufficient since cache traversal is primarily unidirectional
- Global tuple count (ch_ntup) enables efficient monitoring of total cache memory usage
- The header structure is designed to be lightweight to minimize overhead for global operations
- New catalog caches are added to the list during system initialization via InitCatCache
- The structure supports the catalog cache's role in PostgreSQL's MVCC (Multi-Version Concurrency Control) system
- Cache invalidation messages can use this structure to efficiently broadcast changes across all caches