# catcacheheader

## Location
[src/include/utils/catcache.h:184-187](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/catcache.h#L184-L187)

## Overview
The catcacheheader struct serves as the global header structure that manages all catalog caches in a PostgreSQL instance, maintaining a list of all active CatCache structures and global statistics.

## Definition

```c
typedef struct catcacheheader
{
	slist_head	ch_caches;		/* head of list of CatCache structs */
	int			ch_ntup;		/* # of tuples in all caches */
} CatCacheHeader;
```
## Detailed Description
The catcacheheader struct acts as the central management structure for PostgreSQL's entire catalog cache system. It maintains a singly-linked list of all active catalog caches and tracks global statistics such as the total number of cached tuples across all caches. This structure provides a single point of control for operations that need to affect all catalog caches, such as cache invalidation, statistics reporting, and system-wide cache management.

## Parameters / Member Variables
- `ch_caches`: Head of a singly-linked list containing all active CatCache structures in the system
- `ch_ntup`: Total number of tuples currently cached across all catalog caches
## Dependencies
- Functions called/Symbols referenced:
  - [slist_head](../s/slist_head.md) (singly-linked list head structure)
- Called from (representative examples):
  - [CreateCacheMemoryContext](../C/CreateCacheMemoryContext.md) (cache system initialization)
  - [ResetCatalogCaches](../R/ResetCatalogCaches.md) (system-wide cache reset operations)

## Notes and Other Information
- This is a singleton structure - there is typically only one CatCacheHeader instance per PostgreSQL backend
- The structure provides a global view of the catalog cache system for monitoring and management
- The total tuple count (ch_ntup) is useful for memory usage tracking and debugging
- All catalog caches are linked through this structure via their cc_next slist_node members
- The structure enables efficient iteration over all caches for operations like invalidation messages
- The singly-linked list design is sufficient since cache traversal is typically infrequent and performance-critical operations work on individual caches