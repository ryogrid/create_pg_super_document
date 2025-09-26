# ReleaseCatCacheWithOwner

## Location
[src/backend/utils/cache/catcache.c:1630-1662](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/catcache.c#L1630-L1662)

## Overview
Core implementation for releasing catalog cache entries with explicit resource owner management, handling reference counting and potential cache entry removal.

## Definition

```c
static void
ReleaseCatCacheWithOwner(HeapTuple tuple, ResourceOwner resowner)
```
## Detailed Description
ReleaseCatCacheWithOwner performs the actual work of releasing catalog cache entries. It decrements the reference count of the specified cache entry and updates the resource owner tracking. If the reference count reaches zero and certain conditions are met (entry is dead or CATCACHE_FORCE_RELEASE is enabled), it removes the entry from the cache entirely.

The function includes safety checks to ensure the provided tuple is actually a valid cache entry by verifying the magic number and reference count. It also handles the complex logic around when cache entries should be physically removed, considering both the entry's reference count and any associated catalog list reference counts.

## Parameters / Member Variables
- : HeapTuple pointer representing the cached catalog tuple to release
- : ResourceOwner that was tracking this cache reference (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [ResourceOwnerForgetCatCacheRef](ResourceOwnerForgetCatCacheRef.md)
  - [CatCacheRemoveCTup](../C/CatCacheRemoveCTup.md)
  - CT_MAGIC (magic number constant)
  - offsetof (standard C macro)
- Called from (representative examples):
  - [ReleaseCatCache](ReleaseCatCache.md)
  - [ResOwnerReleaseCatCache](ResOwnerReleaseCatCache.md)

## Notes and Other Information
- Uses pointer arithmetic to convert HeapTuple back to CatCTup structure
- Includes safety assertions to verify tuple is a valid cache entry (CT_MAGIC check)
- Conditionally removes entries based on CATCACHE_FORCE_RELEASE compile-time flag
- Handles resource owner bookkeeping to track cache references per transaction/subtransaction
- Only removes cache entries when both the entry and any associated catalog list have zero references
- Supports NULL resource owner parameter for cases where resource tracking is not needed
- Part of PostgreSQL's memory management system for preventing cache entry leaks