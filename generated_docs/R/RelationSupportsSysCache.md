# RelationSupportsSysCache

## Location
[src/backend/utils/cache/syscache.c:771-795](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/syscache.c#L771-L795)

## Overview
Tests whether a relation supports a system cache, meaning it is either a cached table or an index used for cache lookups.

## Definition

```c
bool
RelationSupportsSysCache(Oid relid)
```
## Detailed Description
RelationSupportsSysCache determines if a relation participates in the system catalog cache infrastructure, either as a cached table or as an index that supports cache operations. Unlike RelationHasSysCache which only checks for relations that are directly cached, this function also includes indexes that are used by the caching system. The function performs a binary search on the SysCacheSupportingRelOid array to efficiently locate relations that are part of the cache infrastructure.

## Parameters / Member Variables
- `relid`: Object identifier (Oid) of the relation to check for system cache support

## Dependencies
- Functions called/Symbols referenced:
  - Uses SysCacheSupportingRelOid array and SysCacheSupportingRelOidSize for binary search
- Called from (representative examples):
  - [heap_update](../h/heap_update.md)
  - [RelationIdIsInInitFile](RelationIdIsInInitFile.md)
  - Referenced in syscache.h header

## Notes and Other Information
- Implements O(log n) binary search algorithm for efficient lookup
- Broader scope than RelationHasSysCache - includes both cached tables and their supporting indexes
- Used by heap operations and relation cache management for determining cache-related behavior
- Relies on SysCacheSupportingRelOid being pre-sorted for correct binary search operation
- Important for determining which relations need special handling during updates and cache invalidation
- Located in src/backend/utils/cache/syscache.c:771-795

## Simplified Source

```c
bool
RelationSupportsSysCache(Oid relid)
{
    // Binary search through sorted array of cache-supporting relation OIDs
    int low = 0;
    int high = SysCacheSupportingRelOidSize - 1;

    while (low <= high) {
        int middle = low + (high - low) / 2;

        if (SysCacheSupportingRelOid[middle] == relid)
            return true;

        // Adjust search range based on comparison
        if (SysCacheSupportingRelOid[middle] < relid)
            low = middle + 1;
        else
            high = middle - 1;
    }

    return false;
}
```