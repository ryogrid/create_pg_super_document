# ri_InitHashTables

## Location
[src/backend/utils/adt/ri_triggers.c:2673-2708](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ri_triggers.c#L2673-L2708)

## Overview
Initializes the internal hash tables used for caching referential integrity constraint information, prepared query plans, and comparison operators.

## Definition


## Detailed Description
This function sets up three critical hash tables that serve as caches for the referential integrity system. The first table caches constraint information to avoid repeatedly looking up constraint details from the system catalogs. The second table caches prepared SPI query plans for executing referential integrity checks efficiently. The third table caches comparison operator information for key matching operations. The function also registers a callback to invalidate the constraint cache when pg_constraint catalog changes occur.

These hash tables provide significant performance improvements by avoiding repeated catalog lookups and query planning overhead during referential integrity operations.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - [hash_create](../h/hash_create.md)
  - [CacheRegisterSyscacheCallback](../C/CacheRegisterSyscacheCallback.md)
  - [InvalidateConstraintCacheCallBack](../I/InvalidateConstraintCacheCallBack.md)
  - [RI_ConstraintInfo](../R/RI_ConstraintInfo.md) (structure type)
  - [RI_QueryKey](../R/RI_QueryKey.md) (structure type)
  - [RI_QueryHashEntry](../R/RI_QueryHashEntry.md) (structure type)
  - [RI_CompareKey](../R/RI_CompareKey.md) (structure type)
  - [RI_CompareHashEntry](../R/RI_CompareHashEntry.md) (structure type)
  - RI_INIT_CONSTRAINTHASHSIZE (constant)
  - RI_INIT_QUERYHASHSIZE (constant)
  - HASH_ELEM (flag)
  - HASH_BLOBS (flag)
- Called from (representative examples):
  - [ri_LoadConstraintInfo](ri_LoadConstraintInfo.md)
  - [ri_FetchPreparedPlan](ri_FetchPreparedPlan.md)
  - [ri_HashPreparedPlan](ri_HashPreparedPlan.md)
  - [ri_HashCompareOp](ri_HashCompareOp.md)

## Notes and Other Information
- Creates three separate hash tables: ri_constraint_cache, ri_query_cache, and ri_compare_cache
- Uses HASH_ELEM and HASH_BLOBS flags for efficient hash table operations
- Registers syscache callback for CONSTROID to maintain cache consistency
- Called lazily when hash tables are first needed rather than at system startup
- Cache sizes are controlled by RI_INIT_CONSTRAINTHASHSIZE and RI_INIT_QUERYHASHSIZE constants
- Essential for referential integrity performance optimization by reducing catalog access overhead