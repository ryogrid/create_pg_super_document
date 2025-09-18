# ri_InitHashTables

## Location
src/backend/utils/adt/ri_triggers.c: 2673 - 2708

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
  - hash_create
  - CacheRegisterSyscacheCallback
  - InvalidateConstraintCacheCallBack
  - RI_ConstraintInfo (structure type)
  - RI_QueryKey (structure type)
  - RI_QueryHashEntry (structure type)
  - RI_CompareKey (structure type)
  - RI_CompareHashEntry (structure type)
  - RI_INIT_CONSTRAINTHASHSIZE (constant)
  - RI_INIT_QUERYHASHSIZE (constant)
  - HASH_ELEM (flag)
  - HASH_BLOBS (flag)
- Called from (representative examples):
  - ri_LoadConstraintInfo
  - ri_FetchPreparedPlan
  - ri_HashPreparedPlan
  - ri_HashCompareOp

## Notes and Other Information
- Creates three separate hash tables: ri_constraint_cache, ri_query_cache, and ri_compare_cache
- Uses HASH_ELEM and HASH_BLOBS flags for efficient hash table operations
- Registers syscache callback for CONSTROID to maintain cache consistency
- Called lazily when hash tables are first needed rather than at system startup
- Cache sizes are controlled by RI_INIT_CONSTRAINTHASHSIZE and RI_INIT_QUERYHASHSIZE constants
- Essential for referential integrity performance optimization by reducing catalog access overhead