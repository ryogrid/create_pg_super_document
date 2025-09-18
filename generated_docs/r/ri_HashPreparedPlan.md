# ri_HashPreparedPlan

## Location
src/backend/utils/adt/ri_triggers.c: 2761 - 2794

## Overview
Adds a newly prepared SPI execution plan to the private hash table cache for future retrieval and reuse.

## Definition
```c
static void ri_HashPreparedPlan(RI_QueryKey *key, SPIPlanPtr plan)
```

## Detailed Description
This function stores a prepared SPI execution plan in the private hash table cache used by the referential integrity trigger system. It takes a query key and associates it with the provided plan for future lookups. The function handles hash table initialization on first use and can overwrite entries that were previously marked as invalid by ri_FetchPreparedPlan.

The caching mechanism improves performance by allowing reuse of expensive-to-prepare execution plans across multiple referential integrity operations. When an entry already exists with the same key, it assumes the previous plan was invalidated and safely overwrites it.

## Parameters / Member Variables
- `key`: A pointer to an RI_QueryKey structure that uniquely identifies the query plan being cached
- `plan`: A pointer to the SPI execution plan to be stored in the cache

## Dependencies
- Functions called/Symbols referenced:
  - ri_InitHashTables (initializes hash table on first call)
  - hash_search (inserts the key-plan pair into the cache)
- Called from (representative examples):
  - ri_PlanCheck (after preparing a new plan for constraint checking)

## Notes and Other Information
- Uses HASH_ENTER mode to insert or update entries in the hash table
- Includes assertion to verify that existing entries have NULL plans (indicating they were invalidated)
- Part of the plan caching optimization system for referential integrity triggers
- Works in conjunction with ri_FetchPreparedPlan to provide efficient plan reuse