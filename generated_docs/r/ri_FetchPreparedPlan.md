# ri_FetchPreparedPlan

## Location
[src/backend/utils/adt/ri_triggers.c:2709-2760](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ri_triggers.c#L2709-L2760)

## Overview
Retrieves a cached SPI execution plan from the private hash table using a query key, validating the plan before returning it.

## Definition


## Detailed Description
This function implements a caching mechanism for SPI (Server Programming Interface) execution plans used in referential integrity triggers. It searches a private hash table for a previously prepared and cached plan using the provided query key. The function ensures plan validity before returning it, handling cases where the underlying database objects (tables, columns) may have been renamed or modified since the plan was cached.

The function initializes the hash table on first use and includes logic to detect and handle invalid plans by removing them from the cache and freeing associated memory. This design helps optimize referential integrity operations by avoiding repeated plan preparation for the same queries while maintaining correctness when schema changes occur.

## Parameters / Member Variables
- : A pointer to an RI_QueryKey structure that uniquely identifies the cached query plan to retrieve

## Dependencies
- Functions called/Symbols referenced:
  - [ri_InitHashTables](ri_InitHashTables.md) (initializes hash table on first call)
  - [hash_search](../h/hash_search.md) (searches for the query key in the cache)
  - [SPI_plan_is_valid](../S/SPI_plan_is_valid.md) (validates the cached plan)
  - [SPI_freeplan](../S/SPI_freeplan.md) (frees invalid plans)
- Called from (representative examples):
  - [ri_Check_Pk_Match](ri_Check_Pk_Match.md) (primary key matching validation)
  - [ri_restrict](ri_restrict.md) (restrict action implementation)
  - [RI_FKey_cascade_del](../R/RI_FKey_cascade_del.md) (cascade delete operations)
  - [RI_FKey_cascade_upd](../R/RI_FKey_cascade_upd.md) (cascade update operations)  
  - [ri_set](ri_set.md) (set null/default operations)

## Notes and Other Information
- Returns NULL if no cached plan is found or if the cached plan is invalid
- Requires that both foreign key and primary key relations are locked before calling for trustworthy validity checks
- Automatically cleans up invalid plans to free memory before plan regeneration
- Part of PostgreSQL's referential integrity trigger system that optimizes repeated constraint checks