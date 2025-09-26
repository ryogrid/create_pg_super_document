# CachedPlanIsValid

## Location
[src/backend/utils/cache/plancache.c:1627-1639](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/plancache.c#L1627-L1639)

## Overview
Tests whether the rewritten querytree within a CachedPlanSource is currently valid and not marked as being in need of revalidation.

## Definition

```c
bool
CachedPlanIsValid(CachedPlanSource *plansource)
```
## Detailed Description
CachedPlanIsValid is a simple validity check function that returns the current validation status of a cached plan source. The function checks the  flag of the provided CachedPlanSource structure to determine if the cached plan is still usable. This is a lightweight operation that simply returns the cached validity state without performing any expensive revalidation checks.

The result is only trustworthy and free from race conditions if the caller has acquired locks on all the relations used in the plan. This is crucial for ensuring that the validity check reflects the current state of the database objects referenced by the plan.

## Parameters / Member Variables
- : Pointer to the CachedPlanSource structure whose validity is being checked

## Dependencies
- Functions called/Symbols referenced:
  - [CachedPlanSource](CachedPlanSource.md) (structure type)
  - CACHEDPLANSOURCE_MAGIC (magic number for validation)
- Called from (representative examples):
  - [SPI_plan_is_valid](../S/SPI_plan_is_valid.md)

## Notes and Other Information
- The function includes an assertion to verify the magic number of the CachedPlanSource structure, ensuring structural integrity
- This is a read-only operation that does not modify the plan source
- The validity flag is typically set/unset by other plan cache management functions
- Race conditions can occur if proper locking is not maintained by the caller
- Located in src/backend/utils/cache/plancache.c:1627-1639