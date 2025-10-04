# SPI_keepplan

## Location
[src/backend/executor/spi.c:976-1002](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/spi.c#L976-L1002)

## Overview
Marks an SPI execution plan as saved to prevent it from being automatically freed at the end of the current SPI procedure, allowing it to persist across multiple SPI procedure calls.

## Definition
```c
int SPI_keepplan(SPIPlanPtr plan)
```

## Detailed Description
SPI_keepplan extends the lifetime of an SPI execution plan beyond the current procedure context. By default, SPI plans are automatically freed when SPI_finish is called or when the procedure context ends. This function moves the plan to CacheMemoryContext and marks it as saved, ensuring it persists until explicitly freed with SPI_freeplan. The function also marks all component CachedPlanSource objects as saved using SaveCachedPlan, which prevents them from being invalidated or freed unexpectedly. This is essential for plans that need to be reused across multiple procedure invocations or when building long-lived cached execution plans.

## Parameters / Member Variables
- `plan`: Pointer to the SPI execution plan to be kept

## Dependencies
- Functions called/Symbols referenced:
  - _SPI_PLAN_MAGIC
  - [MemoryContextSetParent](../M/MemoryContextSetParent.md)
  - [CachedPlanSource](../C/CachedPlanSource.md)
  - [SaveCachedPlan](SaveCachedPlan.md)
  - SPI_ERROR_ARGUMENT
- Called from (representative examples):
  - [ri_PlanCheck](../r/ri_PlanCheck.md) (referential integrity triggers)
  - [pg_get_ruledef_worker](../p/pg_get_ruledef_worker.md) (rule definition utilities)
  - [pg_get_viewdef_worker](../p/pg_get_viewdef_worker.md) (view definition utilities)
  - [plperl_spi_prepare](../p/plperl_spi_prepare.md) (PL/Perl language handler)
  - [PLy_spi_prepare](../P/PLy_spi_prepare.md) (PL/Python language handler)
  - [pltcl_SPI_prepare](../p/pltcl_SPI_prepare.md) (PL/Tcl language handler)
  - [ttdummy](../t/ttdummy.md) (test regression function)

## Notes and Other Information
- Returns 0 on success, SPI_ERROR_ARGUMENT on failure
- The plan must not be NULL, must have valid magic number, and must not already be saved or marked as oneshot
- Once saved, the plan persists in CacheMemoryContext until explicitly freed
- The operation is atomic - either all components are successfully saved or none are
- Commonly used by procedural language handlers and system functions that need persistent plans
- The saved plan can be reused across different database connections and transactions
- Memory reparenting ensures the plan survives context switches and procedure exits

## Simplified Source

```c
int SPI_keepplan(SPIPlanPtr plan) {
    ListCell *lc;

    // Validate plan parameters
    if (plan == NULL || plan->magic != _SPI_PLAN_MAGIC ||
        plan->saved || plan->oneshot)
        return SPI_ERROR_ARGUMENT;

    // Mark plan as saved and move to cache memory context
    // This sequence is atomic and cannot fail partway through
    plan->saved = true;
    MemoryContextSetParent(plan->plancxt, CacheMemoryContext);

    // Save all cached plan sources in the plan
    foreach(lc, plan->plancache_list) {
        CachedPlanSource *plansource = (CachedPlanSource *) lfirst(lc);
        SaveCachedPlan(plansource);
    }

    return 0;  // Success
}
```