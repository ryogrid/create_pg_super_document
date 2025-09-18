# SPI_plan_get_cached_plan

## Location
[src/backend/executor/spi.c:2076-2122](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/spi.c#L2076-L2122)

## Overview
Retrieves the generic CachedPlan from a SPI plan that contains exactly one CachedPlanSource, with proper reference counting and error handling.

## Definition
```c
CachedPlan *SPI_plan_get_cached_plan(SPIPlanPtr plan)
```

## Detailed Description
SPI_plan_get_cached_plan extracts the generic cached plan from a SPI plan, but only if the plan contains exactly one CachedPlanSource. This function is designed for cases where a single statement plan needs to be accessed directly, particularly by PL/pgSQL for optimization purposes. The function handles proper reference counting by incrementing the plan's refcount and registering it with the CurrentResourceOwner if it's a saved plan.

The function includes comprehensive error handling with a custom error context callback that provides meaningful error messages. It rejects one-shot plans and multi-statement plans, returning NULL in these cases. For valid single-statement plans, it calls GetCachedPlan to retrieve the generic plan, ensuring proper resource management and error tracking.

## Parameters / Member Variables
- `plan`: A pointer to the SPI plan (SPIPlanPtr) from which to extract the cached plan. Must be a valid, non-one-shot plan containing exactly one CachedPlanSource.

## Dependencies
- Functions called/Symbols referenced:
  - [GetCachedPlan](../G/GetCachedPlan.md)
  - [_SPI_error_callback](_SPI_error_callback.md)
  - _SPI_PLAN_MAGIC (for integrity verification)
  - [SPICallbackArg](SPICallbackArg.md) (for error context)
  - CachedPlanSource
  - CachedPlan
  - list_length, linitial (list operations)
- Called from (representative examples):
  - [test_predtest](../t/test_predtest.md) (testing module)
  - Primarily used by PL/pgSQL internally

## Notes and Other Information
- Returns NULL for one-shot plans or plans with multiple CachedPlanSources
- Increments the plan's reference count - caller must call ReleaseCachedPlan
- Registers with CurrentResourceOwner for saved plans to ensure proper cleanup
- Sets up error context for meaningful error reporting during plan retrieval
- Not documented in main SPI documentation to limit its usage to internal purposes
- Exported specifically for PL/pgSQL optimization needs
- The returned plan is the generic plan (plansource->gplan), not a custom plan