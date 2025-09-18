# SPI_freeplan

## Location
src/backend/executor/spi.c: 1025 - 1046

## Overview
Frees an SPI execution plan and all its associated resources, including cached plan sources and the plans memory context.

## Definition
```c
int SPI_freeplan(SPIPlanPtr plan)
```

## Detailed Description
SPI_freeplan completely deallocates an SPI execution plan and all its associated resources. The function first releases all cached plan sources associated with the plan by calling DropCachedPlan on each entry in the plans plancache_list. After releasing the cached plans, it deletes the entire memory context (plancxt) that contains the plan structure and all its subsidiary data. This ensures complete cleanup of all memory allocated for the plan. The function is essential for preventing memory leaks when working with saved or kept plans that persist beyond their original context.

## Parameters / Member Variables
- `plan`: Pointer to the SPI execution plan to be freed

## Dependencies
- Functions called/Symbols referenced:
  - _SPI_PLAN_MAGIC
  - CachedPlanSource
  - [DropCachedPlan](../D/DropCachedPlan.md)
  - [MemoryContextDelete](../M/MemoryContextDelete.md)
  - SPI_ERROR_ARGUMENT
- Called from (representative examples):
  - [ri_FetchPreparedPlan](../r/ri_FetchPreparedPlan.md) (referential integrity triggers)
  - [tsquery_rewrite_query](../t/tsquery_rewrite_query.md) (text search query rewriting)
  - [ts_stat_sql](../t/ts_stat_sql.md) (text search statistics)
  - [plperl_spi_query](../p/plperl_spi_query.md) (PL/Perl SPI query execution)
  - [plperl_spi_prepare](../p/plperl_spi_prepare.md) (PL/Perl SPI preparation)
  - [plperl_spi_freeplan](../p/plperl_spi_freeplan.md) (PL/Perl plan cleanup)
  - [PLy_cursor_query](../P/PLy_cursor_query.md) (PL/Python cursor queries)
  - [PLy_plan_dealloc](../P/PLy_plan_dealloc.md) (PL/Python plan deallocation)

## Notes and Other Information
- Returns 0 on success, SPI_ERROR_ARGUMENT if plan is NULL or has invalid magic number
- Must be called for all saved or kept plans to avoid memory leaks
- The plan pointer becomes invalid after calling this function
- Automatically handles cleanup of all cached plan sources in the plans cache list
- Memory context deletion ensures all subsidiary data structures are properly freed
- Commonly used in procedural language handlers and cleanup routines
- Should be called in error handling paths to prevent resource leaks
- The function is safe to call on plans that have already been executed multiple times