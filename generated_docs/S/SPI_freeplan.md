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
  - DropCachedPlan
  - MemoryContextDelete
  - SPI_ERROR_ARGUMENT
- Called from (representative examples):
  - ri_FetchPreparedPlan (referential integrity triggers)
  - tsquery_rewrite_query (text search query rewriting)
  - ts_stat_sql (text search statistics)
  - plperl_spi_query (PL/Perl SPI query execution)
  - plperl_spi_prepare (PL/Perl SPI preparation)
  - plperl_spi_freeplan (PL/Perl plan cleanup)
  - PLy_cursor_query (PL/Python cursor queries)
  - PLy_plan_dealloc (PL/Python plan deallocation)

## Notes and Other Information
- Returns 0 on success, SPI_ERROR_ARGUMENT if plan is NULL or has invalid magic number
- Must be called for all saved or kept plans to avoid memory leaks
- The plan pointer becomes invalid after calling this function
- Automatically handles cleanup of all cached plan sources in the plans cache list
- Memory context deletion ensures all subsidiary data structures are properly freed
- Commonly used in procedural language handlers and cleanup routines
- Should be called in error handling paths to prevent resource leaks
- The function is safe to call on plans that have already been executed multiple times