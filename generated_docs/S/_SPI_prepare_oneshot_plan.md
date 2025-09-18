# _SPI_prepare_oneshot_plan

## Location
src/backend/executor/spi.c: 2329 - 2398

## Overview
_SPI_prepare_oneshot_plan is an internal SPI function that performs initial parsing of SQL queries without analysis, creating "one shot" cached plan sources optimized for single-use execution.

## Definition
```c
static void _SPI_prepare_oneshot_plan(const char *src, SPIPlanPtr plan)
```

## Detailed Description
The _SPI_prepare_oneshot_plan function is a stripped-down version of _SPI_prepare_plan that only performs the initial raw parsing phase without doing parse analysis or rule rewriting. It creates "one shot" CachedPlanSources that defer parse analysis until execution time.

This approach provides significant performance benefits by eliminating data copying and invalidation overhead. It also prevents issues when raw parse trees contain DDL commands that might affect the validity of later parse trees. The deferred analysis approach is particularly beneficial for SPI_execute() and similar one-time execution scenarios.

The function follows the same error handling and memory management patterns as _SPI_prepare_plan, but creates simpler cached plan structures that require additional processing at execution time.

## Parameters / Member Variables
- `src`: The SQL query string to be parsed
- `plan`: SPIPlanPtr structure that must have valid parse_mode set on entry

## Dependencies
- Functions called/Symbols referenced:
  - raw_parser
  - CreateOneShotCachedPlan
  - CreateCommandTag
  - _SPI_error_callback
  - lfirst_node
  - lappend
- Called from (representative examples):
  - SPI_execute
  - SPI_execute_extended
  - SPI_execute_with_args

## Notes and Other Information
- Sets plan->oneshot to true, indicating this is a single-use plan
- Creates CachedPlanSource entries without completing parse analysis
- Eliminates data copying and invalidation overhead compared to regular plan preparation
- Prevents DDL command interference by deferring analysis until execution
- Results stored in plan->plancache_list as list of incomplete CachedPlanSource entries
- All memory allocation occurs in CurrentMemoryContext (SPI executor context)
- Parse analysis will be performed later during _SPI_execute_plan execution