# _SPI_prepare_plan

## Location
src/backend/executor/spi.c: 2221 - 2328

## Overview
_SPI_prepare_plan is an internal SPI function that parses and analyzes a SQL query string, creating cached plan sources for later execution.

## Definition
```c
static void _SPI_prepare_plan(const char *src, SPIPlanPtr plan)
```

## Detailed Description
The _SPI_prepare_plan function is responsible for the complete preparation pipeline of SQL statements within the SPI framework. It takes a raw SQL query string and transforms it into executable cached plans through multiple phases: parsing, analysis, and rule rewriting.

The function establishes error context tracking for better error reporting, then uses the PostgreSQL parser to convert the SQL string into raw parse trees. For each parse tree, it creates a CachedPlanSource and performs parse analysis and rule rewriting. The function supports both fixed parameter lists and dynamic parameter setup through parser callback hooks.

All resulting data structures are stored in the current memory context, typically the SPI executor context, creating what is considered a "temporary" SPIPlan that will be cleaned up when the SPI context ends.

## Parameters / Member Variables
- `src`: The SQL query string to be parsed and prepared
- `plan`: SPIPlanPtr structure that must have valid argtypes, nargs, parse_mode, and cursor_options set on entry

## Dependencies
- Functions called/Symbols referenced:
  - raw_parser
  - CreateCachedPlan
  - CreateCommandTag
  - pg_analyze_and_rewrite_withcb
  - pg_analyze_and_rewrite_fixedparams
  - CompleteCachedPlan
  - _SPI_error_callback
- Called from (representative examples):
  - SPI_prepare_cursor
  - SPI_prepare_extended
  - SPI_prepare_params
  - SPI_cursor_open_with_args
  - SPI_cursor_parse_open

## Notes and Other Information
- Results are stored in plan->plancache_list as a list of CachedPlanSource entries
- Sets plan->oneshot to false, indicating this is a reusable plan
- Supports both fixed parameter mode and dynamic parameter callback mode
- Establishes error context stack for enhanced error reporting during parsing
- All memory allocation occurs in CurrentMemoryContext (SPI executor context)
- Creates unsaved plancache entries that can be reused across multiple executions