# scalargtsel

## Location
src/backend/utils/adt/selfuncs.c: 1490 - 1498

## Overview
Selectivity estimator function for the greater-than (">") operator on scalar data types in PostgreSQL query optimization.

## Definition
```c
Datum scalargtsel(PG_FUNCTION_ARGS)
```

## Detailed Description
The `scalargtsel` function is PostgreSQL's selectivity estimator specifically for the greater-than (">") operator when applied to scalar data types. It serves as a thin wrapper around `scalarineqsel_wrapper`, passing boolean flags to indicate that this is a greater-than comparison (not less-than, and not equality-inclusive). This function is called by the PostgreSQL query planner to estimate how many rows will satisfy a greater-than predicate, which is critical for generating efficient execution plans.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function arguments macro that expands to:
  - `root`: PlannerInfo pointer containing planner context and statistics
  - `operator`: OID of the greater-than operator being used
  - `args`: List containing the operands of the comparison
  - `varRelid`: Relation ID if this is a restriction on a specific relation

## Dependencies
- Functions called/Symbols referenced:
  - `scalarineqsel_wrapper`: Core wrapper function that handles preprocessing and delegates to `scalarineqsel()`
- Called from:
  - This function is typically registered in PostgreSQL's system catalogs and called automatically by the query planner when estimating selectivity for greater-than predicates

## Notes and Other Information
- This function passes `isgt=true` and `iseq=false` to `scalarineqsel_wrapper`, indicating a strict greater-than comparison
- Part of PostgreSQL's cost-based optimizer infrastructure for statistical query planning
- The actual selectivity calculation is performed by the underlying `scalarineqsel()` function through the wrapper
- Used for various scalar data types including integers, floats, dates, timestamps, and other orderable types
- The function signature follows PostgreSQL's standard function calling convention using the `PG_FUNCTION_ARGS` macro
- Returns a `Datum` containing a float8 value representing the estimated selectivity (typically between 0.0 and 1.0)
- Complements `scalarltsel` by providing selectivity estimates for the opposite direction of comparison
- The selectivity estimation logic accounts for data distribution patterns and histogram information when available