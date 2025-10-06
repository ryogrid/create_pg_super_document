# scalarlesel

## Location
[src/backend/utils/adt/selfuncs.c:1481-1489](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/selfuncs.c#L1481-L1489)

## Overview
Selectivity estimator function for the less-than-or-equal ("<=") operator on scalar data types in PostgreSQL query optimization.

## Definition
```c
Datum scalarlesel(PG_FUNCTION_ARGS)
```

## Detailed Description
The `scalarlesel` function is PostgreSQL's selectivity estimator specifically for the less-than-or-equal ("<=") operator when applied to scalar data types. It serves as a thin wrapper around `scalarineqsel_wrapper`, passing boolean flags to indicate that this is a less-than comparison that includes equality (not greater-than, but equality-inclusive). This function is called by the PostgreSQL query planner to estimate how many rows will satisfy a less-than-or-equal predicate, which is essential for generating optimal execution plans.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function arguments macro that expands to:
  - `root`: PlannerInfo pointer containing planner context and statistics
  - `operator`: OID of the less-than-or-equal operator being used
  - `args`: List containing the operands of the comparison
  - `varRelid`: Relation ID if this is a restriction on a specific relation

## Dependencies
- Functions called/Symbols referenced:
  - [scalarineqsel_wrapper](scalarineqsel_wrapper.md): Core wrapper function that handles preprocessing and delegates to `scalarineqsel()`
- Called from:
  - This function is typically registered in PostgreSQL's system catalogs and called automatically by the query planner when estimating selectivity for less-than-or-equal predicates

## Notes and Other Information
- This function passes `isgt=false` and `iseq=true` to `scalarineqsel_wrapper`, indicating a less-than comparison that includes equality
- Part of PostgreSQL's cost-based optimizer infrastructure for statistical query planning
- The actual selectivity calculation is performed by the underlying `scalarineqsel()` function through the wrapper
- Used for various scalar data types including integers, floats, dates, timestamps, and other orderable types
- The selectivity estimate typically includes both the less-than portion and the equality portion of the comparison
- The function signature follows PostgreSQL's standard function calling convention using the `PG_FUNCTION_ARGS` macro
- Returns a `Datum` containing a float8 value representing the estimated selectivity (typically between 0.0 and 1.0)
- Generally produces slightly higher selectivity estimates than `scalarltsel` due to the inclusion of equality matches

## Simplified Source
```c
Datum
scalarlesel(PG_FUNCTION_ARGS)
{
    // Delegate to wrapper function for less-than-or-equal comparison
    // isgt=false (not greater-than), iseq=true (equality-inclusive)
    return scalarineqsel_wrapper(fcinfo, false, true);
}
```