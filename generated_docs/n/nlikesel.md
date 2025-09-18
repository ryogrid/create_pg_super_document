# nlikesel

## Location
src/backend/utils/adt/like_support.c: 857 - 865

## Overview
A selectivity estimation function for LIKE pattern non-match operations in PostgreSQL's query planner.

## Definition
```c
Datum nlikesel(PG_FUNCTION_ARGS)
```

## Detailed Description
The `nlikesel` function estimates the selectivity (fraction of rows that will match) for LIKE pattern non-match operations (e.g., `NOT LIKE` operator). It serves as a wrapper function that calls the generic `patternsel` function with specific parameters for LIKE patterns and negation. This function is used by PostgreSQL's query planner to estimate how many rows will NOT match a LIKE pattern, which helps in choosing optimal query execution plans.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro that includes:
  - `root`: PlannerInfo pointer containing planner context
  - `operator`: OID of the operator being evaluated
  - `args`: List of operator arguments
  - `varRelid`: Variable relation ID for statistics lookup
  - `collation`: Collation information

## Dependencies
- Functions called/Symbols referenced:
  - `[patternsel](../p/patternsel.md)` - Generic pattern selectivity estimation function
  - `Pattern_Type_Like` - Enum value for LIKE pattern type
- Called from (representative examples):
  - No direct references found (likely called via function pointer from operator catalog)

## Notes and Other Information
- Returns a float8 value representing the estimated selectivity (0.0 to 1.0)
- The `true` parameter passed to `patternsel` indicates this is for a negated match (NOT operation)
- Part of PostgreSQL's statistical estimation system for query optimization
- Located in `src/backend/utils/adt/like_support.c:857-865`