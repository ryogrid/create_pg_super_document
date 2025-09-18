# likesel

## Location
src/backend/utils/adt/like_support.c: 811 - 819

## Overview
A PostgreSQL selectivity estimation function that calculates the selectivity of LIKE pattern match operations for query optimization purposes.

## Definition
```c
Datum likesel(PG_FUNCTION_ARGS)
```

## Detailed Description
The `likesel` function is a SQL-callable selectivity estimation function that provides the PostgreSQL query planner with statistical estimates for LIKE pattern match operations. It serves as a wrapper around the general `patternsel` function, specifically configured for standard LIKE patterns that support `%` (wildcard for any sequence of characters) and `_` (wildcard for any single character). The function returns a selectivity estimate as a float8 value between 0.0 and 1.0, representing the expected fraction of rows that will match the given LIKE pattern.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - `root`: PlannerInfo pointer for query planning context
  - `operator`: OID of the LIKE operator being estimated
  - `args`: List of operator arguments (typically column and pattern)
  - `varRelid`: Relation ID for variable statistics lookup

## Dependencies
- Functions called/Symbols referenced:
  - `patternsel`: Core pattern selectivity estimation function
  - `Pattern_Type_Like`: Enum constant for standard LIKE pattern type
- Called from (representative examples):
  - No direct references found in the codebase (likely registered as operator selectivity function)

## Notes and Other Information
- This function is a crucial component of PostgreSQL's cost-based query optimizer
- Handles standard SQL LIKE patterns with `%` and `_` wildcards
- Used for estimating selectivity of queries like `WHERE column LIKE 'pattern%'`
- The selectivity estimates help the planner choose between different query execution strategies
- Delegates actual computation to `patternsel` with LIKE pattern type specification
- Located in `src/backend/utils/adt/like_support.c:811-819`