# regexeqjoinsel

## Location
[src/backend/utils/adt/like_support.c:885-893](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/like_support.c#L885-L893)

## Overview
A selectivity estimation function for regular expression pattern match operations in join contexts within PostgreSQL's query planner.

## Definition
```c
Datum regexeqjoinsel(PG_FUNCTION_ARGS)
```

## Detailed Description
The `regexeqjoinsel` function estimates the selectivity for regular expression pattern matches when used in join operations (e.g., `~` operator in join conditions). It serves as a wrapper function that calls the generic `patternjoinsel` function with specific parameters for case-sensitive regular expression patterns. This function is used by PostgreSQL's query planner to estimate how many rows will match between joined tables when a regular expression pattern is used as the join condition.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro that includes:
  - `root`: PlannerInfo pointer containing planner context
  - `operator`: OID of the operator being evaluated
  - `args`: List of operator arguments
  - `varRelid`: Variable relation ID for statistics lookup
  - `collation`: Collation information

## Dependencies
- Functions called/Symbols referenced:
  - [patternjoinsel](../p/patternjoinsel.md) - Generic pattern join selectivity estimation function
  - `Pattern_Type_Regex` - Enum value for case-sensitive regex pattern type
- Called from (representative examples):
  - No direct references found (likely called via function pointer from operator catalog)

## Notes and Other Information
- Returns a float8 value representing the estimated join selectivity (0.0 to 1.0)
- The `false` parameter passed to `patternjoinsel` indicates this is for a non-negated match operation
- Currently returns the default selectivity value (0.005) as `patternjoinsel` uses a simple heuristic
- Part of PostgreSQL's join selectivity estimation system for query optimization
- Located in `src/backend/utils/adt/like_support.c:885-893`