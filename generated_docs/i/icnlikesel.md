# icnlikesel

## Location
[src/backend/utils/adt/like_support.c:866-874](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/like_support.c#L866-L874)

## Overview
A selectivity estimation function for case-insensitive LIKE pattern non-match operations (ILIKE) in PostgreSQL's query planner.

## Definition
```c
Datum icnlikesel(PG_FUNCTION_ARGS)
```

## Detailed Description
The `icnlikesel` function estimates the selectivity (fraction of rows that will match) for case-insensitive LIKE pattern non-match operations (e.g., `NOT ILIKE` operator). It serves as a wrapper function that calls the generic `patternsel` function with specific parameters for case-insensitive LIKE patterns and negation. This function is used by PostgreSQL's query planner to estimate how many rows will NOT match an ILIKE pattern, which helps in choosing optimal query execution plans.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro that includes:
  - `root`: PlannerInfo pointer containing planner context
  - `operator`: OID of the operator being evaluated
  - `args`: List of operator arguments
  - `varRelid`: Variable relation ID for statistics lookup
  - `collation`: Collation information

## Dependencies
- Functions called/Symbols referenced:
  - [patternsel](../p/patternsel.md) - Generic pattern selectivity estimation function
  - `Pattern_Type_Like_IC` - Enum value for case-insensitive LIKE pattern type
- Called from (representative examples):
  - No direct references found (likely called via function pointer from operator catalog)

## Notes and Other Information
- Returns a float8 value representing the estimated selectivity (0.0 to 1.0)
- The `true` parameter passed to `patternsel` indicates this is for a negated match (NOT operation)
- ILIKE provides case-insensitive pattern matching compared to regular LIKE
- Part of PostgreSQL's statistical estimation system for query optimization
- Located in `src/backend/utils/adt/like_support.c:866-874`

## Simplified Source

```c
Datum
icnlikesel(PG_FUNCTION_ARGS)
{
    // Calculate selectivity for case-insensitive LIKE pattern non-match (NOT ILIKE operator)
    return patternsel(fcinfo, Pattern_Type_Like_IC, true);
}
```