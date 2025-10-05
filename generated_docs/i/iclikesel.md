# iclikesel

## Location
[src/backend/utils/adt/like_support.c:830-838](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/like_support.c#L830-L838)

## Overview
A PostgreSQL selectivity estimation function that calculates the selectivity of case-insensitive LIKE pattern match operations (ILIKE) for query optimization purposes.

## Definition
```c
Datum iclikesel(PG_FUNCTION_ARGS)
```

## Detailed Description
The `iclikesel` function is a SQL-callable selectivity estimation function that provides the PostgreSQL query planner with statistical estimates for case-insensitive LIKE pattern match operations, specifically for the ILIKE operator. It serves as a wrapper around the general `patternsel` function, configured for case-insensitive LIKE patterns that support `%` (wildcard for any sequence of characters) and `_` (wildcard for any single character) while ignoring case differences. The function returns a selectivity estimate as a float8 value between 0.0 and 1.0, representing the expected fraction of rows that will match the given ILIKE pattern.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - `root`: PlannerInfo pointer for query planning context
  - `operator`: OID of the ILIKE operator being estimated
  - `args`: List of operator arguments (typically column and pattern)
  - `varRelid`: Relation ID for variable statistics lookup

## Dependencies
- Functions called/Symbols referenced:
  - [patternsel](../p/patternsel.md): Core pattern selectivity estimation function
  - `Pattern_Type_Like_IC`: Enum constant for case-insensitive LIKE pattern type
- Called from (representative examples):
  - No direct references found in the codebase (likely registered as operator selectivity function)

## Notes and Other Information
- This function is part of PostgreSQL's cost-based query optimizer infrastructure
- The `IC` suffix indicates "Ignore Case" functionality for LIKE patterns
- Handles PostgreSQL's ILIKE operator which performs case-insensitive pattern matching
- Used for estimating selectivity of queries like `WHERE column ILIKE 'Pattern%'`
- The case-insensitive nature means 'ABC', 'abc', and 'AbC' would all match pattern 'ab%'
- Selectivity estimates help the planner choose between different query execution strategies
- Delegates actual computation to `patternsel` with case-insensitive LIKE pattern type
- Located in `src/backend/utils/adt/like_support.c:830-838`

## Simplified Source

```c
Datum
iclikesel(PG_FUNCTION_ARGS)
{
    // Calculate selectivity for case-insensitive LIKE pattern match (ILIKE)
    return patternsel(fcinfo, Pattern_Type_Like_IC, false);
}
```