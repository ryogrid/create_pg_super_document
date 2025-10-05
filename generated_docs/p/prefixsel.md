# prefixsel

## Location
[src/backend/utils/adt/like_support.c:820-829](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/like_support.c#L820-L829)

## Overview
A PostgreSQL selectivity estimation function that calculates the selectivity of prefix pattern match operations for query optimization purposes.

## Definition
```c
Datum prefixsel(PG_FUNCTION_ARGS)
```

## Detailed Description
The `prefixsel` function is a SQL-callable selectivity estimation function that provides the PostgreSQL query planner with statistical estimates for prefix match operations. It serves as a wrapper around the general `patternsel` function, specifically configured for prefix patterns where the search pattern matches the beginning of strings. This is particularly useful for operations like text search with prefix matching or range queries on text columns. The function returns a selectivity estimate as a float8 value between 0.0 and 1.0, representing the expected fraction of rows that will match the given prefix pattern.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - `root`: PlannerInfo pointer for query planning context
  - `operator`: OID of the prefix operator being estimated
  - `args`: List of operator arguments (typically column and prefix pattern)
  - `varRelid`: Relation ID for variable statistics lookup

## Dependencies
- Functions called/Symbols referenced:
  - [patternsel](patternsel.md): Core pattern selectivity estimation function
  - `Pattern_Type_Prefix`: Enum constant for prefix pattern type
- Called from (representative examples):
  - [patternsel_common](patternsel_common.md): Core pattern selectivity computation function
  - [prefix_selectivity](prefix_selectivity.md): Specialized prefix selectivity estimation
  - [regex_selectivity](../r/regex_selectivity.md): Regular expression selectivity estimation

## Notes and Other Information
- This function is an essential component of PostgreSQL's cost-based query optimizer
- Handles prefix matching patterns commonly used in text search and range queries
- Used for estimating selectivity of operations that match string prefixes
- Unlike LIKE patterns, prefix operations focus specifically on the beginning of strings
- The selectivity estimates help the planner choose optimal index usage and join strategies
- Delegates actual computation to `patternsel` with prefix pattern type specification
- More heavily referenced than other pattern selectivity functions, indicating its importance in query optimization
- Located in `src/backend/utils/adt/like_support.c:820-829`

## Simplified Source

```c
Datum
prefixsel(PG_FUNCTION_ARGS)
{
    // Calculate selectivity for prefix pattern match
    return patternsel(fcinfo, Pattern_Type_Prefix, false);
}
```