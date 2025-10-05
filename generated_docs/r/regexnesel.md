# regexnesel

## Location
[src/backend/utils/adt/like_support.c:839-847](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/like_support.c#L839-L847)

## Overview
A PostgreSQL selectivity estimation function that calculates the selectivity of regular expression pattern non-match operations for query optimization purposes.

## Definition
```c
Datum regexnesel(PG_FUNCTION_ARGS)
```

## Detailed Description
The `regexnesel` function is a SQL-callable selectivity estimation function that provides the PostgreSQL query planner with statistical estimates for regular expression non-match operations (typically for operators like `!~`). It serves as a wrapper around the general `patternsel` function, specifically configured for regular expression patterns with negation. The function returns a selectivity estimate as a float8 value between 0.0 and 1.0, representing the expected fraction of rows that will NOT match the given regular expression pattern. This is the complement of regular expression match selectivity.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - `root`: PlannerInfo pointer for query planning context
  - `operator`: OID of the regex non-match operator being estimated
  - `args`: List of operator arguments (typically column and pattern)
  - `varRelid`: Relation ID for variable statistics lookup

## Dependencies
- Functions called/Symbols referenced:
  - [patternsel](../p/patternsel.md): Core pattern selectivity estimation function
  - `Pattern_Type_Regex`: Enum constant for regular expression pattern type
- Called from (representative examples):
  - No direct references found in the codebase (likely registered as operator selectivity function)

## Notes and Other Information
- This function is part of PostgreSQL's cost-based query optimizer infrastructure
- The `ne` suffix indicates "not equal" or "negation" functionality
- Used for estimating selectivity of queries with negated regex operations like `WHERE column !~ 'pattern'`
- The third parameter `true` passed to `patternsel` indicates this is a negated operation
- The selectivity returned is essentially 1.0 minus the selectivity of the corresponding positive regex match
- Helps the planner understand how many rows will be excluded by a NOT matching regex condition
- Delegates actual computation to `patternsel` with regex pattern type and negation flag
- Located in `src/backend/utils/adt/like_support.c:839-847`

## Simplified Source

```c
Datum
regexnesel(PG_FUNCTION_ARGS)
{
    // Calculate selectivity for regex pattern non-match (!~ operator)
    return patternsel(fcinfo, Pattern_Type_Regex, true);
}
```