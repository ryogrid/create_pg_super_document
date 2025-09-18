# icregexeqsel

## Location
src/backend/utils/adt/like_support.c: 802 - 810

## Overview
A PostgreSQL selectivity estimation function that calculates the selectivity of case-insensitive regular expression match operations for query optimization purposes.

## Definition
```c
Datum icregexeqsel(PG_FUNCTION_ARGS)
```

## Detailed Description
The `icregexeqsel` function is a SQL-callable selectivity estimation function that provides the PostgreSQL query planner with statistical estimates for case-insensitive regular expression match operations (typically for operators like `~*`). It serves as a thin wrapper around the more general `patternsel` function, specifically configured for case-insensitive regex patterns. The function returns a selectivity estimate as a float8 value between 0.0 and 1.0, representing the fraction of rows expected to match the given pattern.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - `root`: PlannerInfo pointer for query planning context
  - `operator`: OID of the regex operator being estimated  
  - `args`: List of operator arguments (typically column and pattern)
  - `varRelid`: Relation ID for variable statistics lookup

## Dependencies
- Functions called/Symbols referenced:
  - `[patternsel](../p/patternsel.md)`: Core pattern selectivity estimation function
  - `Pattern_Type_Regex_IC`: Enum constant for case-insensitive regex type
- Called from (representative examples):
  - No direct references found in the codebase (likely registered as operator selectivity function)

## Notes and Other Information
- This function is part of PostgreSQL's cost-based query optimizer infrastructure
- The `IC` suffix indicates "Ignore Case" functionality
- Returns selectivity estimates used by the planner to choose optimal query execution plans
- Delegates actual computation to `patternsel` with case-insensitive regex pattern type
- Located in `src/backend/utils/adt/like_support.c:802-810`