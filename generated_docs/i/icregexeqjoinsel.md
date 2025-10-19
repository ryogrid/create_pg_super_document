# icregexeqjoinsel

## Location
[src/backend/utils/adt/like_support.c:894-902](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/like_support.c#L894-L902)

## Overview
A selectivity estimation function for case-insensitive regular expression join operations in PostgreSQL's query planner.

## Definition

```c
Datum
icregexeqjoinsel(PG_FUNCTION_ARGS)
```
## Detailed Description
 is a join selectivity estimation function that provides selectivity estimates for case-insensitive regular expression matching operations ( operator) in join conditions. It serves as a wrapper function that delegates the actual selectivity calculation to the generic  function, specifying the pattern type as case-insensitive regular expression () and indicating that this is not a negated match (false).

The function returns a selectivity estimate as a floating-point value between 0 and 1, representing the expected fraction of rows that will match the join condition. Currently, the underlying implementation uses a simple default selectivity value rather than performing sophisticated pattern analysis.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro that provides access to function call information including operator arguments, though this specific function doesn't directly access individual arguments
## Dependencies
- Functions called/Symbols referenced:
  - : Generic pattern matching join selectivity function
  - : Enum value indicating case-insensitive regular expression pattern type
- Called from:
  - No direct callers found (likely referenced through PostgreSQL's operator selectivity system)

## Notes and Other Information
- Located in 
- Part of PostgreSQL's cost-based query optimization system
- Returns the default selectivity estimate of 0.005 (0.5%) for case-insensitive regex joins
- The current implementation is a placeholder that doesn't perform actual pattern analysis
- Works in conjunction with PostgreSQL's operator class system for the  (case-insensitive regex match) operator

## Simplified Source

```c
Datum icregexeqjoinsel(PG_FUNCTION_ARGS) {
    // Estimate join selectivity for case-insensitive regex patterns
    // Delegates to generic pattern join selectivity function
    return patternjoinsel(fcinfo, Pattern_Type_Regex_IC, false);
}
```