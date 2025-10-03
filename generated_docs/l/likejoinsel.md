# likejoinsel

## Location
[src/backend/utils/adt/like_support.c:903-911](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/like_support.c#L903-L911)

## Overview
A selectivity estimation function for LIKE pattern matching join operations in PostgreSQL's query planner.

## Definition

```c
Datum
likejoinsel(PG_FUNCTION_ARGS)
```
## Detailed Description
 is a join selectivity estimation function that provides selectivity estimates for LIKE pattern matching operations in join conditions. It serves as a wrapper function that delegates the actual selectivity calculation to the generic  function, specifying the pattern type as case-sensitive LIKE pattern () and indicating that this is not a negated match (false).

The function returns a selectivity estimate as a floating-point value between 0 and 1, representing the expected fraction of rows that will match the join condition. This estimate is used by PostgreSQL's query planner to determine the most efficient join order and execution strategy. Currently, the underlying implementation uses a simple default selectivity value rather than performing sophisticated pattern analysis.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro that provides access to function call information including operator arguments, though this specific function doesn't directly access individual arguments
## Dependencies
- Functions called/Symbols referenced:
  - : Generic pattern matching join selectivity function
  - : Enum value indicating case-sensitive LIKE pattern type
- Called from:
  - No direct callers found (likely referenced through PostgreSQL's operator selectivity system)

## Notes and Other Information
- Located in 
- Part of PostgreSQL's cost-based query optimization system
- Returns the default selectivity estimate of 0.005 (0.5%) for LIKE pattern joins
- The current implementation is a placeholder that doesn't perform actual pattern analysis
- Works in conjunction with PostgreSQL's operator class system for the LIKE operator
- Used for estimating selectivity of expressions like 