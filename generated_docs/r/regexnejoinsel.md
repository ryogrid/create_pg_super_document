# regexnejoinsel

## Location
[src/backend/utils/adt/like_support.c:930-938](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/like_support.c#L930-L938)

## Overview
A selectivity estimation function for negated regular expression join operations in PostgreSQL's query planner.

## Definition

```c
Datum
regexnejoinsel(PG_FUNCTION_ARGS)
```
## Detailed Description
 is a join selectivity estimation function that provides selectivity estimates for negated regular expression matching operations ( operator) in join conditions. It serves as a wrapper function that delegates the actual selectivity calculation to the generic  function, specifying the pattern type as case-sensitive regular expression () and indicating that this is a negated match (true).

The function returns a selectivity estimate as a floating-point value between 0 and 1, representing the expected fraction of rows that will NOT match the regular expression pattern in the join condition. Since this is a negated operation, it returns  (approximately 0.995 or 99.5%) instead of the default match selectivity. This estimate helps PostgreSQL's query planner determine the most efficient join order and execution strategy for negated regex operations.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro that provides access to function call information including operator arguments, though this specific function doesn't directly access individual arguments
## Dependencies
- Functions called/Symbols referenced:
  - : Generic pattern matching join selectivity function
  - : Enum value indicating case-sensitive regular expression pattern type
- Called from:
  - No direct callers found (likely referenced through PostgreSQL's operator selectivity system)

## Notes and Other Information
- Located in 
- Part of PostgreSQL's cost-based query optimization system
- Returns the negated default selectivity estimate of approximately 0.995 (99.5%) for negated regex joins
- The current implementation is a placeholder that doesn't perform actual pattern analysis
- Works in conjunction with PostgreSQL's operator class system for the  (does not match regex) operator
- Used for estimating selectivity of expressions like 
- The negation logic is handled by passing  as the third parameter to 