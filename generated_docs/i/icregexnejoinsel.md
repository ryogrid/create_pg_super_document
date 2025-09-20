# icregexnejoinsel

## Location
[src/backend/utils/adt/like_support.c:939-947](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/like_support.c#L939-L947)

## Overview
Computes join selectivity estimates for case-insensitive regular expression non-match operations between two tables.

## Definition

```c
Datum
icregexnejoinsel(PG_FUNCTION_ARGS)
```
## Detailed Description
This function provides selectivity estimation for join operations involving case-insensitive regular expression non-match (NOT SIMILAR TO with case-insensitive matching). It is a PostgreSQL system function that helps the query planner estimate how many rows will result from a join condition using case-insensitive regex non-matching.

The function is a thin wrapper around the generic  function, specifically configured for case-insensitive regular expression patterns with negation (non-match). This allows the optimizer to make informed decisions about join ordering and execution strategies.

## Parameters / Member Variables
- Uses the standard PostgreSQL function calling convention  which includes:

## Dependencies
- Functions called/Symbols referenced:
  - : Core pattern matching join selectivity function
  - : Enum value indicating case-insensitive regex pattern type
- Called from (representative examples):
  - [Query](../Q/Query.md) planner when estimating costs for joins with case-insensitive regex non-match conditions

## Notes and Other Information
- This function is part of PostgreSQL's cost-based optimizer infrastructure
- It specifically handles the negated case (non-match) of case-insensitive regular expressions
- The  parameter passed to  indicates this is for non-matching (negated) operations
- Located in 
- Returns a selectivity estimate as a float8 value between 0.0 and 1.0