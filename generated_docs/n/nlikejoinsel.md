# nlikejoinsel

## Location
[src/backend/utils/adt/like_support.c:948-956](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/like_support.c#L948-L956)

## Overview
Computes join selectivity estimates for LIKE pattern non-match operations between two tables.

## Definition

```c
Datum
nlikejoinsel(PG_FUNCTION_ARGS)
```
## Detailed Description
This function provides selectivity estimation for join operations involving LIKE pattern non-match (NOT LIKE operations). It is a PostgreSQL system function that helps the query planner estimate how many rows will result from a join condition using negated LIKE pattern matching.

The function is a thin wrapper around the generic `patternjoinsel` function, specifically configured for LIKE patterns with negation (non-match). This allows the optimizer to make informed decisions about join ordering and execution strategies when NOT LIKE conditions are present in join predicates.

## Parameters / Member Variables
- Uses the standard PostgreSQL function calling convention `PG_FUNCTION_ARGS` which includes:

## Dependencies
- Functions called/Symbols referenced:
  - [patternjoinsel](../p/patternjoinsel.md): Core pattern matching join selectivity function
  - `Pattern_Type_Like`: Enum value indicating LIKE pattern type
- Called from (representative examples):
  - [Query](../Q/Query.md) planner when estimating costs for joins with NOT LIKE conditions

## Notes and Other Information
- This function is part of PostgreSQL's cost-based optimizer infrastructure
- It specifically handles the negated case (non-match) of LIKE patterns
- The `true` parameter passed to `patternjoinsel` indicates this is for non-matching (negated) operations
- Located in `src/backend/utils/adt/like_support.c:948-956`
- Returns a selectivity estimate as a float8 value between 0.0 and 1.0