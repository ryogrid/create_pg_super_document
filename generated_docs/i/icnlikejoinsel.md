# icnlikejoinsel

## Location
[src/backend/utils/adt/like_support.c:957-991](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/like_support.c#L957-L991)

## Overview
Computes join selectivity estimates for case-insensitive LIKE pattern non-match operations (NOT ILIKE) between two tables.

## Definition

```c
Datum
icnlikejoinsel(PG_FUNCTION_ARGS)
```
## Detailed Description
This function provides selectivity estimation for join operations involving case-insensitive LIKE pattern non-match (NOT ILIKE operations). It is a PostgreSQL system function that helps the query planner estimate how many rows will result from a join condition using negated case-insensitive LIKE pattern matching.

The function is a thin wrapper around the generic `patternjoinsel` function, specifically configured for case-insensitive LIKE patterns with negation (non-match). This allows the optimizer to make informed decisions about join ordering and execution strategies when NOT ILIKE conditions are present in join predicates.

## Parameters / Member Variables
- Uses the standard PostgreSQL function calling convention `PG_FUNCTION_ARGS` which includes:

## Dependencies
- Functions called/Symbols referenced:
  - [patternjoinsel](../p/patternjoinsel.md): Core pattern matching join selectivity function
  - `Pattern_Type_Like_IC`: Enum value indicating case-insensitive LIKE pattern type
  - `Pattern_Prefix_Status`: Type related to pattern prefix analysis
- Called from (representative examples):
  - [Query](../Q/Query.md) planner when estimating costs for joins with NOT ILIKE conditions

## Notes and Other Information
- This function is part of PostgreSQL's cost-based optimizer infrastructure
- It specifically handles the negated case (non-match) of case-insensitive LIKE patterns
- The `true` parameter passed to `patternjoinsel` indicates this is for non-matching (negated) operations
- Located in `src/backend/utils/adt/like_support.c:957-991`
- Returns a selectivity estimate as a float8 value between 0.0 and 1.0
- ILIKE is PostgreSQL's case-insensitive variant of the LIKE operator