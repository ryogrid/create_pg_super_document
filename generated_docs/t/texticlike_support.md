# texticlike_support

## Location
[src/backend/utils/adt/like_support.c:123-130](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/like_support.c#L123-L130)

## Overview
Provides planner support for the ILIKE (case-insensitive LIKE) operator by delegating to the common pattern matching support infrastructure.

## Definition

```c
Datum
texticlike_support(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function serves as a planner support function for PostgreSQL's ILIKE operator, which performs case-insensitive LIKE pattern matching. It acts as a thin wrapper around the generic  function, specifically configured for case-insensitive LIKE pattern matching. This function is called by the PostgreSQL query planner to optimize queries involving ILIKE operations, including selectivity estimation and index condition generation.

The function extracts the support request from its arguments and delegates the actual work to , passing  to indicate that this is for case-insensitive LIKE pattern matching.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro that provides access to:
  - : A Node pointer containing the support request (SupportRequestSelectivity or SupportRequestIndexCondition)

## Dependencies
- Functions called/Symbols referenced:
  - 
  - 
  - 
  - 
- Called from (representative examples):
  - PostgreSQL query planner when processing ILIKE operations

## Notes and Other Information
- This function is part of PostgreSQL's planner support function infrastructure
- It specifically handles case-insensitive LIKE patterns (ILIKE), as opposed to regular LIKE which is case-sensitive
- The "IC" suffix in  stands for "Ignore Case"
- The actual logic for selectivity estimation and index optimization is implemented in the shared  function
- Located in src/backend/utils/adt/like_support.c

## Simplified Source

```c
Datum texticlike_support(PG_FUNCTION_ARGS) {
    // Extract the planner support request
    Node *rawreq = (Node *) PG_GETARG_POINTER(0);

    // Delegate to common pattern support function for case-insensitive LIKE patterns
    PG_RETURN_POINTER(like_regex_support(rawreq, Pattern_Type_Like_IC));
}
``` 