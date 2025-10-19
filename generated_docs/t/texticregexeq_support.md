# texticregexeq_support

## Location
[src/backend/utils/adt/like_support.c:139-146](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/like_support.c#L139-L146)

## Overview
Provides planner support for case-insensitive regular expression operators by delegating to the common pattern matching support infrastructure.

## Definition

```c
Datum
texticregexeq_support(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function serves as a planner support function for PostgreSQL's case-insensitive regular expression operators (such as ~* operator). It acts as a thin wrapper around the generic  function, specifically configured for case-insensitive regular expression pattern matching. This function is called by the PostgreSQL query planner to optimize queries involving case-insensitive regular expression operations, including selectivity estimation and index condition generation.

The function extracts the support request from its arguments and delegates the actual work to , passing  to indicate that this is for case-insensitive regular expression pattern matching.

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
  - PostgreSQL query planner when processing case-insensitive regular expression operations (~* operator)

## Notes and Other Information
- This function is part of PostgreSQL's planner support function infrastructure
- It specifically handles case-insensitive regular expression patterns, as opposed to the case-sensitive variants
- The "IC" suffix in  stands for "Ignore Case"
- The ~* operator in PostgreSQL uses this support function for query optimization
- The actual logic for selectivity estimation and index optimization is implemented in the shared  function
- Located in

## Simplified Source

```c
Datum texticregexeq_support(PG_FUNCTION_ARGS) {
    // Extract the support request from function arguments
    Node *rawreq = (Node *) PG_GETARG_POINTER(0);

    // Delegate to common pattern matching support with case-insensitive regex type
    return like_regex_support(rawreq, Pattern_Type_Regex_IC);
}
```