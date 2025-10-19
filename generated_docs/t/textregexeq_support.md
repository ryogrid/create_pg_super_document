# textregexeq_support

## Location
[src/backend/utils/adt/like_support.c:131-138](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/like_support.c#L131-L138)

## Overview
Provides planner support for case-sensitive regular expression operators by delegating to the common pattern matching support infrastructure.

## Definition

```c
Datum
textregexeq_support(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function serves as a planner support function for PostgreSQL's case-sensitive regular expression operators (such as ~ operator). It acts as a thin wrapper around the generic  function, specifically configured for case-sensitive regular expression pattern matching. This function is called by the PostgreSQL query planner to optimize queries involving regular expression operations, including selectivity estimation and index condition generation.

The function extracts the support request from its arguments and delegates the actual work to , passing  to indicate that this is for case-sensitive regular expression pattern matching.

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
  - PostgreSQL query planner when processing regular expression operations (~ operator)

## Notes and Other Information
- This function is part of PostgreSQL's planner support function infrastructure
- It specifically handles case-sensitive regular expression patterns, as opposed to the case-insensitive variants
- The ~ operator in PostgreSQL uses this support function for query optimization
- The actual logic for selectivity estimation and index optimization is implemented in the shared  function
- Located in

## Simplified Source

```c
Datum textregexeq_support(PG_FUNCTION_ARGS) {
    // Extract the support request from function arguments
    Node *rawreq = (Node *) PG_GETARG_POINTER(0);

    // Delegate to common pattern matching support with regex type
    return like_regex_support(rawreq, Pattern_Type_Regex);
}
```