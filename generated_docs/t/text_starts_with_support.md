# text_starts_with_support

## Location
[src/backend/utils/adt/like_support.c:147-155](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/like_support.c#L147-L155)

## Overview
Provides planner support for prefix matching operations (starts_with function) by delegating to the common pattern matching support infrastructure.

## Definition


## Detailed Description
The  function serves as a planner support function for PostgreSQL's  function and similar prefix matching operations. It acts as a thin wrapper around the generic  function, specifically configured for prefix pattern matching. This function is called by the PostgreSQL query planner to optimize queries involving prefix matching operations, including selectivity estimation and index condition generation.

The function extracts the support request from its arguments and delegates the actual work to , passing  to indicate that this is for prefix pattern matching, which is more efficient than general pattern matching since it only needs to check the beginning of strings.

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
  - PostgreSQL query planner when processing starts_with function calls
  - PostgreSQL query planner when processing prefix operators

## Notes and Other Information
- This function is part of PostgreSQL's planner support function infrastructure
- It specifically handles prefix matching patterns, which are simpler and more efficient than full regex or LIKE patterns
- Prefix matching can often be optimized using B-tree indexes more effectively than general pattern matching
- The  function in PostgreSQL uses this support function for query optimization
- The actual logic for selectivity estimation and index optimization is implemented in the shared  function
- Located in 