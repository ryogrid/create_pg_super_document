# textlike_support

## Location
[src/backend/utils/adt/like_support.c:115-122](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/like_support.c#L115-L122)

## Overview
Provides planner support for the LIKE operator by delegating to the common pattern matching support infrastructure.

## Definition


## Detailed Description
The  function serves as a planner support function for PostgreSQL's LIKE operator. It acts as a thin wrapper around the generic  function, specifically configured for case-sensitive LIKE pattern matching. This function is called by the PostgreSQL query planner to optimize queries involving LIKE operations, including selectivity estimation and index condition generation.

The function extracts the support request from its arguments and delegates the actual work to , passing  to indicate that this is for case-sensitive LIKE pattern matching.

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
  - PostgreSQL query planner when processing LIKE operations

## Notes and Other Information
- This function is part of PostgreSQL's planner support function infrastructure
- It specifically handles case-sensitive LIKE patterns (as opposed to ILIKE which is case-insensitive)
- The actual logic for selectivity estimation and index optimization is implemented in the shared  function
- Located in 