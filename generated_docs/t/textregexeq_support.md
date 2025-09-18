# textregexeq_support

## Location
src/backend/utils/adt/like_support.c: 131 - 138

## Overview
Provides planner support for case-sensitive regular expression operators by delegating to the common pattern matching support infrastructure.

## Definition


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