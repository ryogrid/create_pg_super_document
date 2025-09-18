# pull_exec_paramids

## Location
[src/backend/partitioning/partprune.c:3346-3355](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/partitioning/partprune.c#L3346-L3355)

## Overview
Returns a Bitmapset containing the paramids of all Params with paramkind = PARAM_EXEC in the given expression.

## Definition


## Detailed Description
This function serves as a wrapper around pull_exec_paramids_walker to extract execution parameter IDs from PostgreSQL expressions. It specifically looks for parameters of type PARAM_EXEC, which are used for passing values between different parts of a query execution plan. The function performs a tree walk through the expression structure to identify and collect all such parameters.

PARAM_EXEC parameters are commonly used in subquery execution, correlated queries, and other scenarios where values need to be passed between different execution nodes in the query plan.

## Parameters / Member Variables
- : Expression tree to analyze for PARAM_EXEC parameters

## Dependencies
- Functions called/Symbols referenced:
  - [pull_exec_paramids_walker](pull_exec_paramids_walker.md)
- Called from:
  - [match_clause_to_partition_key](../m/match_clause_to_partition_key.md) (multiple locations)
  - [get_partkey_exec_paramids](../g/get_partkey_exec_paramids.md)

## Notes and Other Information
- This is a static utility function used within the partition pruning subsystem
- The function uses a walker pattern common in PostgreSQL for traversing expression trees
- PARAM_EXEC parameters represent runtime values that may affect partition pruning decisions
- The returned Bitmapset contains the parameter IDs that need to be evaluated at execution time